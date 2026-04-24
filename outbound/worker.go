package outbound

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/logging"
	"git.wisehodl.dev/jay/go-honeybee/queue"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Worker

type Worker interface {
	Start(pool PoolPlugin)
	Stop()
	Send(data []byte) error
	Stats() WorkerStats
}

type WorkerStats struct {
	IncomingAvailable bool
	ChanIncoming      int
	ChanQueue         int
	ChanForwarder     int

	ConnectionAvailable bool
	Connection          transport.ConnectionStats

	TotalProcessed uint64
	TotalDropped   uint64
	TotalSent      uint64
	TotalRestarts  uint64
}

type DefaultWorker struct {
	id   string
	conn atomic.Pointer[transport.Connection]

	heartbeat   chan struct{}
	toQueue     chan types.ReceivedMessage
	toForwarder chan types.ReceivedMessage

	processedCount *atomic.Uint64
	droppedCount   *atomic.Uint64
	outgoingCount  *atomic.Uint64
	restartCount   *atomic.Uint64

	config *WorkerConfig
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func NewWorker(
	ctx context.Context,
	id string,
	config *WorkerConfig,
	logger *slog.Logger,
) (*DefaultWorker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}
	if err := ValidateWorkerConfig(config); err != nil {
		return nil, err
	}

	wctx, wcancel := context.WithCancel(ctx)
	w := &DefaultWorker{
		id:             id,
		config:         config,
		heartbeat:      make(chan struct{}),
		toQueue:        make(chan types.ReceivedMessage, 256),
		toForwarder:    make(chan types.ReceivedMessage, 256),
		processedCount: &atomic.Uint64{},
		droppedCount:   &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		restartCount:   &atomic.Uint64{},
		ctx:            wctx,
		cancel:         wcancel,
		logger:         logger,
	}

	return w, nil
}

func (w *DefaultWorker) Start(pool PoolPlugin) {
	if w.logger != nil {
		w.logger.Debug("starting")
	}

	dial := make(chan struct{}, 1)
	newConn := make(chan *transport.Connection, 1)
	keepalive := make(chan struct{}, 1)

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer wg.Done()
		RunDialer(w.id, w.ctx, pool, dial, newConn, w.logger)
	}()

	go func() {
		defer wg.Done()
		RunKeepalive(w.ctx, w.heartbeat, keepalive, w.config.KeepaliveTimeout, w.logger)
	}()

	go func() {
		defer wg.Done()
		queue.RunQueue(w.id, w.ctx, w.toQueue, w.toForwarder, w.config.MaxQueueSize, w.droppedCount)
	}()

	go func() {
		defer wg.Done()
		RunForwarder(w.id, w.ctx, w.toForwarder, pool.Inbox, w.processedCount, pool.InboxCounter)
	}()

	go func() {
		defer wg.Done()
		session := &Session{
			id:             w.id,
			connPtr:        &w.conn,
			messages:       w.toQueue,
			heartbeat:      w.heartbeat,
			dial:           dial,
			keepalive:      keepalive,
			newConn:        newConn,
			reconnectDelay: w.config.ReconnectDelay,
			restartCount:   w.restartCount,
			logger:         w.logger,
		}
		session.Start(w.ctx, pool)
	}()

	if w.logger != nil {
		w.logger.Info("started")
	}

	wg.Wait()

	if w.logger != nil {
		w.logger.Info("stopped")
	}
}

func (w *DefaultWorker) Stop() {
	if w.logger != nil {
		w.logger.Debug("shutting down")
	}
	w.cancel()
}

func (w *DefaultWorker) Send(data []byte) error {
	conn := w.conn.Load()
	if conn == nil {
		// connection not established by session
		return NewWorkerError(w.id, ErrConnectionUnavailable)
	}

	if err := conn.Send(data); err != nil {
		return NewWorkerError(w.id, err)
	}

	select {
	case w.heartbeat <- struct{}{}:
	case <-w.ctx.Done():
	}

	w.outgoingCount.Add(1)

	return nil
}

func (w *DefaultWorker) Stats() WorkerStats {
	connectionAvailable := false
	incomingLen := 0
	connStats := transport.ConnectionStats{}

	conn := w.conn.Load()
	if conn != nil {
		connectionAvailable = true
		incomingLen = len(conn.Incoming())
		connStats = conn.Stats()
	}

	return WorkerStats{
		IncomingAvailable: connectionAvailable,
		ChanIncoming:      incomingLen,
		ChanQueue:         len(w.toQueue),
		ChanForwarder:     len(w.toForwarder),

		ConnectionAvailable: connectionAvailable,
		Connection:          connStats,

		TotalProcessed: w.processedCount.Load(),
		TotalDropped:   w.droppedCount.Load(),
		TotalRestarts:  w.restartCount.Load(),
		TotalSent:      w.outgoingCount.Load(),
	}
}

type Session struct {
	id      string
	connPtr *atomic.Pointer[transport.Connection]

	messages  chan<- types.ReceivedMessage
	heartbeat chan<- struct{}
	dial      chan<- struct{}

	keepalive <-chan struct{}
	newConn   <-chan *transport.Connection

	reconnectDelay time.Duration
	restartCount   *atomic.Uint64

	logger *slog.Logger
}

func (s *Session) Start(
	ctx context.Context,
	pool PoolPlugin,
) {
	for {
		if s.logger != nil {
			s.logger.Debug("session: requesting connection")
		}

		// request new connection
		select {
		case s.dial <- struct{}{}:
		default:
		}

		// obtain new connection
		var conn *transport.Connection
	preConn:
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.keepalive:
				select {
				case s.dial <- struct{}{}:
					if s.logger != nil {
						s.logger.Debug("session: requesting connection")
					}

				default:
				}
			case conn = <-s.newConn:
				if s.logger != nil {
					s.logger.Debug("session: connected")
				}
				break preConn
			}
		}

		// set up new connection
		s.connPtr.Store(conn)
		pool.Events <- PoolEvent{ID: s.id, Kind: EventConnected}

		// set up session context
		sctx, scancel := context.WithCancel(ctx)
		onStop := func() { scancel() }

		// start session
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			RunReader(sctx, onStop, conn, s.messages, s.heartbeat, s.logger)
		}()
		go func() {
			defer wg.Done()
			RunHeartbeatForwarder(sctx, conn, s.heartbeat, s.logger)
		}()
		go func() {
			defer wg.Done()
			RunStopMonitor(sctx, onStop, conn, s.keepalive, s.logger)
		}()

		if s.logger != nil {
			s.logger.Info("session: started")
		}

		// complete session
		wg.Wait()

		if s.logger != nil {
			s.logger.Info("session: ended")
		}

		// tear down connection
		s.connPtr.Store(nil)
		pool.Events <- PoolEvent{ID: s.id, Kind: EventDisconnected}

		// exit if worker is shutting down
		select {
		case <-ctx.Done():
			return
		default:
		}

		// refresh session
		time.Sleep(s.reconnectDelay)
		s.restartCount.Add(1)
	}

}

func RunReader(
	ctx context.Context,
	onStop func(),
	conn *transport.Connection,
	messages chan<- types.ReceivedMessage,
	heartbeat chan<- struct{},
	logger *slog.Logger,
) {
	defer func() {
		if logger != nil {
			logger.Debug("reader: stopping")
		}

		conn.Close()
		onStop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-conn.Incoming():
			if !ok {
				// connection has closed
				if logger != nil {
					logger.Debug("reader: disconnected")
				}
				return
			}

			// send message forward
			messages <- types.ReceivedMessage{Data: data, ReceivedAt: time.Now()}

			// send heartbeat
			select {
			case heartbeat <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func RunHeartbeatForwarder(
	ctx context.Context,
	conn *transport.Connection,
	heartbeat chan<- struct{},
	logger *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-conn.Heartbeat():
			select {
			case heartbeat <- struct{}{}:
				if logger != nil {
					logger.Debug("ping-pong heartbeat")
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

func RunStopMonitor(
	ctx context.Context,
	onStop func(),
	conn *transport.Connection,
	keepalive <-chan struct{},
	logger *slog.Logger,
) {
	defer func() {
		if logger != nil {
			logger.Debug("stop monitor: stopping")
		}

		conn.Close()
		onStop()
	}()

	select {
	case <-ctx.Done():
	case <-keepalive:
		if logger != nil {
			logger.Debug("stop monitor: stopping: keepalive")
		}
	}
}

func RunForwarder(
	id string,
	ctx context.Context,
	messages <-chan types.ReceivedMessage,
	inbox chan<- InboxMessage,
	workerProcessedCount *atomic.Uint64,
	poolInboxCount *atomic.Uint64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return

			case inbox <- InboxMessage{
				ID:         id,
				Data:       msg.Data,
				ReceivedAt: msg.ReceivedAt,
			}:
				workerProcessedCount.Add(1)
				poolInboxCount.Add(1)
			}
		}
	}
}

func RunKeepalive(
	ctx context.Context,
	heartbeat <-chan struct{},
	keepalive chan<- struct{},
	timeout time.Duration,
	logger *slog.Logger,
) {
	// disable keepalive timeout if not configured
	if timeout <= 0 {
		if logger != nil {
			logger.Debug("keepalive: disabled")
		}
		// drain heartbeats
		// wait for cancel and exit
		for {
			select {
			case <-heartbeat:
			case <-ctx.Done():
				return
			}
		}
	}

	if logger != nil {
		logger.Debug("keepalive: enabled", "timeout", timeout)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat:
			// drain the timer channel and reset
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		// timer completed
		case <-timer.C:
			// send keepalive signal, then reset the timer
			if logger != nil {
				logger.Info("keepalive: no activity observed")
			}
			select {
			case keepalive <- struct{}{}:
			default:
			}
			timer.Reset(timeout)
		}
	}
}

func connect(
	id string,
	ctx context.Context,
	pool PoolPlugin,
) (*transport.Connection, error) {
	var logger *slog.Logger
	if pool.Handler != nil && pool.ConnectionConfig.LoggingEnabled {
		logger = logging.NewConnectionLogger(
			logging.WrapOrDefault(pool.ConnectionConfig.LogLevel, pool.Handler), pool.ID, id)
	}

	conn, err := transport.NewConnection(id, pool.ConnectionConfig, logger)
	if err != nil {
		return nil, err
	}

	conn.SetDialer(pool.Dialer)
	return conn, conn.Connect(ctx)
}

func RunDialer(
	id string,
	ctx context.Context,
	pool PoolPlugin,

	dial <-chan struct{},
	newConn chan<- *transport.Connection,

	logger *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-dial:
			// drain dial signals while connection is being established
			done := make(chan struct{})
			go func() {
				for {
					select {
					case <-dial:
					case <-done:
						return
					}
				}
			}()

			if logger != nil {
				logger.Debug("dialer: dialing")
			}
			// dial a new connection
			conn, err := connect(id, ctx, pool)
			close(done)

			// send error if dial failed and continue
			if err != nil {
				if logger != nil {
					logger.Warn("dialer: dial failed")
				}
				select {
				case pool.Errors <- err:
				case <-ctx.Done():
				}
				continue
			}

			if logger != nil {
				logger.Debug("dialer: connected")
			}

			// send the new connection or close and exit
			select {
			case newConn <- conn:
			case <-ctx.Done():
				conn.Close()
				return
			}
		}
	}
}
