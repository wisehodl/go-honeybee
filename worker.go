package honeybee

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	component "git.wisehodl.dev/jay/go-mana-component"
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

	ConnectionAvailable bool
	Connection          transport.ConnectionStats

	TotalProcessed uint64
	TotalSent      uint64
	TotalRestarts  uint64
}

type DefaultWorker struct {
	id   string
	conn atomic.Pointer[transport.Connection]

	heartbeat chan struct{}

	processedCount *atomic.Uint64
	outgoingCount  *atomic.Uint64
	restartCount   *atomic.Uint64

	config  *WorkerConfig
	ctx     context.Context
	cancel  context.CancelFunc
	handler slog.Handler
	logger  *slog.Logger
}

func NewWorker(
	ctx context.Context,
	id string,
	config *WorkerConfig,
	handler slog.Handler,
) (*DefaultWorker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}
	if err := ValidateWorkerConfig(config); err != nil {
		return nil, err
	}

	if component.FromContext(ctx) == nil {
		ctx = component.MustNew(ctx, "honeybee", "worker")
	} else {
		ctx = component.MustExtend(ctx, "worker")
	}

	var logger *slog.Logger
	if handler != nil {
		c := component.FromContext(ctx)
		logger = slog.New(handler).With(slog.Any("component", c), slog.String("peer_id", id))
	}

	wctx, wcancel := context.WithCancel(ctx)
	w := &DefaultWorker{
		id:             id,
		config:         config,
		heartbeat:      make(chan struct{}),
		processedCount: &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		restartCount:   &atomic.Uint64{},
		ctx:            wctx,
		cancel:         wcancel,
		handler:        handler,
		logger:         logger,
	}

	return w, nil
}

func (w *DefaultWorker) Start(pool PoolPlugin) {
	if w.logger != nil {
		w.logger.Debug("starting")
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		w.runSession(w.ctx, pool)
	})

	if w.logger != nil {
		w.logger.Info("started")
	}

	wg.Wait()

	if w.logger != nil {
		w.logger.Info("stopped")
	}
}

func (w *DefaultWorker) runSession(ctx context.Context, pool PoolPlugin) {
	newConn := make(chan *transport.Connection, 1)

	var timer *time.Timer
	if w.config.KeepaliveTimeout > 0 {
		if w.logger != nil {
			w.logger.Debug("keepalive: enabled", "timeout", w.config.KeepaliveTimeout)
		}
		timer = time.NewTimer(w.config.KeepaliveTimeout)
		defer timer.Stop()
	} else {
		if w.logger != nil {
			w.logger.Debug("keepalive: disabled")
		}
	}

	resetTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.config.KeepaliveTimeout)
	}

	timerC := func() <-chan time.Time {
		if timer == nil {
			return nil
		}
		return timer.C
	}

	var dialCancel context.CancelFunc

	spawnDial := func() {
		if dialCancel != nil {
			dialCancel()
		}
		var dialCtx context.Context
		dialCtx, dialCancel = context.WithCancel(ctx)
		if w.logger != nil {
			w.logger.Debug("session: requesting connection")
		}
		go func() {
			conn, err := connect(w.id, dialCtx, pool, w.handler)
			if err != nil {
				if w.logger != nil {
					w.logger.Warn("dialer: dial failed")
				}
				return
			}
			select {
			case newConn <- conn:
			case <-dialCtx.Done():
				conn.Close()
			}
		}()
	}

	for {
		// spawn initial dial for this reconnect cycle
		spawnDial()

		// obtain new connection
		var conn *transport.Connection
	preConn:
		for {
			select {
			case <-ctx.Done():
				if dialCancel != nil {
					dialCancel()
				}
				return
			case <-w.heartbeat:
				resetTimer()
			case <-timerC():
				if w.logger != nil {
					w.logger.Info("keepalive: no activity observed")
				}
				timer.Reset(w.config.KeepaliveTimeout)
				spawnDial()
			case conn = <-newConn:
				if w.logger != nil {
					w.logger.Debug("session: connected")
				}
				break preConn
			}
		}

		// set up new connection
		w.conn.Store(conn)
		pool.Events <- PoolEvent{ID: w.id, Kind: EventConnected, At: time.Now()}

		if w.logger != nil {
			w.logger.Info("session: started")
		}

		// run session loop
	conn_loop:
		for {
			select {
			case <-ctx.Done():
				break conn_loop
			case <-w.heartbeat:
				resetTimer()
			case <-timerC():
				if w.logger != nil {
					w.logger.Info("keepalive: no activity observed")
				}
				timer.Reset(w.config.KeepaliveTimeout)
				break conn_loop
			case data, ok := <-conn.Incoming():
				if !ok {
					if w.logger != nil {
						w.logger.Debug("reader: disconnected")
					}
					break conn_loop
				}
				pool.Inbox <- types.InboxMessage{
					ID:         w.id,
					Data:       data,
					ReceivedAt: time.Now(),
				}
				resetTimer()
			case <-conn.Heartbeat():
				if w.logger != nil {
					w.logger.Debug("ping-pong heartbeat")
				}
				resetTimer()
			}
		}

		conn.Close()

		if w.logger != nil {
			w.logger.Info("session: ended")
		}

		// tear down connection
		w.conn.Store(nil)
		pool.Events <- PoolEvent{ID: w.id, Kind: EventDisconnected, At: time.Now()}

		// exit if worker is shutting down
		select {
		case <-ctx.Done():
			return
		default:
		}

		// refresh session
		time.Sleep(w.config.ReconnectDelay)
		w.restartCount.Add(1)
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

		ConnectionAvailable: connectionAvailable,
		Connection:          connStats,

		TotalProcessed: w.processedCount.Load(),
		TotalRestarts:  w.restartCount.Load(),
		TotalSent:      w.outgoingCount.Load(),
	}
}

func connect(
	id string,
	ctx context.Context,
	pool PoolPlugin,
	handler slog.Handler,
) (*transport.Connection, error) {
	conn, err := transport.NewConnection(ctx, id, pool.ConnectionConfig, handler)
	if err != nil {
		return nil, err
	}

	conn.SetDialer(pool.Dialer)
	return conn, conn.Connect(ctx)
}
