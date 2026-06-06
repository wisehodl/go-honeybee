package honeybee

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"git.wisehodl.dev/jay/go-mana-component"
)

// ----------------------------------------------------------------------------
// Errors
// ----------------------------------------------------------------------------

var ErrConnectionUnavailable = errors.New("connection unavailable")

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type WorkerConfig struct {
	ConnectionConfig transport.ConnectionConfig
	RetryConfig      transport.RetryConfig
	RetryDisabled    bool
	KeepaliveTimeout time.Duration
	ReconnectDelay   time.Duration
}

type WorkerOption func(*WorkerConfig)

func NewWorkerConfig(opts ...WorkerOption) (*WorkerConfig, error) {
	connCfg, _ := transport.NewConnectionConfig()
	retryCfg, _ := transport.NewRetryConfig()
	cfg := &WorkerConfig{
		ConnectionConfig: *connCfg,
		RetryConfig:      retryCfg,
		KeepaliveTimeout: 60 * time.Second,
		ReconnectDelay:   2 * time.Second,
	}
	for _, o := range opts {
		o(cfg)
	}
	if err := ValidateWorkerConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func WithConnectionConfig(value transport.ConnectionConfig) WorkerOption {
	return func(c *WorkerConfig) {
		c.ConnectionConfig = value
	}
}

func WithRetryConfig(value transport.RetryConfig) WorkerOption {
	return func(c *WorkerConfig) {
		c.RetryConfig = value
	}
}

func WithRetryDisabled() WorkerOption {
	return func(c *WorkerConfig) {
		c.RetryDisabled = true
	}
}

// When KeepaliveTimeout is set to zero, keepalive functionality is disabled.
func WithKeepaliveTimeout(value time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.KeepaliveTimeout = value
	}
}

func WithReconnectDelay(value time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.ReconnectDelay = value
	}
}

func ValidateWorkerConfig(c *WorkerConfig) error {
	if !c.RetryDisabled {
		if err := transport.ValidateRetryConfig(c.RetryConfig); err != nil {
			return err
		}
	}
	if c.KeepaliveTimeout < 0 {
		return fmt.Errorf("invalid keepalive timeout: %v", c.KeepaliveTimeout)
	}
	if c.ReconnectDelay < 0 {
		return fmt.Errorf("invalid reconnect delay: %v", c.ReconnectDelay)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Worker
// ----------------------------------------------------------------------------

type Worker interface {
	Start(pool PoolPlugin)
	Stop()
	Send(data []byte) error
	Stats() WorkerStats
}

type WorkerStats struct {
	ConnectionAvailable bool
	Connection          transport.ConnectionStats
	ChanIncoming        int

	TotalProcessed uint64
	TotalSent      uint64
	TotalRestarts  uint64
}

type DefaultWorker struct {
	url    string
	dialFn func(context.Context) (types.Socket, error)
	conn   atomic.Pointer[transport.Connection]

	sendHeartbeat chan struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	config  WorkerConfig
	handler slog.Handler
	logger  *slog.Logger

	processedCount *atomic.Uint64
	outgoingCount  *atomic.Uint64
	restartCount   *atomic.Uint64
}

func NewWorker(
	ctx context.Context,
	url string,
	dialFn func(context.Context) (types.Socket, error),
	config *WorkerConfig,
	handler slog.Handler,
) (*DefaultWorker, error) {
	if config == nil {
		config, _ = NewWorkerConfig()
	}
	if err := ValidateWorkerConfig(config); err != nil {
		return nil, err
	}

	if component.FromContext(ctx) == nil {
		ctx = component.MustNew(ctx, "honeybee", "worker")
	} else {
		ctx = component.MustExtend(ctx, "worker")
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &DefaultWorker{
		url:    url,
		dialFn: dialFn,

		sendHeartbeat: make(chan struct{}),

		ctx:    ctx,
		cancel: cancel,
		config: *config,

		processedCount: &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		restartCount:   &atomic.Uint64{},
	}

	if handler != nil {
		comp := component.FromContext(ctx)
		w.handler = handler.WithAttrs([]slog.Attr{slog.String("peer", url)})
		w.logger = slog.New(w.handler).With(slog.Any("component", comp))
	}

	return w, nil
}

func (w *DefaultWorker) Start(pool PoolPlugin) {
	if w.logger != nil {
		w.logger.Debug("starting")
	}

	var wg sync.WaitGroup
	wg.Go(func() { w.runSession(w.ctx, pool) })

	if w.logger != nil {
		w.logger.Debug("started")
	}

	wg.Wait()

	if w.logger != nil {
		w.logger.Debug("stopped")
	}
}

func (w *DefaultWorker) runSession(ctx context.Context, pool PoolPlugin) {
	for {
		var retryMgr *transport.RetryManager
		if !w.config.RetryDisabled {
			retryMgr = transport.NewRetryManager(w.config.RetryConfig)
		}
		onError := func(err error) {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return
			}
			pool.Events <- PoolEvent{
				ID: w.url, Kind: EventDialFailed, Err: err, At: time.Now()}
		}
		socket, err := transport.DialWithRetry(
			ctx, retryMgr, w.dialFn, onError, w.handler,
		)
		if err != nil {
			if pool.Retire != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				pool.Retire(err)
			}
			return
		}

		// setup new connection
		conn, _ := transport.NewConnection(
			ctx, socket, &w.config.ConnectionConfig, w.handler)
		w.conn.Store(conn)
		pool.Events <- PoolEvent{ID: w.url, Kind: EventConnected, At: time.Now()}

		if w.logger != nil {
			w.logger.Debug("session: started")
		}

		// start keepalive service
		stopKeepalive, inactive, heartbeat := w.startKeepalive()

		// run session loop
	session:
		for {
			select {
			case <-ctx.Done():
				break session

			case data, ok := <-conn.Incoming():
				if !ok {
					var reason error
					select {
					case reason = <-conn.Errors():
					default:
						reason = fmt.Errorf("unknown")
					}
					if w.logger != nil {
						w.logger.Info("websocket: closed", "reason", reason)
					}
					break session
				}

				pool.Inbox <- types.InboxMessage{
					ID: w.url, Data: data, ReceivedAt: time.Now()}

				pool.InboxCounter.Add(1)
				w.processedCount.Add(1)

				heartbeat()

			case <-conn.Heartbeat():
				heartbeat()

			case <-w.sendHeartbeat:
				heartbeat()

			case <-inactive():
				if w.logger != nil {
					w.logger.Warn("keepalive: no activity observed")
				}
				break session
			}
		}

		// session ended
		conn.Close()
		stopKeepalive()

		if w.logger != nil {
			w.logger.Info("disconnected")
		}
		if w.logger != nil {
			w.logger.Debug("session: ended")
		}

		// tear down connection
		w.conn.Store(nil)
		pool.Events <- PoolEvent{ID: w.url, Kind: EventDisconnected, At: time.Now()}

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

func (w *DefaultWorker) startKeepalive() (
	stop func(),
	inactive func() <-chan time.Time,
	heartbeat func(),
) {
	var timer *time.Timer
	stop = func() {}

	if w.config.KeepaliveTimeout > 0 {
		if w.logger != nil {
			w.logger.Debug("keepalive: enabled", "timeout", w.config.KeepaliveTimeout)
		}
		timer = time.NewTimer(w.config.KeepaliveTimeout)
		stop = func() { timer.Stop() }
	} else {
		if w.logger != nil {
			w.logger.Debug("keepalive: disabled")
		}
	}

	heartbeat = func() {
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

	inactive = func() <-chan time.Time {
		if timer == nil {
			return nil
		}
		return timer.C
	}

	return
}

func (w *DefaultWorker) Stop() {
	if w.logger != nil {
		w.logger.Info("shutting down")
	}
	w.cancel()
}

func (w *DefaultWorker) Send(data []byte) error {
	conn := w.conn.Load()
	if conn == nil {
		// connection not established by session
		return ErrConnectionUnavailable
	}

	err := conn.Send(data)
	if err != nil {
		return err
	}

	select {
	case w.sendHeartbeat <- struct{}{}:
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
		ConnectionAvailable: connectionAvailable,
		Connection:          connStats,
		ChanIncoming:        incomingLen,

		TotalProcessed: w.processedCount.Load(),
		TotalRestarts:  w.restartCount.Load(),
		TotalSent:      w.outgoingCount.Load(),
	}
}
