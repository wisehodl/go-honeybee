package inbound

import (
	"context"
	"errors"
	"git.wisehodl.dev/jay/go-honeybee/queue"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Worker interface {
	Start(pool PoolPlugin)
	Stop()
	Send(data []byte) error
	Stats() WorkerStats
}

type WorkerExitKind string

const (
	ExitDisconnected    WorkerExitKind = "disconnected"
	ExitUnexpectedClose WorkerExitKind = "unexpected_close"
	ExitReadError       WorkerExitKind = "read_error"
	ExitPolicy          WorkerExitKind = "policy"
)

type WorkerStats struct {
	ChanIncoming  int
	ChanQueue     int
	ChanForwarder int
	BufferDepth   int64

	TotalProcessed uint64
	TotalDropped   uint64
	TotalSent      uint64
}

type DefaultWorker struct {
	id   string
	conn *transport.Connection

	heartbeat   chan struct{}
	toQueue     chan types.ReceivedMessage
	toForwarder chan types.ReceivedMessage

	processedCount *atomic.Uint64
	droppedCount   *atomic.Uint64
	outgoingCount  *atomic.Uint64
	bufferDepth    *atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	config *WorkerConfig
	logger *slog.Logger
}

func NewWorker(
	ctx context.Context,
	id string,
	conn *transport.Connection,
	config *WorkerConfig,
	logger *slog.Logger,
) (*DefaultWorker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}
	if err := ValidateWorkerConfig(config); err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	return &DefaultWorker{
		id:             id,
		conn:           conn,
		heartbeat:      make(chan struct{}),
		toQueue:        make(chan types.ReceivedMessage, 256),
		toForwarder:    make(chan types.ReceivedMessage, 256),
		processedCount: &atomic.Uint64{},
		droppedCount:   &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		bufferDepth:    &atomic.Int64{},
		config:         config,
		ctx:            wctx,
		cancel:         cancel,
		logger:         logger,
	}, nil
}

func (w *DefaultWorker) Start(pool PoolPlugin) {
	if w.logger != nil {
		w.logger.Debug("starting")
	}

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer wg.Done()
		RunReader(w.ctx, pool.OnExit, w.conn, w.toQueue, w.heartbeat, w.logger)
	}()

	go func() {
		defer wg.Done()
		RunHeartbeatForwarder(w.ctx, w.conn, w.heartbeat, w.logger)
	}()

	go func() {
		defer wg.Done()
		queue.RunQueue(w.id, w.ctx, w.toQueue, w.toForwarder, w.config.MaxQueueSize, w.droppedCount, w.bufferDepth)
	}()

	go func() {
		defer wg.Done()
		RunForwarder(w.id, w.ctx, w.toForwarder, pool.Inbox, w.processedCount, pool.InboxCounter)
	}()

	go func() {
		defer wg.Done()
		RunWatchdog(w.ctx, pool.OnExit, w.heartbeat, w.config.InactivityTimeout, w.logger)
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
	if err := w.conn.Send(data); err != nil {
		return err
	}

	select {
	case w.heartbeat <- struct{}{}:
	case <-w.ctx.Done():
	}

	w.outgoingCount.Add(1)

	return nil
}

func (w *DefaultWorker) Stats() WorkerStats {
	return WorkerStats{
		ChanIncoming:  len(w.conn.Incoming()),
		ChanQueue:     len(w.toQueue),
		ChanForwarder: len(w.toForwarder),
		BufferDepth:   w.bufferDepth.Load(),

		TotalProcessed: w.processedCount.Load(),
		TotalDropped:   w.droppedCount.Load(),
		TotalSent:      w.outgoingCount.Load(),
	}
}

func RunReader(
	ctx context.Context,
	onPeerClose OnExitFunction,

	conn *transport.Connection,
	messages chan<- types.ReceivedMessage,
	heartbeat chan<- struct{},

	logger *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-conn.Incoming():
			if !ok {
				var err error
				// determine exit kind
				// by default, the peer dropped unexpectedly
				kind := ExitUnexpectedClose
				select {
				// the peer-side error is sent before the connection is closed,
				// so a non-blocking call here is correct
				// if an error is not sent, then assume the default event kind
				case err = <-conn.Errors():
					if errors.Is(err, transport.ErrPeerClosedClean) {
						kind = ExitDisconnected
					}
					if errors.Is(err, transport.ErrPeerClosedUnexpected) {
						kind = ExitUnexpectedClose
					}
					if errors.Is(err, transport.ErrReadError) {
						kind = ExitReadError
					}
				default:
				}

				if logger != nil {
					if kind == ExitUnexpectedClose || kind == ExitReadError {
						logger.Error("reader: peer dropped", "event", kind, "error", err)
					} else {
						logger.Info("reader: peer disconnected", "event", kind)
					}
				}

				onPeerClose(kind)
				return
			}

			messages <- types.ReceivedMessage{Data: data, ReceivedAt: time.Now()}

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

func RunForwarder(
	id string,
	ctx context.Context,
	messages <-chan types.ReceivedMessage,
	inbox chan<- types.InboxMessage,
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

			case inbox <- types.InboxMessage{
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

func RunWatchdog(
	ctx context.Context,
	onInactive OnExitFunction,
	heartbeat <-chan struct{},
	timeout time.Duration,
	logger *slog.Logger,
) {
	// disable watchdog timeout if not configured
	if timeout <= 0 {
		if logger != nil {
			logger.Debug("watchdog: disabled")
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
		logger.Debug("watchdog: enabled", "timeout", timeout)
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
			// signal peer is inactive
			if logger != nil {
				logger.Info("watchdog: no activity observed")
			}
			onInactive(ExitPolicy)
			return
		}
	}
}
