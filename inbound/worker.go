package inbound

import (
	"context"
	"errors"
	"git.wisehodl.dev/jay/go-honeybee/queue"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"time"
)

type Worker interface {
	Start(pool PoolPlugin)
	Stop()
	Send(data []byte) error
}

type WorkerExitKind string

const (
	ExitDisconnected WorkerExitKind = "disconnected"
	ExitError        WorkerExitKind = "error"
	ExitPolicy       WorkerExitKind = "policy"
)

type DefaultWorker struct {
	id        string
	conn      *transport.Connection
	heartbeat chan struct{}
	config    *WorkerConfig
	ctx       context.Context
	cancel    context.CancelFunc
	logger    *slog.Logger
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
		id:        id,
		conn:      conn,
		heartbeat: make(chan struct{}),
		config:    config,
		ctx:       wctx,
		cancel:    cancel,
		logger:    logger,
	}, nil
}

func (w *DefaultWorker) Start(pool PoolPlugin) {
	if w.logger != nil {
		w.logger.Debug("starting")
	}

	toQueue := make(chan types.ReceivedMessage, 256)
	toForwarder := make(chan types.ReceivedMessage, 256)

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		RunReader(w.ctx, pool.OnExit, w.conn, toQueue, w.heartbeat, w.logger)
	}()

	go func() {
		defer wg.Done()
		queue.RunQueue(w.id, w.ctx, toQueue, toForwarder, w.config.MaxQueueSize)
	}()

	go func() {
		defer wg.Done()
		RunForwarder(w.id, w.ctx, toForwarder, pool.Inbox)
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

	return nil
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
				kind := ExitError
				select {
				// the peer-side error is sent before the connection is closed,
				// so a non-blocking call here is correct
				// if an error is not sent, then assume the default event kind
				case err = <-conn.Errors():
					if errors.Is(err, transport.ErrPeerClosedClean) {
						kind = ExitDisconnected
					}
				default:
				}

				if logger != nil {
					if kind == ExitError {
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

func RunForwarder(
	id string,
	ctx context.Context,
	messages <-chan types.ReceivedMessage,
	inbox chan<- InboxMessage,
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
				logger.Info("watchdog: peer is inactive")
			}
			onInactive(ExitPolicy)
			return
		}
	}
}
