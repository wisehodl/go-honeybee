package inbound

import (
	"container/list"
	"context"
	"errors"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"sync"
	"time"
)

type Worker interface {
	Start(pool PoolPlugin, wg *sync.WaitGroup)
	Stop()
	Send(data []byte) error
}

type WorkerExitKind string

const (
	ExitCleanDisconnect WorkerExitKind = "disconnected"
	ExitUnexpectedDrop  WorkerExitKind = "dropped"
	ExitInactive        WorkerExitKind = "inactive"
)

type ReceivedMessage struct {
	data       []byte
	receivedAt time.Time
}

type DefaultWorker struct {
	id        string
	conn      *transport.Connection
	heartbeat chan struct{}
	config    *WorkerConfig
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewWorker(
	ctx context.Context,
	id string,
	conn *transport.Connection,
	config *WorkerConfig,
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
	}, nil
}

func (w *DefaultWorker) Start(pool PoolPlugin, wg *sync.WaitGroup) {
	messages := make(chan ReceivedMessage, 256)

	var owg sync.WaitGroup
	owg.Add(3)

	go func() {
		defer owg.Done()
		RunReader(w.ctx, pool.OnExit, w.conn, messages, w.heartbeat)
	}()

	go func() {
		defer owg.Done()
		RunForwarder(w.id, w.ctx, messages, pool.Inbox, w.config.MaxQueueSize)
	}()

	go func() {
		defer owg.Done()
		RunWatchdog(w.ctx, pool.OnExit, w.heartbeat, w.config.DeadTimeout)
	}()

	owg.Wait()
	wg.Done()
}

func (w *DefaultWorker) Stop() {
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
	messages chan<- ReceivedMessage,
	heartbeat chan<- struct{},
) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-conn.Incoming():
			if !ok {
				// determine exit kind
				// by default, the peer dropped unexpectedly
				kind := ExitUnexpectedDrop
				select {
				// the peer-side error is sent before the connection is closed,
				// so a non-blocking call here is correct
				// if an error is not sent, then assume the default event kind
				case err := <-conn.Errors():
					if errors.Is(err, transport.ErrPeerClosedClean) {
						kind = ExitCleanDisconnect
					}
				default:
				}

				onPeerClose(kind)
				return
			}

			messages <- ReceivedMessage{data: data, receivedAt: time.Now()}

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
	messages <-chan ReceivedMessage,
	inbox chan<- InboxMessage,
	maxQueueSize int,
) {
	queue := list.New()

	for {
		var out chan<- InboxMessage
		var next ReceivedMessage

		// enable inbox if it is populated
		if queue.Len() > 0 {
			out = inbox

			// read the first message in the queue
			next = queue.Front().Value.(ReceivedMessage)
		}

		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			// limit queue size if maximum is configured
			if maxQueueSize > 0 && queue.Len() >= maxQueueSize {
				// drop oldest message
				queue.Remove(queue.Front())
			}
			// add new message
			queue.PushBack(msg)
		// send next message to inbox
		case out <- InboxMessage{
			ID:         id,
			Data:       next.data,
			ReceivedAt: next.receivedAt,
		}:
			// drop message from queue
			queue.Remove(queue.Front())
		}
	}
}

func RunWatchdog(
	ctx context.Context,
	onInactive OnExitFunction,
	heartbeat <-chan struct{},
	timeout time.Duration,
) {
	// disable watchdog timeout if not configured
	if timeout <= 0 {
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
			onInactive(ExitInactive)
			return
		}
	}
}
