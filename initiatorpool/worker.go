package initiatorpool

import (
	"container/list"
	"context"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"sync"
	"time"
)

// Worker

type receivedMessage struct {
	data       []byte
	receivedAt time.Time
}

type Worker struct {
	ctx      context.Context
	cancel   context.CancelFunc
	id       string
	config   *WorkerConfig
	outbound chan []byte
}

func NewWorker(
	ctx context.Context,
	id string,
	config *WorkerConfig,

) (*Worker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}

	err := ValidateWorkerConfig(config)
	if err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &Worker{
		ctx:      wctx,
		cancel:   cancel,
		id:       id,
		outbound: make(chan []byte, 64),
		config:   config,
	}

	return w, nil
}

func (w *Worker) Send(data []byte) error {
	select {
	case w.outbound <- data:
		return nil
	case <-w.ctx.Done():
		return NewWorkerError(w.id, "worker is stopped")
	default:
		return NewWorkerError(w.id, "outbound queue full")
	}
}

func (w *Worker) Start(
	ctx WorkerContext,
	wg *sync.WaitGroup,
) {
}

func (w *Worker) Stop() {
	w.cancel()
}

func (w *Worker) runSession(
	ctx context.Context,
	wctx WorkerContext,

	messages chan<- receivedMessage,
	heartbeat chan<- struct{},
	dial chan<- struct{},

	keepalive <-chan struct{},
	outbound <-chan []byte,
	newConn <-chan *transport.Connection,
) {
}

func (w *Worker) runReader(
	conn *transport.Connection,
	messages chan<- receivedMessage,
	heartbeat chan<- struct{},
	sessionDone <-chan struct{},
	onStop func(),
) {
}

func (w *Worker) runWriter(
	conn *transport.Connection,
	outbound <-chan []byte,
	heartbeat chan<- struct{},
	sessionDone <-chan struct{},
	onStop func(),
) {
}

func (w *Worker) runStopMonitor(
	ctx context.Context,
	conn *transport.Connection,
	keepalive <-chan struct{},
	sessionDone <-chan struct{},
	onStop func(),
) {
}

func (w *Worker) runForwarder(
	ctx context.Context,
	messages <-chan receivedMessage,
	inbox chan<- InboxMessage,
	maxQueueSize int,
) {
	queue := list.New()

	for {
		var out chan<- InboxMessage
		var next receivedMessage

		// enable inbox if it is populated
		if queue.Len() > 0 {
			out = inbox

			// read the first message in the queue
			next = queue.Front().Value.(receivedMessage)
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
			ID:         w.id,
			Data:       next.data,
			ReceivedAt: next.receivedAt,
		}:
			// drop message from queue
			queue.Remove(queue.Front())
		}
	}
}

func (w *Worker) runKeepalive(
	ctx context.Context,
	heartbeat <-chan struct{},
	keepalive chan<- struct{},
) {
	// disable keepalive timeout if not configured
	if w.config.KeepaliveTimeout <= 0 {
		// wait for cancel and exit
		select {
		case <-ctx.Done():
		}
		return
	}

	timer := time.NewTimer(w.config.KeepaliveTimeout)
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
			timer.Reset(w.config.KeepaliveTimeout)
		// timer completed
		case <-timer.C:
			// send keepalive signal, then reset the timer
			select {
			case keepalive <- struct{}{}:
			default:
			}
			timer.Reset(w.config.KeepaliveTimeout)
		}
	}
}

func (w *Worker) dial(
	ctx context.Context,
	wctx WorkerContext,
) (*transport.Connection, error) {
	conn, err := transport.NewConnection(w.id, wctx.ConnectionConfig, wctx.Logger)
	if err != nil {
		return nil, err
	}

	conn.SetDialer(wctx.Dialer)
	return conn, conn.Connect(ctx)
}

func (w *Worker) runDialer(
	ctx context.Context,
	wctx WorkerContext,

	dial <-chan struct{},
	newConn chan<- *transport.Connection,
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

			// dial a new connection
			conn, err := w.dial(ctx, wctx)
			close(done)

			// send error if dial failed and continue
			if err != nil {
				select {
				case wctx.Errors <- err:
				case <-ctx.Done():
				}
				continue
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
