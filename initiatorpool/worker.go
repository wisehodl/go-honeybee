package initiatorpool

import (
	"container/list"
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
	id       string
	stop     <-chan struct{}
	config   *WorkerConfig
	outbound chan []byte
}

func NewWorker(
	id string,
	stop <-chan struct{},
	config *WorkerConfig,

) (*Worker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}

	err := ValidateWorkerConfig(config)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		id:       id,
		stop:     stop,
		outbound: make(chan []byte, 64),
		config:   config,
	}

	return w, nil
}

func (w *Worker) dial(ctx WorkerContext) (*transport.Connection, error) {
	conn, err := transport.NewConnection(w.id, ctx.ConnectionConfig, ctx.Logger)
	if err != nil {
		return nil, err
	}

	conn.SetDialer(ctx.Dialer)
	return conn, conn.Connect()
}

func (w *Worker) Send(data []byte) error {
	select {
	case w.outbound <- data:
		return nil
	case <-w.stop:
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

func (w *Worker) runSession(
	messages chan<- receivedMessage,
	heartbeat chan<- struct{},
	dial chan<- struct{},

	keepalive <-chan struct{},
	outbound <-chan []byte,
	newConn <-chan *transport.Connection,

	ctx WorkerContext,

	workerStop <-chan struct{},
	poolDone <-chan struct{},
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
	conn *transport.Connection,
	keepalive <-chan struct{},
	workerStop <-chan struct{},
	poolDone <-chan struct{},
	sessionDone <-chan struct{},
	onStop func(),
) {
}

func (w *Worker) runForwarder(
	messages <-chan receivedMessage,
	inbox chan<- InboxMessage,
	stop <-chan struct{},
	poolDone <-chan struct{},
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
		case <-stop:
			return
		case <-poolDone:
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
	heartbeat <-chan struct{},
	keepalive chan<- struct{},
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
	// disable keepalive timeout if not configured
	if w.config.KeepaliveTimeout <= 0 {
		// wait for stop signal and exit
		select {
		case <-stop:
		case <-poolDone:
		}
		return
	}

	timer := time.NewTimer(w.config.KeepaliveTimeout)
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-poolDone:
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

func (w *Worker) runDialer(
	dial <-chan struct{},
	newConn chan<- *transport.Connection,
	ctx WorkerContext,
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
}
