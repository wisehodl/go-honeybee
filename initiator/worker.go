package initiator

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
	id     string
	stop   <-chan struct{}
	config *WorkerConfig
	conn   *transport.Connection
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
		id:     id,
		stop:   stop,
		config: config,
	}

	return w, nil
}

func (w *Worker) Send(data []byte) error {
	return w.conn.Send(data)
}

func (w *Worker) Start(
	ctx WorkerContext,
	wg *sync.WaitGroup,
) {
}

func (w *Worker) runReader(
	conn *transport.Connection,
	messages chan<- receivedMessage,
	heartbeat chan<- struct{},
	reconnect chan<- struct{},
	newConn <-chan *transport.Connection,
	stop <-chan struct{},
	poolDone <-chan struct{},

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

func (w *Worker) runHealthMonitor(
	heartbeat <-chan struct{},
	reconnect chan<- struct{},
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
	// disable if idle timeout is disabled
	if w.config.IdleTimeout <= 0 {
		// wait for stop signal and exit
		select {
		case <-stop:
		case <-poolDone:
		}
		return
	}

	timer := time.NewTimer(w.config.IdleTimeout)
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
			timer.Reset(w.config.IdleTimeout)
		// timer completed
		case <-timer.C:
			// initiate reconnect, then reset the timer
			// multiple reconnect signals during reconnection is idempotent
			select {
			case reconnect <- struct{}{}:
			default:
			}
			timer.Reset(w.config.IdleTimeout)
		}
	}
}

func (w *Worker) runReconnector(
	reconnect <-chan struct{},
	newConn chan<- *transport.Connection,
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
}
