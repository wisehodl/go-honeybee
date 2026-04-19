package initiatorpool

import (
	"container/list"
	"context"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"sync"
	"sync/atomic"
	"time"
)

// Worker

type receivedMessage struct {
	data       []byte
	receivedAt time.Time
}

type Worker struct {
	ctx    context.Context
	cancel context.CancelFunc

	id     string
	config *WorkerConfig

	conn      atomic.Pointer[transport.Connection]
	heartbeat chan struct{}
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
		ctx:       wctx,
		cancel:    cancel,
		id:        id,
		config:    config,
		heartbeat: make(chan struct{}),
	}

	return w, nil
}

func (w *Worker) Start(
	ctx WorkerContext,
	wg *sync.WaitGroup,
) {
}

func (w *Worker) Stop() {
	w.cancel()
}

func (w *Worker) Send(data []byte) error {
	conn := w.conn.Load()
	if conn == nil {
		// connection not established by session
		return NewWorkerError(w.id, ErrConnectionUnavailable)
	}

	err := conn.Send(data)

	if err != nil {
		return NewWorkerError(w.id, err)
	}

	select {
	case w.heartbeat <- struct{}{}:
	case <-w.ctx.Done():
	}

	return nil
}

func (w *Worker) runSession(
	ctx context.Context,
	wctx WorkerContext,

	messages chan<- receivedMessage,
	dial chan<- struct{},

	keepalive <-chan struct{},
	newConn <-chan *transport.Connection,
) {
	for {
		// request new connection
		select {
		case dial <- struct{}{}:
		default:
		}

		// obtain new connection
		var conn *transport.Connection
	preConn:
		for {
			select {
			case <-ctx.Done():
				return
			case <-keepalive:
				select {
				case dial <- struct{}{}:
				default:
				}
			case conn = <-newConn:
				break preConn
			}
		}

		// set up new connection
		w.conn.Store(conn)
		wctx.Events <- PoolEvent{ID: w.id, Kind: EventConnected}

		// set up session
		sessionDone := make(chan struct{})
		var once sync.Once
		onStop := func() {
			once.Do(func() { close(sessionDone) })
		}

		// start session
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			w.runReader(conn, messages, sessionDone, onStop)
		}()
		go func() {
			defer wg.Done()
			w.runStopMonitor(ctx, conn, keepalive, sessionDone, onStop)
		}()

		// complete session
		wg.Wait()

		// tear down connection
		w.conn.Store(nil)
		wctx.Events <- PoolEvent{ID: w.id, Kind: EventDisconnected}

		// exit if worker is shutting down
		select {
		case <-ctx.Done():
			return
		default:
		}

		// refresh session
	}

}

func (w *Worker) runReader(
	conn *transport.Connection,
	messages chan<- receivedMessage,
	sessionDone <-chan struct{},
	onStop func(),
) {
	defer func() {
		conn.Close()
		onStop()
	}()

	for {
		select {
		case <-sessionDone:
			return
		case data, ok := <-conn.Incoming():
			if !ok {
				// connection has closed
				return
			}

			// send message forward
			messages <- receivedMessage{
				data:       data,
				receivedAt: time.Now(),
			}

			// send heartbeat
			select {
			case w.heartbeat <- struct{}{}:
			case <-sessionDone:
				return
			}
		}
	}
}

func (w *Worker) runStopMonitor(
	ctx context.Context,
	conn *transport.Connection,
	keepalive <-chan struct{},
	sessionDone <-chan struct{},
	onStop func(),
) {
	defer func() {
		conn.Close()
		onStop()
	}()

	select {
	case <-ctx.Done():
	case <-keepalive:
	case <-sessionDone:
	}
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
		case <-w.heartbeat:
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
