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

type Worker interface {
	Start(wctx WorkerContext, wg *sync.WaitGroup)
	Stop()
	Send(data []byte) error
}

type ReceivedMessage struct {
	data       []byte
	receivedAt time.Time
}

type DefaultWorker struct {
	Ctx    context.Context
	Cancel context.CancelFunc

	Id     string
	Config *WorkerConfig

	Conn      atomic.Pointer[transport.Connection]
	Heartbeat chan struct{}
}

func NewWorker(
	ctx context.Context,
	id string,
	config *WorkerConfig,

) (*DefaultWorker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}

	err := ValidateWorkerConfig(config)
	if err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &DefaultWorker{
		Ctx:       wctx,
		Cancel:    cancel,
		Id:        id,
		Config:    config,
		Heartbeat: make(chan struct{}),
	}

	return w, nil
}

func (w *DefaultWorker) Start(
	wctx WorkerContext,
	wg *sync.WaitGroup,
) {
	dial := make(chan struct{}, 1)
	newConn := make(chan *transport.Connection, 1)
	messages := make(chan ReceivedMessage, 256)
	keepalive := make(chan struct{}, 1)

	var owg sync.WaitGroup
	owg.Add(4)

	go func() { defer owg.Done(); w.RunDialer(w.Ctx, wctx, dial, newConn) }()
	go func() { defer owg.Done(); w.RunKeepalive(w.Ctx, keepalive) }()
	go func() { defer owg.Done(); w.RunForwarder(w.Ctx, messages, wctx.Inbox, w.Config.MaxQueueSize) }()
	go func() { defer owg.Done(); w.RunSession(w.Ctx, wctx, messages, dial, keepalive, newConn) }()

	owg.Wait()
	wg.Done()
}

func (w *DefaultWorker) Stop() {
	w.Cancel()
}

func (w *DefaultWorker) Send(data []byte) error {
	conn := w.Conn.Load()
	if conn == nil {
		// connection not established by session
		return NewWorkerError(w.Id, ErrConnectionUnavailable)
	}

	err := conn.Send(data)

	if err != nil {
		return NewWorkerError(w.Id, err)
	}

	select {
	case w.Heartbeat <- struct{}{}:
	case <-w.Ctx.Done():
	}

	return nil
}

func (w *DefaultWorker) RunSession(
	ctx context.Context,
	wctx WorkerContext,

	messages chan<- ReceivedMessage,
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
		w.Conn.Store(conn)
		wctx.Events <- PoolEvent{ID: w.Id, Kind: EventConnected}

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
			w.RunReader(conn, messages, sessionDone, onStop)
		}()
		go func() {
			defer wg.Done()
			w.RunStopMonitor(ctx, conn, keepalive, sessionDone, onStop)
		}()

		// complete session
		wg.Wait()

		// tear down connection
		w.Conn.Store(nil)
		wctx.Events <- PoolEvent{ID: w.Id, Kind: EventDisconnected}

		// exit if worker is shutting down
		select {
		case <-ctx.Done():
			return
		default:
		}

		// refresh session
	}

}

func (w *DefaultWorker) RunReader(
	conn *transport.Connection,
	messages chan<- ReceivedMessage,
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
			messages <- ReceivedMessage{
				data:       data,
				receivedAt: time.Now(),
			}

			// send heartbeat
			select {
			case w.Heartbeat <- struct{}{}:
			case <-sessionDone:
				return
			}
		}
	}
}

func (w *DefaultWorker) RunStopMonitor(
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

func (w *DefaultWorker) RunForwarder(
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
			ID:         w.Id,
			Data:       next.data,
			ReceivedAt: next.receivedAt,
		}:
			// drop message from queue
			queue.Remove(queue.Front())
		}
	}
}

func (w *DefaultWorker) RunKeepalive(
	ctx context.Context,
	keepalive chan<- struct{},
) {
	// disable keepalive timeout if not configured
	if w.Config.KeepaliveTimeout <= 0 {
		// wait for cancel and exit
		select {
		case <-ctx.Done():
		}
		return
	}

	timer := time.NewTimer(w.Config.KeepaliveTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.Heartbeat:
			// drain the timer channel and reset
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(w.Config.KeepaliveTimeout)
		// timer completed
		case <-timer.C:
			// send keepalive signal, then reset the timer
			select {
			case keepalive <- struct{}{}:
			default:
			}
			timer.Reset(w.Config.KeepaliveTimeout)
		}
	}
}

func (w *DefaultWorker) Dial(
	ctx context.Context,
	wctx WorkerContext,
) (*transport.Connection, error) {
	conn, err := transport.NewConnection(w.Id, wctx.ConnectionConfig, wctx.Logger)
	if err != nil {
		return nil, err
	}

	conn.SetDialer(wctx.Dialer)
	return conn, conn.Connect(ctx)
}

func (w *DefaultWorker) RunDialer(
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
			conn, err := w.Dial(ctx, wctx)
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
