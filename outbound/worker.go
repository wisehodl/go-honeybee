package outbound

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
	Start(pool PoolPlugin, wg *sync.WaitGroup)
	Stop()
	Send(data []byte) error
}

type ReceivedMessage struct {
	data       []byte
	receivedAt time.Time
}

type DefaultWorker struct {
	id        string
	conn      atomic.Pointer[transport.Connection]
	heartbeat chan struct{}
	config    *WorkerConfig
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewWorker(
	ctx context.Context,
	id string,
	config *WorkerConfig,

) (*DefaultWorker, error) {
	if config == nil {
		config = GetDefaultWorkerConfig()
	}
	if err := ValidateWorkerConfig(config); err != nil {
		return nil, err
	}

	wctx, wcancel := context.WithCancel(ctx)
	w := &DefaultWorker{
		id:        id,
		config:    config,
		heartbeat: make(chan struct{}),
		ctx:       wctx,
		cancel:    wcancel,
	}

	return w, nil
}

func (w *DefaultWorker) Start(
	pool PoolPlugin,
	wg *sync.WaitGroup,
) {
	dial := make(chan struct{}, 1)
	newConn := make(chan *transport.Connection, 1)
	messages := make(chan ReceivedMessage, 256)
	keepalive := make(chan struct{}, 1)

	var owg sync.WaitGroup
	owg.Add(4)

	go func() {
		defer owg.Done()
		RunDialer(w.id, w.ctx, pool, dial, newConn)
	}()

	go func() {
		defer owg.Done()
		RunKeepalive(w.ctx, w.heartbeat, keepalive, w.config.KeepaliveTimeout)
	}()

	go func() {
		defer owg.Done()
		RunForwarder(w.id, w.ctx, messages, pool.Inbox, w.config.MaxQueueSize)
	}()

	go func() {
		defer owg.Done()
		session := &Session{
			id:        w.id,
			connPtr:   &w.conn,
			messages:  messages,
			heartbeat: w.heartbeat,
			dial:      dial,
			keepalive: keepalive,
			newConn:   newConn,
		}
		session.Start(w.ctx, pool)
	}()

	owg.Wait()
	wg.Done()
}

func (w *DefaultWorker) Stop() {
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

	return nil
}

type Session struct {
	id      string
	connPtr *atomic.Pointer[transport.Connection]

	messages  chan<- ReceivedMessage
	heartbeat chan<- struct{}
	dial      chan<- struct{}

	keepalive <-chan struct{}
	newConn   <-chan *transport.Connection
}

func (s *Session) Start(
	ctx context.Context,
	pool PoolPlugin,
) {
	for {
		// request new connection
		select {
		case s.dial <- struct{}{}:
		default:
		}

		// obtain new connection
		var conn *transport.Connection
	preConn:
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.keepalive:
				select {
				case s.dial <- struct{}{}:
				default:
				}
			case conn = <-s.newConn:
				break preConn
			}
		}

		// set up new connection
		s.connPtr.Store(conn)
		pool.Events <- PoolEvent{ID: s.id, Kind: EventConnected}

		// set up session context
		sctx, scancel := context.WithCancel(ctx)
		onStop := func() { scancel() }

		// start session
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			RunReader(sctx, onStop, conn, s.messages, s.heartbeat)
		}()
		go func() {
			defer wg.Done()
			RunStopMonitor(sctx, onStop, conn, s.keepalive)
		}()

		// complete session
		wg.Wait()

		// tear down connection
		s.connPtr.Store(nil)
		pool.Events <- PoolEvent{ID: s.id, Kind: EventDisconnected}

		// exit if worker is shutting down
		select {
		case <-ctx.Done():
			return
		default:
		}

		// refresh session
	}

}

func RunReader(
	ctx context.Context,
	onStop func(),
	conn *transport.Connection,
	messages chan<- ReceivedMessage,
	heartbeat chan<- struct{},
) {
	defer func() {
		conn.Close()
		onStop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-conn.Incoming():
			if !ok {
				// connection has closed
				return
			}

			// send message forward
			messages <- ReceivedMessage{data: data, receivedAt: time.Now()}

			// send heartbeat
			select {
			case heartbeat <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func RunStopMonitor(
	ctx context.Context,
	onStop func(),
	conn *transport.Connection,
	keepalive <-chan struct{},
) {
	defer func() {
		conn.Close()
		onStop()
	}()

	select {
	case <-ctx.Done():
	case <-keepalive:
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

func RunKeepalive(
	ctx context.Context,
	heartbeat <-chan struct{},
	keepalive chan<- struct{},
	timeout time.Duration,
) {
	// disable keepalive timeout if not configured
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
			// send keepalive signal, then reset the timer
			select {
			case keepalive <- struct{}{}:
			default:
			}
			timer.Reset(timeout)
		}
	}
}

func connect(
	id string,
	ctx context.Context,
	pool PoolPlugin,
) (*transport.Connection, error) {
	conn, err := transport.NewConnection(id, pool.ConnectionConfig, pool.Logger)
	if err != nil {
		return nil, err
	}

	conn.SetDialer(pool.Dialer)
	return conn, conn.Connect(ctx)
}

func RunDialer(
	id string,
	ctx context.Context,
	pool PoolPlugin,

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
			conn, err := connect(id, ctx, pool)
			close(done)

			// send error if dial failed and continue
			if err != nil {
				select {
				case pool.Errors <- err:
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
