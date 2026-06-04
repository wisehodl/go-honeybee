package honeybee

import (
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func makeWorkerContext(t *testing.T) (
	inbox chan types.InboxMessage,
	events chan PoolEvent,
	pool PoolPlugin,
) {
	t.Helper()
	inbox = make(chan types.InboxMessage, 256)
	events = make(chan PoolEvent, 10)
	pool = PoolPlugin{
		Inbox:            inbox,
		Events:           events,
		InboxCounter:     &atomic.Uint64{},
		ConnectionConfig: *transport.GetDefaultConnectionConfig(),
	}
	return
}

func makeWorker(t *testing.T, ctx context.Context, cancel context.CancelFunc) *DefaultWorker {
	t.Helper()
	config, _ := NewWorkerConfig(
		WithReconnectDelay(0 * time.Second),
	)
	return &DefaultWorker{
		ctx:            ctx,
		cancel:         cancel,
		id:             "wss://test",
		config:         config,
		sendHeartbeat:  make(chan struct{}),
		processedCount: &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		restartCount:   &atomic.Uint64{},
	}
}

func mockDialer(socket *honeybeetest.MockSocket) *honeybeetest.MockDialer {
	return &honeybeetest.MockDialer{
		DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
			return socket, nil, nil
		},
	}
}

func TestWorkerSession(t *testing.T) {
	t.Run("EventConnected emitted after dial succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.ID == w.id && e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")
	})

	t.Run("dial failure exhausted - session stays alive, no events emitted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)

		cc, _ := transport.NewConnectionConfig(transport.WithRetryDisabled())
		pool.ConnectionConfig = *cc
		pool.ConnectionConfig.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, errors.New("connection refused")
			},
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected || e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected no connected/disconnected events when dial fails")

		// worker goroutine is still running
		assert.False(t, func() bool {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return true
			case <-time.After(50 * time.Millisecond):
				return false
			}
		}(), "expected worker to still be running after dial failure")
	})

	t.Run("keepalive fires before connection - dial is cancelled and replaced", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewWorkerConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(20*time.Millisecond),
		)
		w := &DefaultWorker{
			ctx:            ctx,
			cancel:         cancel,
			id:             "wss://test",
			config:         config,
			sendHeartbeat:  make(chan struct{}),
			processedCount: &atomic.Uint64{},
			outgoingCount:  &atomic.Uint64{},
			restartCount:   &atomic.Uint64{},
		}
		_, _, pool := makeWorkerContext(t)

		var dialCount atomic.Uint64
		cc, _ := transport.NewConnectionConfig(transport.WithRetryDisabled())
		pool.ConnectionConfig = *cc
		pool.ConnectionConfig.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(dialCtx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
				dialCount.Add(1)
				<-dialCtx.Done()
				return nil, nil, dialCtx.Err()
			},
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		// keepalive fires after 20ms; a second dial goroutine must be spawned
		honeybeetest.Eventually(t, func() bool {
			return dialCount.Load() >= 2
		}, "expected at least two dial attempts after keepalive fired")
	})

	t.Run("Stop before connection established - exits cleanly, no events", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)

		cc, _ := transport.NewConnectionConfig(transport.WithRetryDisabled())
		pool.ConnectionConfig = *cc
		pool.ConnectionConfig.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(dialCtx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
				<-dialCtx.Done()
				return nil, nil, dialCtx.Err()
			},
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		w.Stop()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected Start to return after Stop")

		assert.Empty(t, events, "expected no events when stopped before connection")
	})

	t.Run("Send delivers data to socket", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)
		_, mockSocket, _, outgoingData := setupTestConnection(t)
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		err := w.Send([]byte("hello"))
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return string(msg.Data) == "hello"
			default:
				return false
			}
		}, "expected data on socket")
	})

	t.Run("socket data arrives on Inbox", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		inbox, events, pool := makeWorkerContext(t)

		incomingData := make(chan honeybeetest.MockIncomingData, 10)
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() { close(mockSocket.Closed) })
			return nil
		}

		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			select {
			case data := <-incomingData:
				return data.MsgType, data.Data, data.Err
			}
		}

		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    []byte("hello"),
		}

		var received types.InboxMessage
		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				received = msg
				return true
			default:
				return false
			}
		}, "expected message on Inbox")
		assert.Equal(t, w.id, received.ID)
		assert.Equal(t, []byte("hello"), received.Data)
		assert.False(t, received.ReceivedAt.IsZero(), "expected non-zero ReceivedAt")
	})

	t.Run("sustained incoming messages reset keepalive - no disconnect", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewWorkerConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(60*time.Millisecond),
		)
		w := &DefaultWorker{
			ctx:            ctx,
			cancel:         cancel,
			id:             "wss://test",
			config:         config,
			sendHeartbeat:  make(chan struct{}),
			processedCount: &atomic.Uint64{},
			outgoingCount:  &atomic.Uint64{},
			restartCount:   &atomic.Uint64{},
		}
		_, events, pool := makeWorkerContext(t)
		_, mockSocket, incomingData, _ := setupTestConnection(t)
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		// send messages every 20ms for 100ms — well within the 60ms timeout each cycle
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					select {
					case incomingData <- honeybeetest.MockIncomingData{MsgType: websocket.TextMessage, Data: []byte("ping")}:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected no EventDisconnected while messages are arriving")
	})

	t.Run("pong heartbeat resets keepalive - no disconnect", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewWorkerConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(60*time.Millisecond),
		)
		w := &DefaultWorker{
			ctx:            ctx,
			cancel:         cancel,
			id:             "wss://test",
			config:         config,
			sendHeartbeat:  make(chan struct{}),
			processedCount: &atomic.Uint64{},
			outgoingCount:  &atomic.Uint64{},
			restartCount:   &atomic.Uint64{},
		}
		_, events, pool := makeWorkerContext(t)

		// socket whose pong handler fires every 20ms; no incoming messages
		var pongHandler func(string) error
		mockSocket, incomingData, _ := honeybeetest.SetupTestSocket(t)
		mockSocket.SetPongHandlerFunc = func(h func(string) error) { pongHandler = h }
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		// fire pong every 20ms — well within the 60ms keepalive window
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if pongHandler != nil {
						_ = pongHandler("")
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected no EventDisconnected while pongs are arriving")

		_ = incomingData // kept open to prevent reader EOF
	})

	t.Run("keepalive fires while connected - EventDisconnected emitted and redial begins", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewWorkerConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(30*time.Millisecond),
		)
		w := &DefaultWorker{
			ctx:            ctx,
			cancel:         cancel,
			id:             "wss://test",
			config:         config,
			sendHeartbeat:  make(chan struct{}),
			processedCount: &atomic.Uint64{},
			outgoingCount:  &atomic.Uint64{},
			restartCount:   &atomic.Uint64{},
		}
		_, events, pool := makeWorkerContext(t)
		_, mockSocket, _, _ := setupTestConnection(t)
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		// no activity — keepalive fires after 30ms
		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected EventDisconnected after keepalive timeout")

		// session must redial — a second EventConnected follows
		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected after redial")
	})

	t.Run("socket close produces EventDisconnected then EventConnected", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)
		_, mockSocket, incomingData, _ := setupTestConnection(t)
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		close(incomingData)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected EventDisconnected")

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected second EventConnected")
	})

	t.Run("connection pointer is nil between disconnect and reconnect", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)
		_, mockSocket, incomingData, _ := setupTestConnection(t)
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		close(incomingData)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected EventDisconnected")

		// conn.Store(nil) happens before EventDisconnected is sent
		assert.Nil(t, w.conn.Load(), "expected connection pointer to be nil after disconnect")
	})

	t.Run("Stop produces EventDisconnected and wg drains", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		w.Stop()

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected EventDisconnected")

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected wg to drain")
	})

	t.Run("parent context cancel exits cleanly and wg drains", func(t *testing.T) {
		parentCtx, parentCancel := context.WithCancel(context.Background())
		workerCtx, workerCancel := context.WithCancel(parentCtx)

		w := makeWorker(t, workerCtx, workerCancel)
		_, events, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		// drain events after parent cancel — we don't assert what they are,
		// only that the worker exits
		parentCancel()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected wg to drain after parent cancel")
	})

	t.Run("EventDialFailed emitted with correct ID, non-nil Err, and non-zero At when dialer fails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)

		cc, _ := transport.NewConnectionConfig(transport.WithRetryDisabled())
		pool.ConnectionConfig = *cc
		dialErr := errors.New("connection refused")
		pool.ConnectionConfig.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, dialErr
			},
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed &&
					e.ID == w.id &&
					e.Err != nil &&
					!e.At.IsZero()
			default:
				return false
			}
		}, "expected EventDialFailed")
	})

	t.Run("no EventDialFailed when first dial succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.ConnectionConfig.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		// drain any remaining events and assert none are EventDialFailed
		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed
			default:
				return false
			}
		}, "expected no EventDialFailed when first dial succeeds")
	})

	t.Run("no EventDialFailed when worker is stopped mid-dial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, pool := makeWorkerContext(t)

		cc, _ := transport.NewConnectionConfig(transport.WithRetryDisabled())
		pool.ConnectionConfig = *cc
		pool.ConnectionConfig.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(dialCtx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
				<-dialCtx.Done()
				return nil, nil, dialCtx.Err()
			},
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		w.Stop()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected worker to exit after Stop")

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed
			default:
				return false
			}
		}, "expected no EventDialFailed when stopped mid-dial")
	})

	t.Run("no EventDialFailed when keepalive replaces an in-progress dial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewWorkerConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(20*time.Millisecond),
		)
		w := &DefaultWorker{
			ctx:            ctx,
			cancel:         cancel,
			id:             "wss://test",
			config:         config,
			sendHeartbeat:  make(chan struct{}),
			processedCount: &atomic.Uint64{},
			outgoingCount:  &atomic.Uint64{},
			restartCount:   &atomic.Uint64{},
		}
		_, events, pool := makeWorkerContext(t)

		var dialCount atomic.Uint64
		cc, _ := transport.NewConnectionConfig(transport.WithRetryDisabled())
		pool.ConnectionConfig = *cc
		pool.ConnectionConfig.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(dialCtx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
				dialCount.Add(1)
				<-dialCtx.Done()
				return nil, nil, dialCtx.Err()
			},
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		// keepalive fires after 20ms, cancelling the first dial and spawning a second
		honeybeetest.Eventually(t, func() bool {
			return dialCount.Load() >= 2
		}, "expected at least two dial attempts after keepalive fired")

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed
			default:
				return false
			}
		}, "expected no EventDialFailed when keepalive replaces dial")
	})
}

func TestWorkerSend(t *testing.T) {
	t.Run("data sent to mock socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})
		heartbeatCount := atomic.Int32{}

		w := &DefaultWorker{
			ctx:           ctx,
			cancel:        cancel,
			id:            "wss://test",
			sendHeartbeat: heartbeat,
			outgoingCount: &atomic.Uint64{},
		}
		w.conn.Store(conn)
		defer w.cancel()

		go func() {
			for range heartbeat {
				heartbeatCount.Add(1)
			}
		}()

		testData := []byte("hello")
		err := w.Send(testData)
		assert.NoError(t, err)

		// at least one heartbeat was sent
		honeybeetest.Eventually(t, func() bool {
			return heartbeatCount.Load() >= 1
		}, "expected heartbeats")

		// message was sent by the socket
		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return string(msg.Data) == "hello"
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("sends one heartbeat per successful send", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)
		defer conn.Close()

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})
		heartbeatCount := atomic.Int32{}

		w := &DefaultWorker{
			ctx:           ctx,
			cancel:        cancel,
			id:            "wss://test",
			sendHeartbeat: heartbeat,
			outgoingCount: &atomic.Uint64{},
		}
		w.conn.Store(conn)
		defer w.cancel()

		go func() {
			for range heartbeat {
				heartbeatCount.Add(1)
			}
		}()

		const count = 3
		for i := range count {
			err := w.Send(fmt.Appendf(nil, "msg-%d", i))
			assert.NoError(t, err)
		}

		honeybeetest.Eventually(t, func() bool {
			return heartbeatCount.Load() == count
		}, "expected heartbeats")
	})

	t.Run("returns error if connection is unavailable", func(t *testing.T) {
		// no connection available to worker

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})

		w := &DefaultWorker{
			ctx:           ctx,
			cancel:        cancel,
			id:            "wss://test",
			sendHeartbeat: heartbeat,
		}
		defer w.cancel()

		go func() {
			for range heartbeat {
			}
		}()

		err := w.Send([]byte("hello"))
		assert.ErrorIs(t, err, ErrConnectionUnavailable)
	})
}
