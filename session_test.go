package honeybee

import (
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func makeSessionContext(t *testing.T) (
	inbox chan types.InboxMessage,
	events chan PoolEvent,
	pool PoolPlugin,
) {
	t.Helper()
	inbox = make(chan types.InboxMessage, 256)
	events = make(chan PoolEvent, 10)
	pool = PoolPlugin{
		Inbox:        inbox,
		Events:       events,
		InboxCounter: &atomic.Uint64{},
		Retire:       func(_ error) {},
	}
	return
}

func makeSession(
	t *testing.T,
	socket *honeybeetest.MockSocket,
	config *SessionConfig,
	ctx context.Context,
	cancel context.CancelFunc,
) *session {
	t.Helper()
	if config == nil {
		config, _ = NewSessionConfig(WithReconnectDelay(0 * time.Second))
	}
	dialFn := func(_ context.Context) (types.Socket, error) { return socket, nil }
	return &session{
		ctx:            ctx,
		cancel:         cancel,
		url:            "wss://test",
		dialFn:         dialFn,
		config:         *config,
		sendHeartbeat:  make(chan struct{}),
		processedCount: &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		restartCount:   &atomic.Uint64{},
	}
}

func TestSession(t *testing.T) {
	t.Run("EventConnected emitted after dial succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockSocket := honeybeetest.NewMockSocket()
		w := makeSession(t, mockSocket, nil, ctx, cancel)
		_, events, pool := makeSessionContext(t)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.URL == w.url && e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")
	})

	t.Run("EventDisconnected carries ErrReadLimit when read limit exceeded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, websocket.ErrReadLimit
		}

		config, _ := NewSessionConfig(
			WithRetryDisabled(), WithReconnectDelay(0))
		w := makeSession(t, mockSocket, config, ctx, cancel)
		_, events, pool := makeSessionContext(t)

		var wg sync.WaitGroup
		wg.Go(func() {
			w.Start(pool)
		})

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected &&
					e.Err != nil &&
					errors.Is(e.Err, ErrReadLimit)
			default:
				return false
			}
		}, "expected EventDisconnected with Err matching ErrReadLimit")
	})

	t.Run("dial failure exhausted - session exits cleanly, no connected/disconnected events", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewSessionConfig(WithRetryDisabled())
		w := makeSession(t, nil, config, ctx, cancel)
		w.dialFn = func(_ context.Context) (types.Socket, error) {
			return nil, fmt.Errorf("connection refused")
		}
		_, events, pool := makeSessionContext(t)

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected session to exit after terminal dial failure")

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected || e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected no connected/disconnected events when dial fails")
	})

	t.Run("Retire called on terminal dial failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewSessionConfig(WithRetryDisabled())
		w := makeSession(t, nil, config, ctx, cancel)
		w.dialFn = func(_ context.Context) (types.Socket, error) {
			return nil, fmt.Errorf("connection refused")
		}
		_, events, pool := makeSessionContext(t)

		retired := atomic.Bool{}
		var retiredErr atomic.Pointer[error]
		pool.Retire = func(err error) { retiredErr.Store(&err); retired.Store(true) }

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			return retired.Load()
		}, "expected Retire to be called")

		assert.ErrorContains(t, *retiredErr.Load(), "connection refused", "expected dial error forwarded to Retire")

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected session to exit cleanly")

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected || e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected no connected/disconnected events")
	})

	t.Run("Retire not called when context cancelled during dial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeSession(t, nil, nil, ctx, cancel)
		w.dialFn = func(dialCtx context.Context) (types.Socket, error) {
			<-dialCtx.Done()
			return nil, dialCtx.Err()
		}
		_, _, pool := makeSessionContext(t)

		retired := atomic.Bool{}
		pool.Retire = func(_ error) { retired.Store(true) }

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
		}, "expected session to exit after Stop")

		assert.False(t, retired.Load(), "expected Retire not called")
	})

	t.Run("Retire not called on successful connection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockSocket := honeybeetest.NewMockSocket()
		w := makeSession(t, mockSocket, nil, ctx, cancel)
		_, events, pool := makeSessionContext(t)

		retired := atomic.Bool{}
		pool.Retire = func(_ error) { retired.Store(true) }

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected without Retire being called")

		assert.False(t, retired.Load(), "expected Retire not called")
	})

	t.Run("Stop before connection established - exits cleanly, dial failed event", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeSession(t, nil, nil, ctx, cancel)
		w.dialFn = func(dialCtx context.Context) (types.Socket, error) {
			<-dialCtx.Done()
			return nil, dialCtx.Err()
		}
		_, events, pool := makeSessionContext(t)

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

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed
			default:
				return false
			}
		}, "expected EventDialFailed when stopped before connection")
	})

	t.Run("Send delivers data to socket", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, mockSocket, _, outgoingData := setupTestConnection(t)
		w := makeSession(t, mockSocket, nil, ctx, cancel)
		_, events, pool := makeSessionContext(t)

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

		w := makeSession(t, mockSocket, nil, ctx, cancel)
		inbox, events, pool := makeSessionContext(t)

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
		assert.Equal(t, w.url, received.URL)
		assert.Equal(t, []byte("hello"), received.Data)
		assert.False(t, received.ReceivedAt.IsZero(), "expected non-zero ReceivedAt")
	})

	t.Run("sustained incoming messages reset keepalive - no disconnect", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config, _ := NewSessionConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(60*time.Millisecond),
		)
		_, mockSocket, incomingData, _ := setupTestConnection(t)
		w := makeSession(t, mockSocket, config, ctx, cancel)
		_, events, pool := makeSessionContext(t)

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

		// socket whose pong handler fires every 20ms; no incoming messages
		var pongHandler func(string) error
		mockSocket, incomingData, _ := honeybeetest.SetupTestSocket(t)
		mockSocket.SetPongHandlerFunc = func(h func(string) error) { pongHandler = h }

		config, _ := NewSessionConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(60*time.Millisecond),
		)
		w := makeSession(t, mockSocket, config, ctx, cancel)
		_, events, pool := makeSessionContext(t)

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

		config, _ := NewSessionConfig(
			WithReconnectDelay(0),
			WithKeepaliveTimeout(30*time.Millisecond),
		)
		_, mockSocket, _, _ := setupTestConnection(t)
		w := makeSession(t, mockSocket, config, ctx, cancel)
		_, events, pool := makeSessionContext(t)

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

		_, events, pool := makeSessionContext(t)
		_, mockSocket, incomingData, _ := setupTestConnection(t)
		w := makeSession(t, mockSocket, nil, ctx, cancel)

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

	t.Run("Stop produces EventDisconnected and wg drains", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, events, pool := makeSessionContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		w := makeSession(t, mockSocket, nil, ctx, cancel)

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
		sessionCtx, sessionCancel := context.WithCancel(parentCtx)

		_, events, pool := makeSessionContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		w := makeSession(t, mockSocket, nil, sessionCtx, sessionCancel)

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
		// only that the session exits
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

	t.Run("EventDialFailed emitted when dialer fails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeSession(t, nil, nil, ctx, cancel)
		_, events, pool := makeSessionContext(t)

		dialErr := errors.New("connection refused")
		w.dialFn = func(_ context.Context) (types.Socket, error) {
			return nil, dialErr
		}

		var wg sync.WaitGroup
		wg.Go(func() { w.Start(pool) })

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed &&
					e.URL == w.url &&
					e.Err == dialErr &&
					!e.At.IsZero()
			default:
				return false
			}
		}, "expected EventDialFailed")
	})

	t.Run("EventDialFailed when session is stopped mid-dial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeSession(t, nil, nil, ctx, cancel)
		_, events, pool := makeSessionContext(t)

		w.dialFn = func(dialCtx context.Context) (types.Socket, error) {
			<-dialCtx.Done()
			return nil, dialCtx.Err()
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
		}, "expected session to exit after Stop")

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDialFailed
			default:
				return false
			}
		}, "expected EventDialFailed when stopped mid-dial")
	})
}

func TestSession_Send(t *testing.T) {
	t.Run("data sent to mock socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})
		heartbeatCount := atomic.Int32{}

		w := &session{
			ctx:           ctx,
			cancel:        cancel,
			url:           "wss://test",
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

		w := &session{
			ctx:           ctx,
			cancel:        cancel,
			url:           "wss://test",
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
		// no connection available to session

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})

		w := &session{
			ctx:           ctx,
			cancel:        cancel,
			url:           "wss://test",
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

func TestSessionResetCancel(t *testing.T) {
	w := &session{
		config:       SessionConfig{ReconnectDelay: 30 * time.Second},
		restartCount: &atomic.Uint64{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)

	go func() {
		done <- w.reset(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	honeybeetest.Eventually(t, func() bool {
		select {
		case ok := <-done:
			return !ok
		default:
			return false
		}
	}, "expected reset to return false promptly after cancel")
}
