package initiatorpool

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func drainEvent(t *testing.T, events <-chan PoolEvent, kind PoolEventKind) {
	t.Helper()
	honeybeetest.Eventually(t, func() bool {
		select {
		case e := <-events:
			return e.Kind == kind
		default:
			return false
		}
	}, fmt.Sprintf("expected %s event", kind))
}

func TestRunSessionDial(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		return
	}

	expectDial := func(t *testing.T, dial <-chan struct{}) {
		t.Helper()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-dial:
				return true
			default:
				return false
			}
		}, "expected dial signal")
	}

	t.Run("fires dial immediately on entry", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn := setup(t)
		defer cancel()

		messages := make(chan receivedMessage, 1)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		expectDial(t, dial)
	})

	t.Run("keepalive fires dial", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn := setup(t)
		defer cancel()

		messages := make(chan receivedMessage, 1)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		// drain initial dial
		expectDial(t, dial)

		keepalive <- struct{}{}
		expectDial(t, dial)
	})

	t.Run("multiple keepalive signals each fire dial", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn := setup(t)
		defer cancel()

		messages := make(chan receivedMessage, 1)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		// drain initial dial
		expectDial(t, dial)

		for i := 0; i < 3; i++ {
			keepalive <- struct{}{}
			expectDial(t, dial)
		}
	})
}

func TestRunSessionConnect(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
		messages chan receivedMessage,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		messages = make(chan receivedMessage, 256)
		return
	}

	t.Run("w.conn set after newConn received", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}
		defer cancel()

		conn, _, _, _ := setupWorkerTestConnection(t)
		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		newConn <- conn

		honeybeetest.Eventually(t, func() bool {
			return w.conn.Load() != nil
		}, "expected w.conn to be set")
	})

	t.Run("EventConnected emitted", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		conn, _, _, _ := setupWorkerTestConnection(t)
		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		newConn <- conn

		honeybeetest.Eventually(t, func() bool {
			select {
			case event := <-events:
				return event.ID == w.id && event.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")
	})
}

func TestRunSessionDisconnect(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
		messages chan receivedMessage,
		conn *transport.Connection,
		incomingData chan honeybeetest.MockIncomingData,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		messages = make(chan receivedMessage, 256)
		conn, _, incomingData, _ = setupWorkerTestConnection(t)
		return
	}

	t.Run("EventDisconnected emitted on connection close", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		close(incomingData)

		drainEvent(t, events, EventDisconnected)
	})

	t.Run("w.conn cleared after disconnect", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		close(incomingData)
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return w.conn.Load() == nil
		}, "expected w.conn to be cleared")
	})

	t.Run("dial fires again after disconnect", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		// drain the initial dial signal before disconnecting
		<-dial

		close(incomingData)
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-dial:
				return true
			default:
				return false
			}
		}, "expected dial signal after disconnect")
	})

	t.Run("second connection cycle emits EventConnected", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		close(incomingData)
		drainEvent(t, events, EventDisconnected)

		conn2, _, _, _ := setupWorkerTestConnection(t)
		newConn <- conn2

		drainEvent(t, events, EventConnected)
	})
}

func TestRunSessionCancellation(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
		messages chan receivedMessage,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		messages = make(chan receivedMessage, 256)
		return
	}

	t.Run("ctx cancelled pre-connection exits without emitting events", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}

		done := make(chan struct{})
		go func() {
			defer close(done)
			w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		}()

		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected runSession to exit")

		honeybeetest.Never(t, func() bool {
			select {
			case <-events:
				return true
			default:
				return false
			}
		}, "expected no events emitted")
	})

	t.Run("ctx cancelled post-connection emits EventDisconnected", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}

		conn, _, _, _ := setupWorkerTestConnection(t)

		done := make(chan struct{})
		go func() {
			defer close(done)
			w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		}()

		newConn <- conn

		drainEvent(t, events, EventConnected)

		cancel()

		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected runSession to exit")
	})

	t.Run("ctx cancelled post-connection clears w.conn", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}

		conn, _, _, _ := setupWorkerTestConnection(t)

		done := make(chan struct{})
		go func() {
			defer close(done)
			w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		}()

		newConn <- conn

		drainEvent(t, events, EventConnected)

		cancel()

		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return w.conn.Load() == nil
		}, "expected w.conn to clear")
	})
}

func TestRunReader(t *testing.T) {
	t.Run("message arrives with correct data and non-zero receivedAt", func(t *testing.T) {
		conn, _, incomingData, _ := setupWorkerTestConnection(t)
		defer conn.Close()

		messages := make(chan receivedMessage, 1)
		heartbeat := make(chan struct{})
		sessionDone := make(chan struct{})
		onStop := func() {}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			heartbeat: heartbeat,
		}
		go func() {
			for range heartbeat {
			}
		}()
		go w.runReader(conn, messages, sessionDone, onStop)

		before := time.Now()
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    []byte("hello"),
		}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-messages:
				return string(msg.data) == "hello" && msg.receivedAt.After(before)
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("heartbeat receives one signal per message", func(t *testing.T) {
		conn, _, incomingData, _ := setupWorkerTestConnection(t)
		defer conn.Close()

		messages := make(chan receivedMessage, 10)
		heartbeat := make(chan struct{})
		sessionDone := make(chan struct{})
		onStop := func() {}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			heartbeat: heartbeat,
		}

		received := atomic.Int32{}
		go func() {
			for range heartbeat {
				received.Add(1)
			}
		}()
		go func() {
			for range messages {
			}
		}()
		go w.runReader(conn, messages, sessionDone, onStop)

		const count = 3
		for i := 0; i < count; i++ {
			incomingData <- honeybeetest.MockIncomingData{
				MsgType: websocket.TextMessage,
				Data:    []byte(fmt.Sprintf("msg-%d", i)),
			}
		}

		honeybeetest.Eventually(t, func() bool {
			return received.Load() == count
		}, fmt.Sprintf("expected %d messages", count))
	})

	t.Run("incoming channel close calls conn.Close and onStop", func(t *testing.T) {
		conn, _, incomingData, _ := setupWorkerTestConnection(t)

		messages := make(chan receivedMessage, 1)
		heartbeat := make(chan struct{})
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }
		ctx := context.Background()

		w := &Worker{
			ctx:       ctx,
			id:        "wss://test",
			heartbeat: heartbeat,
		}
		go func() {
			for range heartbeat {
			}
		}()
		go func() {
			for range messages {
			}
		}()
		go w.runReader(conn, messages, sessionDone, onStop)

		// induce connection closure via reader
		incomingData <- honeybeetest.MockIncomingData{Err: io.EOF}

		err := <-conn.Errors()
		assert.Equal(t, io.EOF, err)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})

	t.Run("sessionDone close calls conn.Close and onStop", func(t *testing.T) {
		conn, _, _, _ := setupWorkerTestConnection(t)

		messages := make(chan receivedMessage, 1)
		heartbeat := make(chan struct{})
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }
		ctx := context.Background()

		w := &Worker{
			ctx:       ctx,
			id:        "wss://test",
			heartbeat: heartbeat,
		}
		go w.runReader(conn, messages, sessionDone, onStop)

		close(sessionDone)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})
}

func TestRunStopMonitor(t *testing.T) {
	t.Run("keepalive signal calls conn.Close and onStop", func(t *testing.T) {
		conn, _, _, _ := setupWorkerTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		keepalive := make(chan struct{}, 1)
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }

		w := &Worker{id: "wss://test"}
		go w.runStopMonitor(ctx, conn, keepalive, sessionDone, onStop)

		keepalive <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})

	t.Run("ctx.Done calls conn.Close and onStop", func(t *testing.T) {
		conn, _, _, _ := setupWorkerTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())

		keepalive := make(chan struct{})
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }

		w := &Worker{id: "wss://test"}
		go w.runStopMonitor(ctx, conn, keepalive, sessionDone, onStop)

		cancel()

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})

	t.Run("sessionDone close calls conn.Close and onStop", func(t *testing.T) {
		conn, _, _, _ := setupWorkerTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		keepalive := make(chan struct{})
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }

		w := &Worker{id: "wss://test"}
		go w.runStopMonitor(ctx, conn, keepalive, sessionDone, onStop)

		close(sessionDone)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})
}

func TestRunForwarder(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{id: "wss://test"}
		go w.runForwarder(ctx, messages, inbox, 0)

		messages <- receivedMessage{data: []byte("hello"), receivedAt: time.Now()}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				return string(msg.Data) == "hello" && msg.ID == "wss://test"
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("oldest message dropped when queue is full", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gate := make(chan struct{})
		gatedInbox := make(chan InboxMessage)

		// gate the inbox from receiving messages until the gate is opened
		go func() {
			<-gate
			for msg := range gatedInbox {
				inbox <- msg
			}
		}()

		w := &Worker{id: "wss://test"}
		go w.runForwarder(ctx, messages, gatedInbox, 2)

		// send three messages while the gated inbox is blocked
		messages <- receivedMessage{data: []byte("first"), receivedAt: time.Now()}
		messages <- receivedMessage{data: []byte("second"), receivedAt: time.Now()}
		messages <- receivedMessage{data: []byte("third"), receivedAt: time.Now()}

		// allow time for the first message to be dropped
		time.Sleep(20 * time.Millisecond)

		// close the gate, draining messages into the inbox
		close(gate)

		// receive messages from the inbox
		var received []string
		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				received = append(received, string(msg.Data))
			default:
			}
			return len(received) == 2
		}, "expected messages")

		// first message was dropped
		assert.Equal(t, []string{"second", "third"}, received)

	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{id: "wss://test"}
		done := make(chan struct{})
		go func() {
			w.runForwarder(ctx, messages, inbox, 0)
			close(done)
		}()

		cancel()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected done signal")
	})
}

func TestRunKeepalive(t *testing.T) {
	t.Run("heartbeat resets timer, no keepalive signal fired", func(t *testing.T) {
		heartbeat := make(chan struct{})
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{
			config:    &WorkerConfig{KeepaliveTimeout: 100 * time.Millisecond},
			heartbeat: heartbeat,
		}
		go w.runKeepalive(ctx, keepalive)

		// send heartbeats faster than the timeout
		for i := 0; i < 5; i++ {
			time.Sleep(30 * time.Millisecond)
			w.heartbeat <- struct{}{}
		}

		// because the timer is being reset, keepalive signal should not be sent
		honeybeetest.Never(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, "unexpected keepalive signal")
	})

	t.Run("keepalive timeout fires signal", func(t *testing.T) {
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 20 * time.Millisecond}}
		go w.runKeepalive(ctx, keepalive)

		// send no heartbeats, wait for timeout and keepalive signal
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, "expected keepalive signal")
	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 20 * time.Second}}
		done := make(chan struct{})
		go func() {
			w.runKeepalive(ctx, keepalive)
			close(done)
		}()

		cancel()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected done signal")
	})
}

func TestRunDialer(t *testing.T) {
	t.Run("successful dial delivers connection to newConn", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockSocket := honeybeetest.NewMockSocket()
		wctx := WorkerContext{
			Errors: make(chan error, 1),
			Dialer: &honeybeetest.MockDialer{
				DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
					return mockSocket, nil, nil
				},
			},
		}

		go w.runDialer(ctx, wctx, dial, newConn)
		dial <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-newConn:
				return true
			default:
				return false
			}
		}, "expected new connection")
	})

	t.Run("concurrent dial signals are drained; only one connection produced.",
		func(t *testing.T) {
			w := &Worker{id: "wss://test"}
			dial := make(chan struct{}, 1)
			newConn := make(chan *transport.Connection, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gate := make(chan struct{})
			dialCount := atomic.Int32{}

			mockSocket := honeybeetest.NewMockSocket()
			connConfig := &transport.ConnectionConfig{Retry: nil} // disable retry
			wctx := WorkerContext{
				Errors: make(chan error, 1),
				Dialer: &honeybeetest.MockDialer{
					DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
						dialCount.Add(1)
						<-gate
						return mockSocket, nil, nil
					},
				},
				ConnectionConfig: connConfig,
			}

			go w.runDialer(ctx, wctx, dial, newConn)
			dial <- struct{}{}

			// wait for dial to start blocking on gate
			time.Sleep(20 * time.Millisecond)

			// flood dial while dialer is blocked
			for i := 0; i < 5; i++ {
				select {
				case dial <- struct{}{}:
				default:
				}
			}

			close(gate)

			// connection is cleared to connect
			honeybeetest.Eventually(t, func() bool {
				select {
				case <-newConn:
					return true
				default:
					return false
				}
			}, "expected new connection")

			// connection was only dialed once
			assert.Equal(t, int32(1), dialCount.Load())

			// dial channel still writable
			select {
			case dial <- struct{}{}:
			default:
				t.Fatal("dial channel should still accept sends")
			}
		})

	t.Run("dial failure emits error, succeeds on next signal", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		errors := make(chan error, 1)
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// use atomic counter to fail first dial and pass second
		dialCount := atomic.Int32{}
		mockSocket := honeybeetest.NewMockSocket()
		connConfig := &transport.ConnectionConfig{Retry: nil} // disable retry
		wctx := WorkerContext{
			Errors: errors,
			Dialer: &honeybeetest.MockDialer{
				DialContextFunc: func(
					context.Context, string, http.Header,
				) (types.Socket, *http.Response, error) {
					if dialCount.Add(1) == 1 {
						// fail first
						return nil, nil, fmt.Errorf("dial failed")
					}
					// pass second
					return mockSocket, nil, nil
				},
			},
			ConnectionConfig: connConfig,
		}

		go w.runDialer(ctx, wctx, dial, newConn)
		dial <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-errors:
				return err != nil
			default:
				return false
			}
		}, "expected error")

		dial <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-newConn:
				return true
			default:
				return false
			}
		}, "expected new connection")
	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())

		wctx := WorkerContext{Errors: make(chan error, 1)}

		done := make(chan struct{})
		go func() {
			w.runDialer(ctx, wctx, dial, newConn)
			close(done)
		}()

		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected done signal")
	})

	t.Run("context cancelled during in-progress dial exits without delivering connection", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())

		wctx := WorkerContext{
			Errors:           make(chan error, 1),
			ConnectionConfig: &transport.ConnectionConfig{Retry: nil},
			Dialer: &honeybeetest.MockDialer{
				DialContextFunc: func(ctx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
					// block until context is cancelled
					select {
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					}
				},
			},
		}

		done := make(chan struct{})
		go func() {
			w.runDialer(ctx, wctx, dial, newConn)
			close(done)
		}()

		dial <- struct{}{}

		// wait for dialer to block
		time.Sleep(20 * time.Millisecond)
		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected done signal")

		// no connection was sent
		assert.Empty(t, newConn)
	})
}

func TestWorkerSend(t *testing.T) {
	t.Run("data sent to mock socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupWorkerTestConnection(t)
		defer conn.Close()

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})
		heartbeatCount := atomic.Int32{}

		w := &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			heartbeat: heartbeat,
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

		// one heartbeat was sent
		assert.Equal(t, 1, int(heartbeatCount.Load()))

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
		conn, _, _, _ := setupWorkerTestConnection(t)
		defer conn.Close()

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})
		heartbeatCount := atomic.Int32{}

		w := &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			heartbeat: heartbeat,
		}
		w.conn.Store(conn)
		defer w.cancel()

		go func() {
			for range heartbeat {
				heartbeatCount.Add(1)
			}
		}()

		const count = 3
		for i := 0; i < count; i++ {
			err := w.Send([]byte(fmt.Sprintf("msg-%d", i)))
			assert.NoError(t, err)
		}

		assert.Equal(t, count, int(heartbeatCount.Load()))
	})

	t.Run("returns error if connection is unavailable", func(t *testing.T) {
		// no connection available to worker

		ctx, cancel := context.WithCancel(context.Background())

		heartbeat := make(chan struct{})

		w := &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			heartbeat: heartbeat,
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
