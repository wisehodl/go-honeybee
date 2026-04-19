package initiatorpool

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunForwarder(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{id: "wss://test"}
		go w.runForwarder(ctx, messages, inbox, 0)

		messages <- receivedMessage{data: []byte("hello"), receivedAt: time.Now()}

		assert.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				return string(msg.Data) == "hello" && msg.ID == "wss://test"
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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
		assert.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				received = append(received, string(msg.Data))
			default:
			}
			return len(received) == 2
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

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
		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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
		assert.Never(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, honeybeetest.NegativeTestTimeout, honeybeetest.TestTick)
	})

	t.Run("keepalive timeout fires signal", func(t *testing.T) {
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 20 * time.Millisecond}}
		go w.runKeepalive(ctx, keepalive)

		// send no heartbeats, wait for timeout and keepalive signal
		assert.Eventually(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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
		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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

		assert.Eventually(t, func() bool {
			return connClosed(conn)
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		assert.True(t, onStopCalled.Load())
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

		assert.Eventually(t, func() bool {
			return connClosed(conn)
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		assert.True(t, onStopCalled.Load())
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

		assert.Eventually(t, func() bool {
			return connClosed(conn)
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		assert.True(t, onStopCalled.Load())
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

		assert.Eventually(t, func() bool {
			select {
			case <-newConn:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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
			assert.Eventually(t, func() bool {
				select {
				case <-newConn:
					return true
				default:
					return false
				}
			}, honeybeetest.TestTimeout, honeybeetest.TestTick)

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

		assert.Eventually(t, func() bool {
			select {
			case err := <-errors:
				return err != nil
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		dial <- struct{}{}

		assert.Eventually(t, func() bool {
			select {
			case <-newConn:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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

		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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

		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

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
		assert.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return string(msg.Data) == "hello"
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
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
