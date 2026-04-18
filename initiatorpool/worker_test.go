package initiatorpool

import (
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Forwarder

func TestRunForwarder(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		stop := make(chan struct{})
		defer close(stop)

		w := &Worker{id: "wss://test"}
		go w.runForwarder(messages, inbox, stop, nil, 0)

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
		stop := make(chan struct{})
		defer close(stop)

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
		go w.runForwarder(messages, gatedInbox, stop, nil, 2)

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

	t.Run("exits on stop", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		stop := make(chan struct{})

		w := &Worker{id: "wss://test"}
		done := make(chan struct{})
		go func() {
			w.runForwarder(messages, inbox, stop, nil, 0)
			close(done)
		}()

		close(stop)
		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

	t.Run("exits on pool done", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		poolDone := make(chan struct{})

		w := &Worker{id: "wss://test"}
		done := make(chan struct{})
		go func() {
			w.runForwarder(messages, inbox, nil, poolDone, 0)
			close(done)
		}()

		close(poolDone)
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
		heartbeat := make(chan struct{}, 3)
		keepalive := make(chan struct{}, 1)
		stop := make(chan struct{})
		defer close(stop)

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 100 * time.Millisecond}}
		go w.runKeepalive(heartbeat, keepalive, stop, nil)

		// send heartbeats faster than the timeout
		for i := 0; i < 5; i++ {
			time.Sleep(30 * time.Millisecond)
			heartbeat <- struct{}{}
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
		heartbeat := make(chan struct{})
		keepalive := make(chan struct{}, 1)
		stop := make(chan struct{})
		defer close(stop)

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 20 * time.Millisecond}}
		go w.runKeepalive(heartbeat, keepalive, stop, nil)

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

	t.Run("exits on stop", func(t *testing.T) {
		heartbeat := make(chan struct{})
		keepalive := make(chan struct{}, 1)
		stop := make(chan struct{})

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 20 * time.Second}}
		done := make(chan struct{})
		go func() {
			w.runKeepalive(heartbeat, keepalive, stop, nil)
			close(done)
		}()

		close(stop)
		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

	t.Run("exits on stop", func(t *testing.T) {
		heartbeat := make(chan struct{})
		keepalive := make(chan struct{}, 1)
		poolDone := make(chan struct{})

		w := &Worker{config: &WorkerConfig{KeepaliveTimeout: 20 * time.Second}}
		done := make(chan struct{})
		go func() {
			w.runKeepalive(heartbeat, keepalive, nil, poolDone)
			close(done)
		}()

		close(poolDone)
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

func TestRunDialer(t *testing.T) {
	t.Run("successful dial delivers connection to newConn", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		stop := make(chan struct{})
		defer close(stop)

		mockSocket := honeybeetest.NewMockSocket()
		ctx := WorkerContext{
			Errors: make(chan error, 1),
			Dialer: &honeybeetest.MockDialer{
				DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
					return mockSocket, nil, nil
				},
			},
		}

		go w.runDialer(dial, newConn, ctx, stop, nil)
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
			stop := make(chan struct{})
			defer close(stop)

			gate := make(chan struct{})
			dialCount := atomic.Int32{}

			mockSocket := honeybeetest.NewMockSocket()
			connConfig := &transport.ConnectionConfig{Retry: nil} // disable retry
			ctx := WorkerContext{
				Errors: make(chan error, 1),
				Dialer: &honeybeetest.MockDialer{
					DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
						dialCount.Add(1)
						<-gate
						return mockSocket, nil, nil
					},
				},
				ConnectionConfig: connConfig,
			}

			go w.runDialer(dial, newConn, ctx, stop, nil)
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
		stop := make(chan struct{})
		defer close(stop)

		// use atomic counter to fail first dial and pass second
		dialCount := atomic.Int32{}
		mockSocket := honeybeetest.NewMockSocket()
		connConfig := &transport.ConnectionConfig{Retry: nil} // disable retry
		ctx := WorkerContext{
			Errors: errors,
			Dialer: &honeybeetest.MockDialer{
				DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
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

		go w.runDialer(dial, newConn, ctx, stop, nil)
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

	t.Run("exits on stop", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		stop := make(chan struct{})

		ctx := WorkerContext{Errors: make(chan error, 1)}

		done := make(chan struct{})
		go func() {
			w.runDialer(dial, newConn, ctx, stop, nil)
			close(done)
		}()

		close(stop)

		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

	t.Run("exits on pool done", func(t *testing.T) {
		w := &Worker{id: "wss://test"}
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		poolDone := make(chan struct{})

		ctx := WorkerContext{Errors: make(chan error, 1)}

		done := make(chan struct{})
		go func() {
			w.runDialer(dial, newConn, ctx, nil, poolDone)
			close(done)
		}()

		close(poolDone)

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
