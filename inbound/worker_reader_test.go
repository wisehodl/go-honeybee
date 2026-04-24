package inbound

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunReader(t *testing.T) {
	t.Run("message forwarded with correct data and non-zero receivedAt", func(t *testing.T) {
		conn, _, incoming, _ := setupTestConnection(t)
		defer conn.Close()

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunReader(ctx, func(WorkerExitKind) {}, conn, messages, heartbeat, nil)

		before := time.Now()
		incoming <- honeybeetest.MockIncomingData{MsgType: websocket.TextMessage, Data: []byte("hello")}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-messages:
				return string(msg.Data) == "hello" && msg.ReceivedAt.After(before)
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("heartbeat sent per forwarded message", func(t *testing.T) {
		conn, _, incoming, _ := setupTestConnection(t)
		defer conn.Close()

		messages := make(chan types.ReceivedMessage, 10)
		heartbeat := make(chan struct{}, 10)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		count := atomic.Int32{}
		go func() {
			for range heartbeat {
				count.Add(1)
			}
		}()
		go func() {
			for range messages {
			}
		}()
		go RunReader(ctx, func(WorkerExitKind) {}, conn, messages, heartbeat, nil)

		const n = 3
		for i := 0; i < n; i++ {
			incoming <- honeybeetest.MockIncomingData{MsgType: websocket.TextMessage, Data: []byte("msg")}
		}

		honeybeetest.Eventually(t, func() bool {
			return count.Load() == n
		}, "expected heartbeats")
	})

	t.Run("clean close calls onPeerClose with ExitCleanDisconnect", func(t *testing.T) {
		mock := honeybeetest.NewMockSocket()
		mock.CloseFunc = func() error {
			mock.Once.Do(func() { close(mock.Closed) })
			return nil
		}
		mock.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{Code: websocket.CloseNormalClosure}
		}

		conn, err := transport.NewConnectionFromSocket(mock, nil, nil)
		assert.NoError(t, err)

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotKind WorkerExitKind
		done := make(chan struct{})
		go RunReader(ctx, func(kind WorkerExitKind) {
			gotKind = kind
			close(done)
		}, conn, messages, heartbeat, nil)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onPeerClose")

		assert.Equal(t, ExitDisconnected, gotKind)
	})

	t.Run("unexpected close calls onPeerClose with ExitUnexpectedDrop", func(t *testing.T) {
		mock := honeybeetest.NewMockSocket()
		mock.CloseFunc = func() error {
			mock.Once.Do(func() { close(mock.Closed) })
			return nil
		}
		mock.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{Code: websocket.CloseProtocolError}
		}

		conn, err := transport.NewConnectionFromSocket(mock, nil, nil)
		assert.NoError(t, err)

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotKind WorkerExitKind
		done := make(chan struct{})
		go RunReader(ctx, func(kind WorkerExitKind) {
			gotKind = kind
			close(done)
		}, conn, messages, heartbeat, nil)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onPeerClose")

		assert.Equal(t, ExitError, gotKind)
	})

	t.Run("read error calls onPeerClose with ExitUnexpectedDrop", func(t *testing.T) {
		mock := honeybeetest.NewMockSocket()
		mock.CloseFunc = func() error {
			mock.Once.Do(func() { close(mock.Closed) })
			return nil
		}
		mock.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		conn, err := transport.NewConnectionFromSocket(mock, nil, nil)
		assert.NoError(t, err)

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotKind WorkerExitKind
		done := make(chan struct{})
		go RunReader(ctx, func(kind WorkerExitKind) {
			gotKind = kind
			close(done)
		}, conn, messages, heartbeat, nil)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onPeerClose")

		assert.Equal(t, ExitError, gotKind)
	})

	t.Run("ctx.Done exits without calling onPeerClose", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)
		defer conn.Close()

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			RunReader(ctx, func(WorkerExitKind) {
				called.Store(true)
			}, conn, messages, heartbeat, nil)
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
		}, "expected RunReader to exit")

		assert.False(t, called.Load())
	})
}
