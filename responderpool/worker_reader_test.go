package responderpool

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunReader(t *testing.T) {
	t.Run("message forwarded with correct data and non-zero receivedAt", func(t *testing.T) {
		conn, _, incoming, _ := setupReaderTestConnection(t)
		defer conn.Close()

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunReader(ctx, func(PoolEventKind) {}, conn, messages, heartbeat)

		before := time.Now()
		incoming <- honeybeetest.MockIncomingData{MsgType: websocket.TextMessage, Data: []byte("hello")}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-messages:
				return string(msg.data) == "hello" && msg.receivedAt.After(before)
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("heartbeat sent per forwarded message", func(t *testing.T) {
		conn, _, incoming, _ := setupReaderTestConnection(t)
		defer conn.Close()

		messages := make(chan ReceivedMessage, 10)
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
		go RunReader(ctx, func(PoolEventKind) {}, conn, messages, heartbeat)

		const n = 3
		for i := 0; i < n; i++ {
			incoming <- honeybeetest.MockIncomingData{MsgType: websocket.TextMessage, Data: []byte("msg")}
		}

		honeybeetest.Eventually(t, func() bool {
			return count.Load() == n
		}, "expected heartbeats")
	})

	t.Run("clean close calls onPeerClose with EventPeerDisconnected", func(t *testing.T) {
		conn, mock, _, _ := setupReaderTestConnection(t)
		mock.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{Code: websocket.CloseNormalClosure}
		}

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotKind PoolEventKind
		done := make(chan struct{})
		go RunReader(ctx, func(kind PoolEventKind) {
			gotKind = kind
			close(done)
		}, conn, messages, heartbeat)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onPeerClose")

		assert.Equal(t, EventPeerDisconnected, gotKind)
	})

	t.Run("unexpected close calls onPeerClose with EventPeerDropped", func(t *testing.T) {
		conn, mock, _, _ := setupReaderTestConnection(t)
		mock.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{Code: websocket.CloseProtocolError}
		}

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotKind PoolEventKind
		done := make(chan struct{})
		go RunReader(ctx, func(kind PoolEventKind) {
			gotKind = kind
			close(done)
		}, conn, messages, heartbeat)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onPeerClose")

		assert.Equal(t, EventPeerDropped, gotKind)
	})

	t.Run("read error calls onPeerClose with EventPeerDropped", func(t *testing.T) {
		conn, mock, _, _ := setupReaderTestConnection(t)
		mock.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotKind PoolEventKind
		done := make(chan struct{})
		go RunReader(ctx, func(kind PoolEventKind) {
			gotKind = kind
			close(done)
		}, conn, messages, heartbeat)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onPeerClose")

		assert.Equal(t, EventPeerDropped, gotKind)
	})

	t.Run("ctx.Done exits without calling onPeerClose", func(t *testing.T) {
		conn, _, _, _ := setupReaderTestConnection(t)
		defer conn.Close()

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			RunReader(ctx, func(PoolEventKind) {
				called.Store(true)
			}, conn, messages, heartbeat)
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
