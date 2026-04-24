package outbound

import (
	"context"
	"fmt"
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
	t.Run("message arrives with correct data and non-zero receivedAt", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t)
		defer conn.Close()

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for range heartbeat {
			}
		}()
		go RunReader(ctx, cancel, conn, messages, heartbeat, nil)

		before := time.Now()
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    []byte("hello"),
		}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-messages:
				return string(msg.Data) == "hello" && msg.ReceivedAt.After(before)
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("heartbeat receives one signal per message", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t)
		defer conn.Close()

		messages := make(chan types.ReceivedMessage, 10)
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
		go RunReader(ctx, cancel, conn, messages, heartbeat, nil)

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
		conn, _, incomingData, _ := setupTestConnection(t)

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for range heartbeat {
			}
		}()
		go func() {
			for range messages {
			}
		}()
		go RunReader(ctx, cancel, conn, messages, heartbeat, nil)

		// induce connection closure via reader
		incomingData <- honeybeetest.MockIncomingData{Err: io.EOF}

		err := <-conn.Errors()
		assert.ErrorIs(t, err, io.EOF)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-ctx.Done():
				return true
			default:
				return false
			}
		}, "expected context to cancel")
	})

	t.Run("sessionDone close calls conn.Close and onStop", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)

		messages := make(chan types.ReceivedMessage, 1)
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		go RunReader(ctx, cancel, conn, messages, heartbeat, nil)

		cancel()

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-ctx.Done():
				return true
			default:
				return false
			}
		}, "expected context to cancel")
	})
}

func TestHeartbeatForwarder(t *testing.T) {
	t.Run("connection level heartbeat propagates", func(t *testing.T) {
		socket, _, _ := honeybeetest.SetupTestSocket(t)
		var pongHandler func(string) error
		socket.SetPongHandlerFunc = func(h func(string) error) { pongHandler = h }

		conn, err := transport.NewConnectionFromSocket(socket, nil, nil)
		assert.NoError(t, err)

		heartbeat := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunHeartbeatForwarder(ctx, conn, heartbeat, nil)

		honeybeetest.Eventually(t, func() bool {
			return pongHandler != nil
		}, "expected Connection to register PongHandler")

		if pongHandler == nil {
			t.Fatal("pong handler was never set")
		}

		pongHandler("") // Trigger pong

		select {
		case <-heartbeat:
		case <-time.After(time.Second):
			t.Fatal("pong did not propagate to worker heartbeat")
		}
	})
}

func TestRunStopMonitor(t *testing.T) {
	t.Run("keepalive signal calls conn.Close and cancel", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		keepalive := make(chan struct{}, 1)

		go RunStopMonitor(ctx, cancel, conn, keepalive, nil)

		keepalive <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-ctx.Done():
				return true
			default:
				return false
			}
		}, "expected context to cancel")
	})

	t.Run("ctx.Done calls conn.Close and cancel", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		keepalive := make(chan struct{})

		go RunStopMonitor(ctx, cancel, conn, keepalive, nil)

		cancel()

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-ctx.Done():
				return true
			default:
				return false
			}
		}, "expected context to cancel")
	})
}
