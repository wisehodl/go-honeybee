package initiatorpool

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunReader(t *testing.T) {
	t.Run("message arrives with correct data and non-zero receivedAt", func(t *testing.T) {
		conn, _, incomingData, _ := setupWorkerTestConnection(t)
		defer conn.Close()

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for range heartbeat {
			}
		}()
		go RunReader(ctx, cancel, conn, messages, heartbeat)

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

		messages := make(chan ReceivedMessage, 10)
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
		go RunReader(ctx, cancel, conn, messages, heartbeat)

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

		messages := make(chan ReceivedMessage, 1)
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
		go RunReader(ctx, cancel, conn, messages, heartbeat)

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
		conn, _, _, _ := setupWorkerTestConnection(t)

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		go RunReader(ctx, cancel, conn, messages, heartbeat)

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

func TestRunStopMonitor(t *testing.T) {
	t.Run("keepalive signal calls conn.Close and cancel", func(t *testing.T) {
		conn, _, _, _ := setupWorkerTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		keepalive := make(chan struct{}, 1)

		go RunStopMonitor(ctx, cancel, conn, keepalive)

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
		conn, _, _, _ := setupWorkerTestConnection(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		keepalive := make(chan struct{})

		go RunStopMonitor(ctx, cancel, conn, keepalive)

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
