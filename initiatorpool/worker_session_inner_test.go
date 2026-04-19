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
		sessionDone := make(chan struct{})
		onStop := func() {}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &DefaultWorker{
			Ctx:       ctx,
			Cancel:    cancel,
			Id:        "wss://test",
			Heartbeat: heartbeat,
		}
		go func() {
			for range heartbeat {
			}
		}()
		go w.RunReader(conn, messages, sessionDone, onStop)

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
		sessionDone := make(chan struct{})
		onStop := func() {}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &DefaultWorker{
			Ctx:       ctx,
			Cancel:    cancel,
			Id:        "wss://test",
			Heartbeat: heartbeat,
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
		go w.RunReader(conn, messages, sessionDone, onStop)

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
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }
		ctx := context.Background()

		w := &DefaultWorker{
			Ctx:       ctx,
			Id:        "wss://test",
			Heartbeat: heartbeat,
		}
		go func() {
			for range heartbeat {
			}
		}()
		go func() {
			for range messages {
			}
		}()
		go w.RunReader(conn, messages, sessionDone, onStop)

		// induce connection closure via reader
		incomingData <- honeybeetest.MockIncomingData{Err: io.EOF}

		err := <-conn.Errors()
		assert.ErrorIs(t, err, io.EOF)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})

	t.Run("sessionDone close calls conn.Close and onStop", func(t *testing.T) {
		conn, _, _, _ := setupWorkerTestConnection(t)

		messages := make(chan ReceivedMessage, 1)
		heartbeat := make(chan struct{})
		sessionDone := make(chan struct{})
		onStopCalled := atomic.Bool{}
		onStop := func() { onStopCalled.Store(true) }
		ctx := context.Background()

		w := &DefaultWorker{
			Ctx:       ctx,
			Id:        "wss://test",
			Heartbeat: heartbeat,
		}
		go w.RunReader(conn, messages, sessionDone, onStop)

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

		w := &DefaultWorker{Id: "wss://test"}
		go w.RunStopMonitor(ctx, conn, keepalive, sessionDone, onStop)

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

		w := &DefaultWorker{Id: "wss://test"}
		go w.RunStopMonitor(ctx, conn, keepalive, sessionDone, onStop)

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

		w := &DefaultWorker{Id: "wss://test"}
		go w.RunStopMonitor(ctx, conn, keepalive, sessionDone, onStop)

		close(sessionDone)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == transport.StateClosed
		}, "expected closed state")

		honeybeetest.Eventually(t, func() bool {
			return onStopCalled.Load()
		}, "expected onStop to be called")
	})
}
