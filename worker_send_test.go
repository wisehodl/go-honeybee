package honeybee

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

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
			heartbeat:     heartbeat,
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
			heartbeat:     heartbeat,
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
		for i := 0; i < count; i++ {
			err := w.Send([]byte(fmt.Sprintf("msg-%d", i)))
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
