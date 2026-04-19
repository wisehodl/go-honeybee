package initiatorpool

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
