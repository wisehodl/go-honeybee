package honeybee

import (
	"bytes"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDisconnectedConnectionClose(t *testing.T) {
	t.Run("close succeeds on disconnected connection", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, StateDisconnected, conn.State())

		conn.Close()
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("close is idempotent", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		conn.Close()
		conn.Close()
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("close with nil socket", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, conn.socket)

		conn.Close()
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("socket close error does not propagate", func(t *testing.T) {
		expectedErr := fmt.Errorf("socket close failed")
		mockSocket := NewMockSocket()
		mockSocket.CloseFunc = func() error {
			return expectedErr
		}

		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)
		conn.socket = mockSocket

		conn.Close()
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("channels close after close", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		conn.Close()

		assert.Eventually(t, func() bool {
			select {
			case _, ok := <-conn.Errors():
				return !ok
			default:
				return false
			}
		}, testTimeout, testTick, "errors channel should close")
	})

	t.Run("send fails after close", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		conn.Close()

		err = conn.Send([]byte("test"))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "connection closed")
	})

}

func TestConnectedConnectionClose(t *testing.T) {
	t.Run("blocked on ReadMessage, unblocks on closed", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)

		// Send a message to ensure reader loop is blocking
		canary := []byte("canary")
		incomingData <- mockIncomingData{msgType: websocket.TextMessage, data: canary}

		assert.Eventually(t, func() bool {
			select {
			case msg := <-conn.Incoming():
				return bytes.Equal(msg, canary)
			default:
				return false
			}
		}, testTimeout, testTick)

		conn.Close()
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("writer active during close exits cleanly", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t, nil)

		for i := 0; i < 50; i++ {
			conn.Send([]byte("message"))
		}

		conn.Close()

		err := conn.Send([]byte("late"))
		assert.Error(t, err, "Send should fail after close")
		assert.ErrorContains(t, err, "connection closed")
	})

	t.Run("both goroutines active during close", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)

		for i := 0; i < 10; i++ {
			incomingData <- mockIncomingData{
				msgType: websocket.TextMessage,
				data:    []byte(fmt.Sprintf("in-%d", i)),
			}
			conn.Send([]byte(fmt.Sprintf("out-%d", i)))
		}

		conn.Close()
	})
}
