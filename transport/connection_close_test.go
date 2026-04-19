package transport

import (
	"bytes"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
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
		mockSocket := honeybeetest.NewMockSocket()
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

		assert.True(t, conn.closed)
		_, ok := <-conn.incoming
		assert.False(t, ok)
		_, ok = <-conn.errors
		assert.False(t, ok)
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
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage, Data: canary}

		assert.Eventually(t, func() bool {
			select {
			case msg := <-conn.Incoming():
				return bytes.Equal(msg, canary)
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		conn.Close()
		assert.Equal(t, StateClosed, conn.State())
	})
}
