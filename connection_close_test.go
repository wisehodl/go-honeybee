package honeybee

import (
	"fmt"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDisconnectedConnectionClose(t *testing.T) {
	t.Run("close succeeds on disconnected connection", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil)
		assert.NoError(t, err)
		assert.Equal(t, StateDisconnected, conn.State())

		err = conn.Close()
		assert.NoError(t, err)
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("close is idempotent", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil)
		assert.NoError(t, err)

		err = conn.Close()
		assert.NoError(t, err)

		// Second close should succeed without error
		err = conn.Close()
		assert.NoError(t, err)
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("close with nil socket", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil)
		assert.NoError(t, err)
		assert.Nil(t, conn.socket)

		err = conn.Close()
		assert.NoError(t, err)
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("socket close error propagates", func(t *testing.T) {
		expectedErr := fmt.Errorf("socket close failed")
		mockSocket := NewMockSocket()
		mockSocket.CloseFunc = func() error {
			return expectedErr
		}

		conn, err := NewConnection("ws://test", nil)
		assert.NoError(t, err)
		conn.socket = mockSocket

		err = conn.Close()
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("channels close after close", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil)
		assert.NoError(t, err)

		err = conn.Close()
		assert.NoError(t, err)

		// Verify incoming channel closed
		select {
		case _, ok := <-conn.incoming:
			assert.False(t, ok, "incoming channel should be closed")
		case <-time.After(50 * time.Millisecond):
			t.Fatal("timeout waiting for incoming channel closure")
		}

		// Verify outgoing channel closed
		select {
		case _, ok := <-conn.outgoing:
			assert.False(t, ok, "outgoing channel should be closed")
		case <-time.After(50 * time.Millisecond):
			t.Fatal("timeout waiting for outgoing channel closure")
		}

		// Verify errors channel closed
		select {
		case _, ok := <-conn.errors:
			assert.False(t, ok, "errors channel should be closed")
		case <-time.After(50 * time.Millisecond):
			t.Fatal("timeout waiting for errors channel closure")
		}
	})

	t.Run("send fails after close", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil)
		assert.NoError(t, err)

		err = conn.Close()
		assert.NoError(t, err)

		err = conn.Send([]byte("test"))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "connection closed")
	})

}

func TestConnectedConnectionClose(t *testing.T) {
	t.Run("blocked on ReadMessage, unblocks on closed", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)

		// Wait for reader to block
		time.Sleep(10 * time.Millisecond)

		err := conn.Close()
		assert.NoError(t, err)
		assert.Equal(t, StateClosed, conn.State())

		close(incomingData)
	})

	t.Run("writer active during close exits cleanly", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t, nil)

		for i := 0; i < 50; i++ {
			conn.Send([]byte("message"))
		}

		err := conn.Close()
		assert.NoError(t, err)

		err = conn.Send([]byte("late"))
		assert.Error(t, err, "Send should fail after close")
		assert.ErrorContains(t, err, "connection closed")

		close(outgoingData)
	})

	t.Run("both goroutines active during close", func(t *testing.T) {
		conn, _, incomingData, outgoingData := setupTestConnection(t, nil)

		for i := 0; i < 10; i++ {
			incomingData <- mockIncomingData{
				msgType: websocket.TextMessage,
				data:    []byte(fmt.Sprintf("in-%d", i)),
			}
			conn.Send([]byte(fmt.Sprintf("out-%d", i)))
		}

		time.Sleep(10 * time.Millisecond)

		err := conn.Close()
		assert.NoError(t, err)

		close(incomingData)
		close(outgoingData)
	})
}
