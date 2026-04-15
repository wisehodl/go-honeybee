package honeybee

import (
	"bytes"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
	"time"
)

func TestStartReader(t *testing.T) {
	t.Run("text messages route to incoming channel", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)
		defer conn.Close()

		testData := []byte("hello")
		incomingData <- mockIncomingData{
			msgType: websocket.TextMessage,
			data:    testData,
			err:     nil,
		}

		expectIncoming(t, conn, testData)
	})

	t.Run("binary messages route to incoming channel", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)
		defer conn.Close()

		testData := []byte{0x00, 0x01, 0x02}
		incomingData <- mockIncomingData{
			msgType: websocket.BinaryMessage,
			data:    testData,
			err:     nil,
		}

		expectIncoming(t, conn, testData)
	})

	t.Run("multiple messages processed sequentially", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)
		defer conn.Close()

		messages := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
		for _, msg := range messages {
			incomingData <- mockIncomingData{msgType: websocket.TextMessage, data: msg, err: nil}
		}

		for _, expected := range messages {
			expectIncoming(t, conn, expected)
		}
	})

	t.Run("reader exits on socket read error", func(t *testing.T) {
		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		readErr := fmt.Errorf("read failed")
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, readErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		assert.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return err == readErr
			default:
				return false
			}
		}, testTimeout, testTick)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, testTimeout, testTick)
	})
}

func TestStartWriter(t *testing.T) {
	t.Run("data from outgoing triggers write", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t, nil)
		defer conn.Close()

		testData := []byte("test message")
		err := conn.Send(testData)
		assert.NoError(t, err)

		expectWrite(t, outgoingData, websocket.TextMessage, testData)
	})

	t.Run("multiple messages processed sequentially", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t, nil)
		defer conn.Close()

		messages := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
		for _, msg := range messages {
			err := conn.Send(msg)
			assert.NoError(t, err)
		}

		for _, expected := range messages {
			expectWrite(t, outgoingData, websocket.TextMessage, expected)
		}
	})

	t.Run("write timeout disabled when zero", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping test in short mode")
		}

		config := &Config{WriteTimeout: 0}

		outgoingData := make(chan mockOutgoingData, 10)
		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		deadlineCalled := make(chan struct{}, 1)
		mockSocket.SetWriteDeadlineFunc = func(t time.Time) error {
			deadlineCalled <- struct{}{}
			return nil
		}

		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			select {
			case outgoingData <- mockOutgoingData{msgType: msgType, data: data}:
			case <-mockSocket.closed:
				return io.EOF
			}
			return nil
		}

		conn, err := NewConnectionFromSocket(mockSocket, config, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		assert.Never(t, func() bool {
			select {
			case <-deadlineCalled:
				return true
			default:
				return false
			}
		}, negativeTestTimeout, testTick,
			"SetWriteDeadline should not be called when timeout is zero")
	})

	t.Run("write timeout sets deadline when positive", func(t *testing.T) {
		config := &Config{WriteTimeout: 30 * time.Millisecond}

		outgoingData := make(chan mockOutgoingData, 10)
		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		deadlineCalled := make(chan struct{}, 1)
		mockSocket.SetWriteDeadlineFunc = func(t time.Time) error {
			deadlineCalled <- struct{}{}
			return nil
		}

		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			select {
			case outgoingData <- mockOutgoingData{msgType: msgType, data: data}:
			case <-mockSocket.closed:
				return io.EOF
			}
			return nil
		}

		conn, err := NewConnectionFromSocket(mockSocket, config, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			select {
			case <-deadlineCalled:
				return true
			default:
				return false
			}
		}, testTimeout, testTick,
			"SetWriteDeadline should be called when timeout is positive")
	})

	t.Run("writer exits on deadline error", func(t *testing.T) {
		config := &Config{WriteTimeout: 1 * time.Millisecond}

		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		mockSocket.SetWriteDeadlineFunc = func(t time.Time) error {
			return fmt.Errorf("test error")
		}

		conn, err := NewConnectionFromSocket(mockSocket, config, nil)
		assert.NoError(t, err)

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)
		defer conn.Close()

		assert.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return err != nil &&
					strings.Contains(err.Error(), "failed to set write deadline")
			default:
				return false
			}
		}, testTimeout, testTick)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, testTimeout, testTick)
	})

	t.Run("writer exits on socket write error", func(t *testing.T) {
		mockSocket := NewMockSocket()

		writeErr := fmt.Errorf("write failed")
		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			return writeErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return err == writeErr
			default:
				return false
			}
		}, testTimeout, testTick)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, testTimeout, testTick)
	})
}

// Helpers

func expectIncoming(t *testing.T, conn *Connection, expected []byte) {
	t.Helper()
	assert.Eventually(t, func() bool {
		select {
		case received := <-conn.Incoming():
			return bytes.Equal(received, expected)
		default:
			return false
		}
	}, testTimeout, testTick)
}

func expectWrite(t *testing.T, outgoingData chan mockOutgoingData, msgType int, expected []byte) {
	t.Helper()

	var call mockOutgoingData
	found := assert.Eventually(t, func() bool {
		select {
		case received := <-outgoingData:
			call = received
			return true
		default:
			return false
		}
	}, testTimeout, testTick)

	if found {

		assert.Equal(t, msgType, call.msgType)
		assert.Equal(t, expected, call.data)
	}
}
