package transport

import (
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
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
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    testData,
			Err:     nil,
		}

		honeybeetest.ExpectIncoming(t, conn.Incoming(), testData)
	})

	t.Run("binary messages route to incoming channel", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)
		defer conn.Close()

		testData := []byte{0x00, 0x01, 0x02}
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.BinaryMessage,
			Data:    testData,
			Err:     nil,
		}

		honeybeetest.ExpectIncoming(t, conn.Incoming(), testData)
	})

	t.Run("multiple messages processed sequentially", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t, nil)
		defer conn.Close()

		messages := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
		for _, msg := range messages {
			incomingData <- honeybeetest.MockIncomingData{
				MsgType: websocket.TextMessage, Data: msg, Err: nil}
		}

		for _, expected := range messages {
			honeybeetest.ExpectIncoming(t, conn.Incoming(), expected)
		}
	})

	t.Run("reader exits on socket read error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
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
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})
}

func TestStartWriter(t *testing.T) {
	t.Run("data from outgoing triggers write", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t, nil)
		defer conn.Close()

		testData := []byte("test message")
		err := conn.Send(testData)
		assert.NoError(t, err)

		honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, testData)
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
			honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, expected)
		}
	})

	t.Run("write timeout disabled when zero", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping test in short mode")
		}

		config := &ConnectionConfig{WriteTimeout: 0}

		outgoingData := make(chan honeybeetest.MockOutgoingData, 10)
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
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
			case outgoingData <- honeybeetest.MockOutgoingData{
				MsgType: msgType, Data: data}:
			case <-mockSocket.Closed:
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
		}, honeybeetest.NegativeTestTimeout, honeybeetest.TestTick,
			"SetWriteDeadline should not be called when timeout is zero")
	})

	t.Run("write timeout sets deadline when positive", func(t *testing.T) {
		config := &ConnectionConfig{WriteTimeout: 30 * time.Millisecond}

		outgoingData := make(chan honeybeetest.MockOutgoingData, 10)
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
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
			case outgoingData <- honeybeetest.MockOutgoingData{
				MsgType: msgType, Data: data}:
			case <-mockSocket.Closed:
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
		}, honeybeetest.TestTimeout, honeybeetest.TestTick,
			"SetWriteDeadline should be called when timeout is positive")
	})

	t.Run("writer exits on deadline error", func(t *testing.T) {
		config := &ConnectionConfig{WriteTimeout: 1 * time.Millisecond}

		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
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
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

	t.Run("writer exits on socket write error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()

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
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})
}

// Helpers
