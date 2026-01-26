package ws

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
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

	t.Run("read timeout disabled when zero", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping test in short mode")
		}

		config := &Config{ReadTimeout: 0}

		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		deadlineCalled := make(chan struct{}, 1)
		mockSocket.SetReadDeadlineFunc = func(t time.Time) error {
			deadlineCalled <- struct{}{}
			return nil
		}

		conn, err := NewConnectionFromSocket(mockSocket, config)
		assert.NoError(t, err)
		defer conn.Close()

		select {
		case <-deadlineCalled:
			t.Fatal("SetReadDeadline should not be called when timeout is zero")
		case <-time.After(100 * time.Millisecond):
		}

	})

	t.Run("read timeout sets deadline when positive", func(t *testing.T) {
		config := &Config{ReadTimeout: 30}

		incomingData := make(chan mockIncomingData, 10)
		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		deadlineCalled := make(chan struct{}, 1)
		mockSocket.SetReadDeadlineFunc = func(t time.Time) error {
			deadlineCalled <- struct{}{}
			return nil
		}

		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			select {
			case data := <-incomingData:
				return data.msgType, data.data, data.err
			case <-mockSocket.closed:
				return 0, nil, io.EOF
			}
		}

		conn, err := NewConnectionFromSocket(mockSocket, config)
		assert.NoError(t, err)
		defer conn.Close()

		incomingData <- mockIncomingData{msgType: websocket.TextMessage, data: []byte("test"), err: nil}

		select {
		case <-conn.Incoming():
		case <-time.After(100 * time.Millisecond):
		}

		select {
		case _, ok := <-deadlineCalled:
			assert.True(t, ok, "SetReadDeadline should be called when timeout is positive")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("SetReadDeadline was never called")
		}
	})

	t.Run("reader exits on deadline error", func(t *testing.T) {
		config := &Config{ReadTimeout: 1 * time.Millisecond}

		mockSocket := NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.once.Do(func() {
				close(mockSocket.closed)
			})
			return nil
		}

		mockSocket.SetReadDeadlineFunc = func(t time.Time) error {
			return fmt.Errorf("test error")
		}

		conn, err := NewConnectionFromSocket(mockSocket, config)
		assert.NoError(t, err)
		defer conn.Close()

		select {
		case err := <-conn.Errors():
			assert.ErrorContains(t, err, "failed to set read deadline")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for deadline error")
		}

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, StateClosed, conn.State())

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

		conn, err := NewConnectionFromSocket(mockSocket, nil)
		assert.NoError(t, err)
		defer conn.Close()

		select {
		case err := <-conn.Errors():
			assert.Equal(t, readErr, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for read error")
		}

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, StateClosed, conn.State())

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

		conn, err := NewConnectionFromSocket(mockSocket, config)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)

		select {
		case <-deadlineCalled:
			t.Fatal("SetWriteDeadline should not be called when timeout is zero")
		case <-time.After(100 * time.Millisecond):
		}
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

		conn, err := NewConnectionFromSocket(mockSocket, config)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)

		select {
		case _, ok := <-deadlineCalled:
			assert.True(t, ok, "SetWriteDeadline should be called when timeout is positive")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("SetWriteDeadline was never called")
		}
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

		conn, err := NewConnectionFromSocket(mockSocket, config)
		assert.NoError(t, err)

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)
		defer conn.Close()

		select {
		case err := <-conn.Errors():
			assert.ErrorContains(t, err, "failed to set write deadline")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for deadline error")
		}

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("writer exits on socket write error", func(t *testing.T) {
		mockSocket := NewMockSocket()

		writeErr := fmt.Errorf("write failed")
		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			return writeErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		select {
		case err := <-conn.Errors():
			assert.Equal(t, writeErr, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for write error")
		}

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, StateClosed, conn.State())
	})
}

// Helpers

func expectIncoming(t *testing.T, conn *Connection, expected []byte) {
	t.Helper()

	select {
	case received := <-conn.Incoming():
		assert.Equal(t, expected, received)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for message")
	}
}

func expectWrite(t *testing.T, outgoingData chan mockOutgoingData, msgType int, expected []byte) {
	t.Helper()

	select {
	case call := <-outgoingData:
		assert.Equal(t, msgType, call.msgType)
		assert.Equal(t, expected, call.data)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for write")
	}
}
