package transport

import (
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"sync"
	"testing"
	"time"
)

func TestConnectionSend(t *testing.T) {
	t.Run("writes message to socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		testData := []byte("test message")
		err := conn.Send(testData)
		assert.NoError(t, err)

		honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, testData)
	})

	t.Run("writes multiple message to socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
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

	t.Run("concurrent sends write messages to socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		mu := sync.Mutex{}
		messages := []string{}
		done := make(chan struct{})

		go func() {
			for {
				select {
				case msg := <-outgoingData:
					mu.Lock()
					messages = append(messages, string(msg.Data))
					mu.Unlock()
				case <-done:
					return
				}
			}
		}()

		defer close(done)

		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					data := []byte(fmt.Sprintf("msg-%d-%d", id, j))
					for {
						// send and retry until success
						err := conn.Send(data)
						if err != nil {
							continue
						} else {
							break
						}
					}
				}
			}(i)
		}

		wg.Wait()

		honeybeetest.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(messages) == 50
		}, "should have received 50 messages")

	})

	t.Run("send fails when connection is closed", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)
		conn.Close()

		testData := []byte("test message")
		err := conn.Send(testData)
		assert.ErrorIs(t, err, ErrConnectionClosed)
	})

	t.Run("write timeout disabled when zero", func(t *testing.T) {
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

		honeybeetest.Never(t, func() bool {
			select {
			case <-deadlineCalled:
				return true
			default:
				return false
			}
		}, "SetWriteDeadline should not be called when timeout is zero")
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

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-deadlineCalled:
				return true
			default:
				return false
			}
		}, "SetWriteDeadline should be called when timeout is positive")
	})

	t.Run("send fails on deadline error", func(t *testing.T) {
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
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.ErrorIs(t, err, ErrFailedWriteDeadline)

		honeybeetest.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, "expected closed state")
	})

	t.Run("send fails on socket write error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()

		writeErr := fmt.Errorf("test error")
		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			return writeErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.ErrorIs(t, err, ErrWriteFailed)
		assert.ErrorContains(t, err, "test error")
	})
}
