package transport

import (
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
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

		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			return conn.State() == StateClosed
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})
}
