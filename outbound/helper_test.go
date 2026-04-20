package outbound

import (
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func setupTestConnection(t *testing.T) (
	conn *transport.Connection,
	mockSocket *honeybeetest.MockSocket,
	incomingData chan honeybeetest.MockIncomingData,
	outgoingData chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	incomingData = make(chan honeybeetest.MockIncomingData, 10)
	outgoingData = make(chan honeybeetest.MockOutgoingData, 10)
	mockSocket = honeybeetest.NewMockSocket()

	mockSocket.CloseFunc = func() error {
		mockSocket.Once.Do(func() { close(mockSocket.Closed) })
		return nil
	}

	mockSocket.ReadMessageFunc = func() (int, []byte, error) {
		select {
		case data, ok := <-incomingData:
			if !ok {
				return 0, nil, io.EOF
			}
			return data.MsgType, data.Data, data.Err
		case <-mockSocket.Closed:
			return 0, nil, io.EOF
		}
	}

	mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
		select {
		case outgoingData <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}:
			return nil
		case <-mockSocket.Closed:
			return io.EOF
		default:
			return fmt.Errorf("mock outgoing channel unavailable")
		}
	}

	var err error
	conn, err = transport.NewConnectionFromSocket(mockSocket, nil, nil)
	assert.NoError(t, err)
	return
}
