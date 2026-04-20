package inbound

import (
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func setupTestSocket(t *testing.T) (
	socket *honeybeetest.MockSocket,
	incoming chan honeybeetest.MockIncomingData,
	outgoing chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	incoming = make(chan honeybeetest.MockIncomingData, 10)
	outgoing = make(chan honeybeetest.MockOutgoingData, 10)
	socket = honeybeetest.NewMockSocket()

	socket.CloseFunc = func() error {
		socket.Once.Do(func() { close(socket.Closed) })
		return nil
	}

	socket.ReadMessageFunc = func() (int, []byte, error) {
		select {
		case data, ok := <-incoming:
			if !ok {
				return 0, nil, io.EOF
			}
			return data.MsgType, data.Data, data.Err
		case <-socket.Closed:
			return 0, nil, io.EOF
		}
	}

	socket.WriteMessageFunc = func(msgType int, data []byte) error {
		select {
		case outgoing <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}:
			return nil
		case <-socket.Closed:
			return io.EOF
		default:
			return io.EOF
		}
	}

	return
}

func setupTestConnection(t *testing.T) (
	conn *transport.Connection,
	socket *honeybeetest.MockSocket,
	incoming chan honeybeetest.MockIncomingData,
	outgoing chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	socket, incoming, outgoing = setupTestSocket(t)

	var err error
	conn, err = transport.NewConnectionFromSocket(socket, nil, nil)
	assert.NoError(t, err)
	return
}
