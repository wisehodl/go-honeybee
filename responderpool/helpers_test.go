package responderpool

import (
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func setupReaderTestConnection(t *testing.T) (
	conn *transport.Connection,
	mock *honeybeetest.MockSocket,
	incoming chan honeybeetest.MockIncomingData,
	outgoing chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	incoming = make(chan honeybeetest.MockIncomingData, 10)
	outgoing = make(chan honeybeetest.MockOutgoingData, 10)
	mock = honeybeetest.NewMockSocket()

	mock.CloseFunc = func() error {
		mock.Once.Do(func() { close(mock.Closed) })
		return nil
	}

	mock.ReadMessageFunc = func() (int, []byte, error) {
		select {
		case data, ok := <-incoming:
			if !ok {
				return 0, nil, io.EOF
			}
			return data.MsgType, data.Data, data.Err
		case <-mock.Closed:
			return 0, nil, io.EOF
		}
	}

	mock.WriteMessageFunc = func(msgType int, data []byte) error {
		select {
		case outgoing <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}:
			return nil
		case <-mock.Closed:
			return io.EOF
		default:
			return fmt.Errorf("mock outgoing channel unavailable")
		}
	}

	var err error
	conn, err = transport.NewConnectionFromSocket(mock, nil, nil)
	assert.NoError(t, err)
	return
}
