package honeybeetest

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

// Constants

const (
	TestTimeout         = 2 * time.Second
	TestTick            = 10 * time.Millisecond
	NegativeTestTimeout = 100 * time.Millisecond
)

// Types

type MockIncomingData struct {
	MsgType int
	Data    []byte
	Err     error
}

type MockOutgoingData struct {
	MsgType int
	Data    []byte
}

// Setup

func SetupTestSocket(t *testing.T) (
	socket *MockSocket,
	incoming chan MockIncomingData,
	outgoing chan MockOutgoingData,
) {
	t.Helper()

	incoming = make(chan MockIncomingData, 10)
	outgoing = make(chan MockOutgoingData, 10)
	socket = NewMockSocket()

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
		case outgoing <- MockOutgoingData{MsgType: msgType, Data: data}:
			return nil
		case <-socket.Closed:
			return io.EOF
		default:
			return io.EOF
		}
	}

	return
}

// Helpers

func ExpectIncoming(t *testing.T, incoming <-chan []byte, expected []byte) {
	t.Helper()
	assert.Eventually(t, func() bool {
		select {
		case received := <-incoming:
			return bytes.Equal(received, expected)
		default:
			return false
		}
	}, TestTimeout, TestTick)
}

func ExpectWrite(t *testing.T, outgoingData chan MockOutgoingData, msgType int, expected []byte) {
	t.Helper()

	var call MockOutgoingData
	found := assert.Eventually(t, func() bool {
		select {
		case received := <-outgoingData:
			call = received
			return true
		default:
			return false
		}
	}, TestTimeout, TestTick)

	if found {

		assert.Equal(t, msgType, call.MsgType)
		assert.Equal(t, expected, call.Data)
	}
}

func Eventually(t *testing.T, condition func() bool, msg string) {
	t.Helper()
	assert.Eventually(t, condition, TestTimeout, TestTick, msg)
}

func Never(t *testing.T, condition func() bool, msg string) {
	t.Helper()
	assert.Never(t, condition, NegativeTestTimeout, TestTick, msg)
}
