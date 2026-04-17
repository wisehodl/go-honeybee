package honeybeetest

import (
	"bytes"
	"github.com/stretchr/testify/assert"
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
