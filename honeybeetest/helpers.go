package honeybeetest

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"log/slog"
	"strings"
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

type ExpectedLog struct {
	Level slog.Level
	Msg   string
	Attrs map[string]any
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

// Logging Helpers

func AssertLogSequence(t *testing.T, records []slog.Record, expected []ExpectedLog) {
	t.Helper()

	recIndex := 0
	for expIndex, exp := range expected {
		found := false

		for recIndex < len(records) {
			rec := records[recIndex]

			if rec.Level == exp.Level && strings.Contains(rec.Message, exp.Msg) {
				allAttrsMatch := true
				for key, expectedValue := range exp.Attrs {
					if !AssertAttributePresent(t, rec, key, expectedValue) {
						allAttrsMatch = false
						break
					}
				}

				if allAttrsMatch {
					found = true
					recIndex++
					break
				}
			}

			recIndex++
		}

		if !found {
			t.Fatalf(
				"expected log not found: index=%d level=%v msg=%q attrs=%v",
				expIndex, exp.Level, exp.Msg, exp.Attrs,
			)
		}
	}
}

func FindLogRecord(records []slog.Record, level slog.Level, msgSnippet string) *slog.Record {
	for i := range records {
		if records[i].Level == level && strings.Contains(records[i].Message, msgSnippet) {
			return &records[i]
		}
	}
	return nil
}

func AssertAttributePresent(t *testing.T, record slog.Record, key string, expectedValue any) bool {
	t.Helper()

	var found bool
	var actualValue any

	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key {
			found = true
			actualValue = attr.Value.Any()
			return false
		}
		return true
	})

	if !found {
		t.Fatalf("attribute %q not found in log record", key)
		return false
	}

	if !logValuesEqual(actualValue, expectedValue) {
		t.Errorf("attribute %q: expected=%v actual=%v", key, expectedValue, actualValue)
		return false
	}

	return true
}

func logValuesEqual(a, b any) bool {
	if a == b {
		return true
	}
	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)
	if aOk && bOk {
		return aInt == bInt
	}
	return false
}

func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case int16:
		return int64(val), true
	case int8:
		return int64(val), true
	default:
		return 0, false
	}
}
