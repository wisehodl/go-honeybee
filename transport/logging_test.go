package transport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

// Helpers

type expectedLog struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

func assertLogSequence(t *testing.T, records []slog.Record, expected []expectedLog) {
	t.Helper()

	recIndex := 0
	for expIndex, exp := range expected {
		found := false

		// Search forward through records
		for recIndex < len(records) {
			rec := records[recIndex]

			if rec.Level == exp.level && strings.Contains(rec.Message, exp.msg) {
				allAttrsMatch := true
				for key, expectedValue := range exp.attrs {
					if !assertAttributePresent(t, rec, key, expectedValue) {
						allAttrsMatch = false
						break
					}
				}

				if allAttrsMatch {
					found = true
					recIndex++ // Consume this record
					break
				}
			}

			recIndex++ // Move to next record
		}

		if !found {
			t.Fatalf(
				"expected log not found: index=%d level=%v msg=%q attrs=%v",
				expIndex, exp.level, exp.msg, exp.attrs)
		}
	}
}

func findLogRecord(
	records []slog.Record, level slog.Level, msgSnippet string,
) *slog.Record {
	for i := range records {
		if records[i].Level == level && strings.Contains(records[i].Message, msgSnippet) {
			return &records[i]
		}
	}
	return nil
}

func assertAttributePresent(
	t *testing.T, record slog.Record, key string, expectedValue any,
) bool {
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
	}

	if !valuesEqual(actualValue, expectedValue) {
		t.Errorf("attribute %q mismatch: expected=%v actual=%v", key, expectedValue, actualValue)
		return false
	}

	return true
}

func valuesEqual(a, b any) bool {
	// Direct equality
	if a == b {
		return true
	}

	// Handle int/int64 conversions
	aInt, aIsInt := toInt64(a)
	bInt, bIsInt := toInt64(b)
	if aIsInt && bIsInt {
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

// Tests

func TestConnectLogging(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		conn, err := NewConnection("ws://test", nil, logger)
		assert.NoError(t, err)

		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.NoError(t, err)
		defer conn.Close()

		records := mockHandler.GetRecords()

		expected := []expectedLog{
			{slog.LevelInfo, "connecting", map[string]any{}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 1}},
			{slog.LevelInfo, "dial successful", map[string]any{"attempt": 1}},
			{slog.LevelInfo, "connected", map[string]any{}},
		}

		assertLogSequence(t, records, expected)
	})

	t.Run("max retries failure", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		config := &ConnectionConfig{
			Retry: &RetryConfig{
				MaxRetries:   2,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			},
		}

		conn, err := NewConnection("ws://test", config, logger)
		assert.NoError(t, err)

		dialErr := fmt.Errorf("dial error")
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, dialErr
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.Error(t, err)

		records := mockHandler.GetRecords()

		expected := []expectedLog{
			{slog.LevelInfo, "connecting", map[string]any{}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 1}},
			{slog.LevelWarn, "dial failed, retrying", map[string]any{"attempt": 1, "error": dialErr}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 2}},
			{slog.LevelWarn, "dial failed, retrying", map[string]any{"attempt": 2, "error": dialErr}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 3}},
			{slog.LevelError, "dial failed, max retries reached", map[string]any{"attempt": 3, "error": dialErr}},
			{slog.LevelError, "connection failed", map[string]any{"error": dialErr}},
		}

		assertLogSequence(t, records, expected)
	})

	t.Run("success after retry", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		config := &ConnectionConfig{
			Retry: &RetryConfig{
				MaxRetries:   3,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			},
		}

		conn, err := NewConnection("ws://test", config, logger)
		assert.NoError(t, err)

		attemptCount := 0
		dialErr := fmt.Errorf("dial error")
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				attemptCount++
				if attemptCount < 3 {
					return nil, nil, dialErr
				}
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.NoError(t, err)
		defer conn.Close()

		records := mockHandler.GetRecords()

		expected := []expectedLog{
			{slog.LevelInfo, "connecting", map[string]any{}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 1}},
			{slog.LevelWarn, "dial failed, retrying", map[string]any{"attempt": 1, "error": dialErr}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 2}},
			{slog.LevelWarn, "dial failed, retrying", map[string]any{"attempt": 2, "error": dialErr}},
			{slog.LevelInfo, "dialing", map[string]any{"attempt": 3}},
			{slog.LevelInfo, "dial successful", map[string]any{"attempt": 3}},
			{slog.LevelInfo, "connected", map[string]any{}},
		}

		assertLogSequence(t, records, expected)
	})
}

func TestCloseLogging(t *testing.T) {
	t.Run("normal close", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		mockSocket := honeybeetest.NewMockSocket()
		conn, err := NewConnectionFromSocket(mockSocket, nil, logger)
		assert.NoError(t, err)

		conn.Close()

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelInfo, "closed") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		records := mockHandler.GetRecords()

		expected := []expectedLog{
			{slog.LevelInfo, "closing", map[string]any{"state": "connected"}},
			{slog.LevelInfo, "closed", map[string]any{}},
		}

		assertLogSequence(t, records, expected)
	})

	t.Run("close with socket error", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		closeErr := fmt.Errorf("close error")
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.CloseFunc = func() error {
			return closeErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, logger)
		assert.NoError(t, err)

		conn.Close()

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "socket close failed") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		records := mockHandler.GetRecords()

		expected := []expectedLog{
			{slog.LevelInfo, "closing", map[string]any{"state": "connected"}},
			{slog.LevelError, "socket close failed", map[string]any{"error": closeErr}},
		}

		assertLogSequence(t, records, expected)
	})
}

func TestReaderLogging(t *testing.T) {
	t.Run("clean close by peer", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{
				Code: websocket.CloseNormalClosure,
				Text: "goodbye",
			}
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, logger)
		assert.NoError(t, err)
		defer conn.Close()

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelInfo, "connection closed by peer") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		record := findLogRecord(mockHandler.GetRecords(), slog.LevelInfo, "connection closed by peer")
		assert.NotNil(t, record)
		assertAttributePresent(t, *record, "code", websocket.CloseNormalClosure)
		assertAttributePresent(t, *record, "text", "goodbye")

	})

	t.Run("unexpected close", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{
				Code: websocket.CloseProtocolError,
				Text: "bad protocol",
			}
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, logger)
		assert.NoError(t, err)
		defer conn.Close()

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "unexpected close") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		record := findLogRecord(mockHandler.GetRecords(), slog.LevelError, "unexpected close")
		assert.NotNil(t, record)
		assertAttributePresent(t, *record, "code", websocket.CloseProtocolError)
		assertAttributePresent(t, *record, "text", "bad protocol")

	})

	t.Run("read error", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, logger)
		assert.NoError(t, err)
		defer conn.Close()

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "read error") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})
}

func TestWriterLogging(t *testing.T) {
	t.Run("write deadline error", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		config := &ConnectionConfig{WriteTimeout: 1 * time.Millisecond}

		deadlineErr := fmt.Errorf("deadline error")
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.SetWriteDeadlineFunc = func(time.Time) error {
			return deadlineErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, config, logger)
		assert.NoError(t, err)

		err = conn.Send([]byte("test"))
		assert.ErrorContains(t, err, "failed to set write deadline: deadline error")

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "write deadline error") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		records := mockHandler.GetRecords()

		record := findLogRecord(records, slog.LevelError, "write deadline error")
		assert.NotNil(t, record)
		assertAttributePresent(t, *record, "error", deadlineErr)

		conn.Close()
	})

	t.Run("write message error", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()
		logger := slog.New(mockHandler)

		writeErr := fmt.Errorf("write error")
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.WriteMessageFunc = func(int, []byte) error {
			return writeErr
		}

		conn, err := NewConnectionFromSocket(mockSocket, nil, logger)
		assert.NoError(t, err)

		err = conn.Send([]byte("test"))
		assert.ErrorContains(t, err, "write error")

		assert.Eventually(t, func() bool {
			return findLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "write error") != nil
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		records := mockHandler.GetRecords()

		record := findLogRecord(records, slog.LevelError, "write error")
		assert.NotNil(t, record)
		assertAttributePresent(t, *record, "error", writeErr)

		conn.Close()
	})
}

func TestLoggingDisabled(t *testing.T) {
	t.Run("nil logger produces no logs", func(t *testing.T) {
		mockHandler := honeybeetest.NewMockSlogHandler()

		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.NoError(t, err)

		conn.Close()

		records := mockHandler.GetRecords()
		assert.Empty(t, records)
	})
}
