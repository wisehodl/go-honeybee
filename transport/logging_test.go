package transport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

// Helpers

func log(level slog.Level, msg string, attrs map[string]any) honeybeetest.ExpectedLog {
	return honeybeetest.ExpectedLog{Level: level, Msg: msg, Attrs: attrs}
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

		expected := []honeybeetest.ExpectedLog{
			log(slog.LevelDebug, "connecting", map[string]any{}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 1}),
			log(slog.LevelDebug, "dial successful", map[string]any{"attempt": 1}),
			log(slog.LevelInfo, "connected", map[string]any{}),
		}

		honeybeetest.AssertLogSequence(t, records, expected)
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

		expected := []honeybeetest.ExpectedLog{
			log(slog.LevelDebug, "connecting", map[string]any{}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 1}),
			log(slog.LevelDebug, "dial failed, retrying", map[string]any{"attempt": 1, "error": dialErr}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 2}),
			log(slog.LevelDebug, "dial failed, retrying", map[string]any{"attempt": 2, "error": dialErr}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 3}),
			log(slog.LevelError, "dial failed, max retries reached", map[string]any{"attempt": 3, "error": dialErr}),
			log(slog.LevelError, "connection failed", map[string]any{"error": dialErr}),
		}

		honeybeetest.AssertLogSequence(t, records, expected)
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

		expected := []honeybeetest.ExpectedLog{
			log(slog.LevelDebug, "connecting", map[string]any{}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 1}),
			log(slog.LevelDebug, "dial failed, retrying", map[string]any{"attempt": 1, "error": dialErr}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 2}),
			log(slog.LevelDebug, "dial failed, retrying", map[string]any{"attempt": 2, "error": dialErr}),
			log(slog.LevelDebug, "dialing", map[string]any{"attempt": 3}),
			log(slog.LevelDebug, "dial successful", map[string]any{"attempt": 3}),
			log(slog.LevelInfo, "connected", map[string]any{}),
		}

		honeybeetest.AssertLogSequence(t, records, expected)
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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelInfo, "connection closed") != nil
		}, "expected log")

		records := mockHandler.GetRecords()

		expected := []honeybeetest.ExpectedLog{
			log(slog.LevelInfo, "shutting down", map[string]any{}),
			log(slog.LevelInfo, "connection closed", map[string]any{}),
		}

		honeybeetest.AssertLogSequence(t, records, expected)
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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "socket close failed") != nil
		}, "expected log")

		records := mockHandler.GetRecords()

		expected := []honeybeetest.ExpectedLog{
			log(slog.LevelInfo, "shutting down", map[string]any{}),
			log(slog.LevelError, "socket close failed", map[string]any{"error": closeErr}),
		}

		honeybeetest.AssertLogSequence(t, records, expected)
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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelInfo, "connection closed by peer") != nil
		}, "expected log")

		record := honeybeetest.FindLogRecord(mockHandler.GetRecords(), slog.LevelInfo, "connection closed by peer")
		assert.NotNil(t, record)
		honeybeetest.AssertAttributePresent(t, *record, "code", websocket.CloseNormalClosure)
		honeybeetest.AssertAttributePresent(t, *record, "text", "goodbye")

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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "unexpected close") != nil
		}, "expected log")

		record := honeybeetest.FindLogRecord(mockHandler.GetRecords(), slog.LevelError, "unexpected close")
		assert.NotNil(t, record)
		honeybeetest.AssertAttributePresent(t, *record, "code", websocket.CloseProtocolError)
		honeybeetest.AssertAttributePresent(t, *record, "text", "bad protocol")

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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "read error") != nil
		}, "expected log")
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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "write deadline error") != nil
		}, "expected log")

		records := mockHandler.GetRecords()

		record := honeybeetest.FindLogRecord(records, slog.LevelError, "write deadline error")
		assert.NotNil(t, record)
		honeybeetest.AssertAttributePresent(t, *record, "error", deadlineErr)

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

		honeybeetest.Eventually(t, func() bool {
			return honeybeetest.FindLogRecord(
				mockHandler.GetRecords(), slog.LevelError, "write error") != nil
		}, "expected log")

		records := mockHandler.GetRecords()

		record := honeybeetest.FindLogRecord(records, slog.LevelError, "write error")
		assert.NotNil(t, record)
		honeybeetest.AssertAttributePresent(t, *record, "error", writeErr)

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
