package honeybee

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Dialer Mocks

type MockDialer struct {
	DialFunc func(string, http.Header) (Socket, *http.Response, error)
}

func (m *MockDialer) Dial(url string, h http.Header) (Socket, *http.Response, error) {
	return m.DialFunc(url, h)
}

// Socket Mocks

type MockSocket struct {
	WriteMessageFunc     func(int, []byte) error
	SetReadDeadlineFunc  func(t time.Time) error
	SetWriteDeadlineFunc func(t time.Time) error
	ReadMessageFunc      func() (int, []byte, error)
	CloseFunc            func() error
	SetCloseHandlerFunc  func(func(int, string) error)
	closed               chan struct{}
	once                 sync.Once
}

func NewMockSocket() *MockSocket {
	return &MockSocket{
		WriteMessageFunc: func(int, []byte) error { return nil },
		ReadMessageFunc:  func() (int, []byte, error) { return 0, []byte("message"), nil },
		CloseFunc:        func() error { return nil },

		SetReadDeadlineFunc:  func(time.Time) error { return nil },
		SetWriteDeadlineFunc: func(time.Time) error { return nil },
		SetCloseHandlerFunc:  func(func(int, string) error) {},

		closed: make(chan struct{}),
	}

}

func (m *MockSocket) WriteMessage(t int, d []byte) error {
	return m.WriteMessageFunc(t, d)
}

func (m *MockSocket) ReadMessage() (int, []byte, error) {
	return m.ReadMessageFunc()
}

func (m *MockSocket) Close() error {
	return m.CloseFunc()
}

func (m *MockSocket) SetReadDeadline(t time.Time) error {
	return m.SetReadDeadlineFunc(t)
}

func (m *MockSocket) SetWriteDeadline(t time.Time) error {
	return m.SetWriteDeadlineFunc(t)
}

func (m *MockSocket) SetCloseHandler(h func(code int, text string) error) {
	m.SetCloseHandlerFunc(h)
}

// Connection Mocks

type mockIncomingData struct {
	msgType int
	data    []byte
	err     error
}

type mockOutgoingData struct {
	msgType int
	data    []byte
}

func setupTestConnection(t *testing.T, config *Config) (
	conn *Connection,
	mockSocket *MockSocket,
	incomingData chan mockIncomingData,
	outgoingData chan mockOutgoingData,
) {
	t.Helper()

	incomingData = make(chan mockIncomingData, 10)
	outgoingData = make(chan mockOutgoingData, 10)

	mockSocket = NewMockSocket()

	mockSocket.CloseFunc = func() error {
		mockSocket.once.Do(func() {
			close(mockSocket.closed)
		})
		return nil
	}

	// Wire ReadMessage to pull from incomingData channel
	mockSocket.ReadMessageFunc = func() (int, []byte, error) {
		select {
		case data := <-incomingData:
			return data.msgType, data.data, data.err
		case <-mockSocket.closed:
			return 0, nil, io.EOF
		}
	}

	// Wire WriteMessage to push to outgoingData channel
	mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
		select {
		case <-mockSocket.closed:
			return io.EOF
		default:
			select {
			case outgoingData <- mockOutgoingData{msgType: msgType, data: data}:
				return nil
			case <-mockSocket.closed:
				return io.EOF
			default:
				return fmt.Errorf("mock outgoing chanel unavailable")
			}
		}
	}

	var err error
	conn, err = NewConnectionFromSocket(mockSocket, config, nil)
	assert.NoError(t, err)

	return conn, mockSocket, incomingData, outgoingData
}

// Logging mocks

type mockSlogHandler struct {
	records []slog.Record
	mu      sync.RWMutex
}

func newMockSlogHandler() *mockSlogHandler {
	return &mockSlogHandler{
		records: make([]slog.Record, 0),
	}
}

func (m *mockSlogHandler) Handle(ctx context.Context, record slog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
	return nil
}

func (m *mockSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (m *mockSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return m
}

func (m *mockSlogHandler) WithGroup(name string) slog.Handler {
	return m
}

func (m *mockSlogHandler) GetRecords() []slog.Record {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]slog.Record, len(m.records))
	copy(result, m.records)
	return result
}

func (m *mockSlogHandler) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = make([]slog.Record, 0)
}
