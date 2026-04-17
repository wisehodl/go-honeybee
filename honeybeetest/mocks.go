package honeybeetest

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Dialer Mocks

type MockDialer struct {
	DialFunc func(string, http.Header) (types.Socket, *http.Response, error)
}

func (m *MockDialer) Dial(url string, h http.Header) (types.Socket, *http.Response, error) {
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
	Closed               chan struct{}
	Once                 sync.Once
	Mu                   sync.Mutex
}

func NewMockSocket() *MockSocket {
	return &MockSocket{
		WriteMessageFunc: func(int, []byte) error { return nil },
		ReadMessageFunc:  func() (int, []byte, error) { return 0, []byte("message"), nil },
		CloseFunc:        func() error { return nil },

		SetReadDeadlineFunc:  func(time.Time) error { return nil },
		SetWriteDeadlineFunc: func(time.Time) error { return nil },
		SetCloseHandlerFunc:  func(func(int, string) error) {},

		Closed: make(chan struct{}),
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

// Logging mocks

type MockSlogHandler struct {
	records []slog.Record
	mu      sync.RWMutex
}

func NewMockSlogHandler() *MockSlogHandler {
	return &MockSlogHandler{
		records: make([]slog.Record, 0),
	}
}

func (m *MockSlogHandler) Handle(ctx context.Context, record slog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
	return nil
}

func (m *MockSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (m *MockSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return m
}

func (m *MockSlogHandler) WithGroup(name string) slog.Handler {
	return m
}

func (m *MockSlogHandler) GetRecords() []slog.Record {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]slog.Record, len(m.records))
	copy(result, m.records)
	return result
}

func (m *MockSlogHandler) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = make([]slog.Record, 0)
}
