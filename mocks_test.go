package honeybee

import (
	"github.com/stretchr/testify/assert"
	"io"
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
		case outgoingData <- mockOutgoingData{msgType: msgType, data: data}:
		case <-mockSocket.closed:
			return io.EOF
		}
		return nil
	}

	var err error
	conn, err = NewConnectionFromSocket(mockSocket, config)
	assert.NoError(t, err)

	return conn, mockSocket, incomingData, outgoingData
}
