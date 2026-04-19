package transport

import (
	"bytes"
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Connection state tests

func TestConnectionStateString(t *testing.T) {
	cases := []struct {
		state ConnectionState
		want  string
	}{
		{StateDisconnected, "disconnected"},
		{StateConnecting, "connecting"},
		{StateConnected, "connected"},
		{StateClosed, "closed"},
		{ConnectionState(99), "unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.state.String())
		})
	}
}

func TestConnectionState(t *testing.T) {
	// Test initial state
	conn, _ := NewConnection("ws://test", nil, nil)
	assert.Equal(t, StateDisconnected, conn.State())

	// Test state after FromSocket (should be Connected)
	conn2, _ := NewConnectionFromSocket(honeybeetest.NewMockSocket(), nil, nil)
	assert.Equal(t, StateConnected, conn2.State())

	// Test state after close
	conn.Close()
	assert.Equal(t, StateClosed, conn.State())
}

// Connection constructor tests

func TestNewConnection(t *testing.T) {
	cases := []struct {
		name        string
		url         string
		config      *ConnectionConfig
		wantErr     bool
		wantErrText string
	}{
		{
			name:   "valid url, nil config",
			url:    "ws://example.com",
			config: nil,
		},
		{
			name:   "valid url, valid config",
			url:    "wss://relay.example.com:8080/path",
			config: &ConnectionConfig{WriteTimeout: 30 * time.Second},
		},
		{
			name:        "invalid url",
			url:         "http://example.com",
			config:      nil,
			wantErr:     true,
			wantErrText: "URL must use ws:// or wss:// scheme",
		},
		{
			name: "invalid config",
			url:  "ws://example.com",
			config: &ConnectionConfig{
				Retry: &RetryConfig{
					InitialDelay: 10 * time.Second,
					MaxDelay:     1 * time.Second,
				},
			},
			wantErr:     true,
			wantErrText: "initial delay may not exceed maximum delay",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := NewConnection(tc.url, tc.config, nil)

			if tc.wantErr {
				assert.Error(t, err)
				if tc.wantErrText != "" {
					assert.ErrorContains(t, err, tc.wantErrText)
				}
				assert.Nil(t, conn)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, conn)

			// Verify struct fields
			assert.NotNil(t, conn.url)
			assert.NotNil(t, conn.dialer)
			assert.Nil(t, conn.socket)
			assert.NotNil(t, conn.config)
			assert.NotNil(t, conn.incoming)
			assert.NotNil(t, conn.errors)
			assert.NotNil(t, conn.done)
			assert.Equal(t, StateDisconnected, conn.state)
			assert.False(t, conn.closed)

			// Verify default config is used if nil is passed
			if tc.config == nil {
				assert.Equal(t, GetDefaultConnectionConfig(), conn.config)
			} else {
				assert.Equal(t, tc.config, conn.config)
			}
		})
	}
}

func TestNewConnectionFromSocket(t *testing.T) {
	cases := []struct {
		name        string
		socket      types.Socket
		config      *ConnectionConfig
		wantErr     bool
		wantErrText string
	}{
		{
			name:        "nil socket",
			socket:      nil,
			config:      nil,
			wantErr:     true,
			wantErrText: "socket cannot be nil",
		},
		{
			name:   "valid socket with nil config",
			socket: honeybeetest.NewMockSocket(),
			config: nil,
		},
		{
			name:   "valid socket with valid config",
			socket: honeybeetest.NewMockSocket(),
			config: &ConnectionConfig{WriteTimeout: 30 * time.Second},
		},
		{
			name:   "invalid config",
			socket: honeybeetest.NewMockSocket(),
			config: &ConnectionConfig{
				Retry: &RetryConfig{
					InitialDelay: 10 * time.Second,
					MaxDelay:     1 * time.Second,
				},
			},
			wantErr:     true,
			wantErrText: "initial delay may not exceed maximum delay",
		},
		{
			name:   "close handler set when provided",
			socket: honeybeetest.NewMockSocket(),
			config: &ConnectionConfig{
				CloseHandler: func(code int, text string) error {
					return nil
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// track if SetCloseHandler was called
			closeHandlerSet := false
			if tc.socket != nil {
				mockSocket := tc.socket.(*honeybeetest.MockSocket)
				originalSetCloseHandler := mockSocket.SetCloseHandlerFunc

				// wrapper around the original handler function
				mockSocket.SetCloseHandlerFunc = func(h func(int, string) error) {
					closeHandlerSet = true
					if originalSetCloseHandler != nil {
						originalSetCloseHandler(h)
					}
				}
			}

			conn, err := NewConnectionFromSocket(tc.socket, tc.config, nil)

			if tc.wantErr {
				assert.Error(t, err)
				if tc.wantErrText != "" {
					assert.ErrorContains(t, err, tc.wantErrText)
				}
				assert.Nil(t, conn)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, conn)

			// Verify fields initialized correctly
			assert.Nil(t, conn.url)
			assert.Nil(t, conn.dialer)
			assert.Equal(t, tc.socket, conn.socket)
			assert.NotNil(t, conn.config)
			assert.NotNil(t, conn.incoming)
			assert.NotNil(t, conn.errors)
			assert.NotNil(t, conn.done)
			assert.Equal(t, StateConnected, conn.state)
			assert.False(t, conn.closed)

			// Verify default config is used if nil is passed
			if tc.config == nil {
				assert.Equal(t, GetDefaultConnectionConfig(), conn.config)
			} else {
				assert.Equal(t, tc.config, conn.config)
			}

			// Verify close handler was set if provided
			if tc.config != nil && tc.config.CloseHandler != nil {
				assert.True(t, closeHandlerSet, "CloseHandler should be set on socket")
			}
		})
	}
}

func TestConnect(t *testing.T) {
	t.Run("connect fails when socket already present", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		conn.socket = honeybeetest.NewMockSocket()

		err = conn.Connect(context.Background())
		assert.Error(t, err)
		assert.ErrorContains(t, err, "already has socket")
		assert.Equal(t, StateDisconnected, conn.State())
	})

	t.Run("connect fails when connection closed", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		conn.Close()

		err = conn.Connect(context.Background())
		assert.Error(t, err)
		assert.ErrorContains(t, err, "connection is closed")
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("connect succeeds and starts goroutines", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		outgoingData := make(chan honeybeetest.MockOutgoingData, 10)

		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			outgoingData <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}
			return nil
		}

		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, StateConnected, conn.State())

		testData := []byte("test")
		conn.Send(testData)

		assert.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return bytes.Equal(msg.Data, testData)
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		conn.Close()
	})

	t.Run("connect retries on dial failure", func(t *testing.T) {
		config := &ConnectionConfig{
			Retry: &RetryConfig{
				MaxRetries:   2,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			},
		}
		conn, err := NewConnection("ws://test", config, nil)
		assert.NoError(t, err)

		attemptCount := 0
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				attemptCount++
				if attemptCount < 3 {
					return nil, nil, fmt.Errorf("dial failed")
				}
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 3, attemptCount)
		assert.Equal(t, StateConnected, conn.State())

		conn.Close()
	})

	t.Run("connect fails after max retries", func(t *testing.T) {
		config := &ConnectionConfig{
			Retry: &RetryConfig{
				MaxRetries:   2,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			},
		}
		conn, err := NewConnection("ws://test", config, nil)
		assert.NoError(t, err)

		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect(context.Background())
		assert.Error(t, err)
		assert.ErrorContains(t, err, "dial failed")
		assert.Equal(t, StateDisconnected, conn.State())
	})

	t.Run("state transitions during connect", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, StateDisconnected, conn.State())

		stateDuringDial := StateDisconnected
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				stateDuringDial = conn.state
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		conn.dialer = mockDialer

		conn.Connect(context.Background())

		assert.Equal(t, StateConnecting, stateDuringDial)
		assert.Equal(t, StateConnected, conn.State())

		conn.Close()
	})

	t.Run("close handler configured when provided", func(t *testing.T) {
		handlerSet := false
		config := &ConnectionConfig{
			CloseHandler: func(code int, text string) error {
				return nil
			},
		}
		conn, err := NewConnection("ws://test", config, nil)
		assert.NoError(t, err)

		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.SetCloseHandlerFunc = func(h func(int, string) error) {
			handlerSet = true
		}

		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		conn.dialer = mockDialer

		conn.Connect(context.Background())

		assert.True(t, handlerSet, "close handler should be set on socket")

		conn.Close()
	})
}

func TestConnectContextCancellation(t *testing.T) {
	t.Run("context cancelled during connect returns before retries exhaust", func(t *testing.T) {
		config := &ConnectionConfig{
			Retry: &RetryConfig{
				MaxRetries:   100,
				InitialDelay: 500 * time.Millisecond,
				MaxDelay:     1 * time.Second,
				JitterFactor: 0.0,
			},
		}
		conn, err := NewConnection("ws://test", config, nil)
		assert.NoError(t, err)

		dialCount := atomic.Int32{}
		ctx, cancel := context.WithCancel(context.Background())

		conn.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(ctx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
				dialCount.Add(1)
				return nil, nil, fmt.Errorf("dial failed")
			},
		}

		done := make(chan error, 1)
		go func() {
			done <- conn.Connect(ctx)
		}()

		// wait for first dial
		assert.Eventually(t, func() bool {
			return dialCount.Load() >= 1
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
		cancel()

		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)

			// number of dials is fewer than max retry count
			assert.Less(t, dialCount.Load(), int32(100))
		case <-time.After(honeybeetest.TestTimeout):
			t.Fatal("Connect did not return after context cancellation")
		}
	})
}

// Connection method tests

func TestConnectionIncoming(t *testing.T) {
	conn, err := NewConnection("ws://test", nil, nil)
	assert.NoError(t, err)

	incoming := conn.Incoming()
	assert.NotNil(t, incoming)

	// send data through the channel to verify they are the same
	testData := []byte("test")
	conn.incoming <- testData
	received := <-incoming
	assert.Equal(t, testData, received)
}

func TestConnectionErrors(t *testing.T) {
	conn, err := NewConnection("ws://test", nil, nil)
	assert.NoError(t, err)

	errors := conn.Errors()
	assert.NotNil(t, errors)

	// send data through the channel to verify they are the same
	testErr := fmt.Errorf("test error")
	conn.errors <- testErr
	received := <-errors
	assert.Equal(t, testErr, received)
}

// Test helpers

func setupTestConnection(t *testing.T, config *ConnectionConfig) (
	conn *Connection,
	mockSocket *honeybeetest.MockSocket,
	incomingData chan honeybeetest.MockIncomingData,
	outgoingData chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	incomingData = make(chan honeybeetest.MockIncomingData, 10)
	outgoingData = make(chan honeybeetest.MockOutgoingData, 10)

	mockSocket = honeybeetest.NewMockSocket()

	mockSocket.CloseFunc = func() error {
		mockSocket.Once.Do(func() {
			close(mockSocket.Closed)
		})
		return nil
	}

	// Wire ReadMessage to pull from incomingData channel
	mockSocket.ReadMessageFunc = func() (int, []byte, error) {
		select {
		case data := <-incomingData:
			return data.MsgType, data.Data, data.Err
		case <-mockSocket.Closed:
			return 0, nil, io.EOF
		}
	}

	// Wire WriteMessage to push to outgoingData channel
	mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
		select {
		case outgoingData <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}:
			return nil
		case <-mockSocket.Closed:
			return io.EOF
		default:
			return fmt.Errorf("mock outgoing chanel unavailable")
		}
	}

	var err error
	conn, err = NewConnectionFromSocket(mockSocket, config, nil)
	assert.NoError(t, err)

	return conn, mockSocket, incomingData, outgoingData
}
