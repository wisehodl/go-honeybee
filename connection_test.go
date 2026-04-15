package honeybee

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
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
	conn2, _ := NewConnectionFromSocket(NewMockSocket(), nil, nil)
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
			assert.NotNil(t, conn.outgoing)
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
		socket      Socket
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
			socket: NewMockSocket(),
			config: nil,
		},
		{
			name:   "valid socket with valid config",
			socket: NewMockSocket(),
			config: &ConnectionConfig{WriteTimeout: 30 * time.Second},
		},
		{
			name:   "invalid config",
			socket: NewMockSocket(),
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
			socket: NewMockSocket(),
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
				mockSocket := tc.socket.(*MockSocket)
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
			assert.NotNil(t, conn.outgoing)
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

		conn.socket = NewMockSocket()

		err = conn.Connect()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "already has socket")
		assert.Equal(t, StateDisconnected, conn.State())
	})

	t.Run("connect fails when connection closed", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		conn.Close()

		err = conn.Connect()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "connection is closed")
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("connect succeeds and starts goroutines", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)

		outgoingData := make(chan mockOutgoingData, 10)

		mockSocket := NewMockSocket()
		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			outgoingData <- mockOutgoingData{msgType: msgType, data: data}
			return nil
		}

		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect()
		assert.NoError(t, err)
		assert.Equal(t, StateConnected, conn.State())

		testData := []byte("test")
		conn.Send(testData)

		assert.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return bytes.Equal(msg.data, testData)
			default:
				return false
			}
		}, testTimeout, testTick)

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
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				attemptCount++
				if attemptCount < 3 {
					return nil, nil, fmt.Errorf("dial failed")
				}
				return NewMockSocket(), nil, nil
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect()
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

		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}
		conn.dialer = mockDialer

		err = conn.Connect()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "dial failed")
		assert.Equal(t, StateDisconnected, conn.State())
	})

	t.Run("state transitions during connect", func(t *testing.T) {
		conn, err := NewConnection("ws://test", nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, StateDisconnected, conn.State())

		stateDuringDial := StateDisconnected
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				stateDuringDial = conn.state
				return NewMockSocket(), nil, nil
			},
		}
		conn.dialer = mockDialer

		conn.Connect()

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

		mockSocket := NewMockSocket()
		mockSocket.SetCloseHandlerFunc = func(h func(int, string) error) {
			handlerSet = true
		}

		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		conn.dialer = mockDialer

		conn.Connect()

		assert.True(t, handlerSet, "close handler should be set on socket")

		conn.Close()
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

// Connect() tests
