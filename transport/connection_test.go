package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"sync"
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
	conn, _ := NewConnection(context.Background(), "ws://test", nil, nil)
	assert.Equal(t, StateDisconnected, conn.State())

	// Test state after FromSocket (should be Connected)
	conn2, _ := NewConnectionFromSocket(context.Background(), honeybeetest.NewMockSocket(), nil, nil)
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
			config: &ConnectionConfig{WriteTimeout: 30 * time.Second, Retry: RetryConfig{Disabled: true}},
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
				Retry: RetryConfig{
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
			conn, err := NewConnection(context.Background(), tc.url, tc.config, nil)

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
				expected := *GetDefaultConnectionConfig()
				expected.Dialer = conn.config.Dialer // dialer resolved at construction
				assert.Equal(t, expected, conn.config)
			} else {
				expected := *tc.config
				expected.Dialer = conn.config.Dialer
				assert.Equal(t, expected, conn.config)
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
			config: &ConnectionConfig{WriteTimeout: 30 * time.Second, Retry: RetryConfig{Disabled: true}},
		},
		{
			name:   "invalid config",
			socket: honeybeetest.NewMockSocket(),
			config: &ConnectionConfig{
				Retry: RetryConfig{
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
				Retry: RetryConfig{Disabled: true},
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

			conn, err := NewConnectionFromSocket(context.Background(), tc.socket, tc.config, nil)

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

			// Verify default config is used if nil is passed.
			// CloseHandler is a func; exclude it from the struct comparison
			// (identity is verified separately via closeHandlerSet).
			gotCfg := conn.config
			gotCfg.CloseHandler = nil
			if tc.config == nil {
				expected := *GetDefaultConnectionConfig()
				expected.CloseHandler = nil
				assert.Equal(t, expected, gotCfg)
			} else {
				expected := *tc.config
				expected.CloseHandler = nil
				assert.Equal(t, expected, gotCfg)
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
		conn, err := NewConnection(context.Background(), "ws://test", nil, nil)
		assert.NoError(t, err)

		conn.socket = honeybeetest.NewMockSocket()

		err = conn.Connect(context.Background(), nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrSocketExists)
		assert.Equal(t, StateDisconnected, conn.State())
	})

	t.Run("connect fails when connection closed", func(t *testing.T) {
		conn, err := NewConnection(context.Background(), "ws://test", nil, nil)
		assert.NoError(t, err)

		conn.Close()

		err = conn.Connect(context.Background(), nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionClosed)
		assert.Equal(t, StateClosed, conn.State())
	})

	t.Run("connect succeeds and starts goroutines", func(t *testing.T) {
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
		conn, err := NewConnection(context.Background(), "ws://test",
			&ConnectionConfig{Retry: RetryConfig{Disabled: true}, Dialer: mockDialer}, nil)
		assert.NoError(t, err)

		err = conn.Connect(context.Background(), nil)
		assert.NoError(t, err)
		assert.Equal(t, StateConnected, conn.State())

		testData := []byte("test")
		conn.Send(testData)

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return bytes.Equal(msg.Data, testData)
			default:
				return false
			}
		}, "expected message")

		conn.Close()
	})

	t.Run("connect retries on dial failure", func(t *testing.T) {
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
		config := &ConnectionConfig{
			Retry: RetryConfig{
				MaxRetries:   2,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			},
			Dialer: mockDialer,
		}
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		err = conn.Connect(context.Background(), nil)
		assert.NoError(t, err)
		assert.Equal(t, 3, attemptCount)
		assert.Equal(t, StateConnected, conn.State())

		conn.Close()
	})

	t.Run("connect fails after max retries", func(t *testing.T) {
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}
		config := &ConnectionConfig{
			Retry: RetryConfig{
				MaxRetries:   2,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
				JitterFactor: 0.0,
			},
			Dialer: mockDialer,
		}
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		err = conn.Connect(context.Background(), nil)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "dial failed")
		assert.Equal(t, StateDisconnected, conn.State())
	})

	t.Run("state transitions during connect", func(t *testing.T) {
		stateDuringDial := StateDisconnected
		// conn captured after construction; closure safe because dialer runs during Connect
		var conn *Connection
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				stateDuringDial = conn.state
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		var err error
		conn, err = NewConnection(context.Background(), "ws://test",
			&ConnectionConfig{Retry: RetryConfig{Disabled: true}, Dialer: mockDialer}, nil)
		assert.NoError(t, err)
		assert.Equal(t, StateDisconnected, conn.State())

		conn.Connect(context.Background(), nil)

		assert.Equal(t, StateConnecting, stateDuringDial)
		assert.Equal(t, StateConnected, conn.State())

		conn.Close()
	})

	t.Run("close handler configured when provided", func(t *testing.T) {
		handlerSet := false
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.SetCloseHandlerFunc = func(h func(int, string) error) {
			handlerSet = true
		}
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}
		config := &ConnectionConfig{
			CloseHandler: func(code int, text string) error {
				return nil
			},
			Retry:  RetryConfig{Disabled: true},
			Dialer: mockDialer,
		}
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		conn.Connect(context.Background(), nil)

		assert.True(t, handlerSet, "close handler should be set on socket")

		conn.Close()
	})

	t.Run("passes headers when configured", func(t *testing.T) {
		header := http.Header{"X-Custom": []string{"val"}}
		dialCalled := false
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(ctx context.Context, url string, h http.Header) (types.Socket, *http.Response, error) {
				assert.Equal(t, "val", h.Get("X-Custom"))
				dialCalled = true
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		conf, _ := NewConnectionConfig(WithRequestHeader(header), WithConnectionDialer(mockDialer))
		conn, _ := NewConnection(context.Background(), "ws://test", conf, nil)

		err := conn.Connect(context.Background(), nil)

		assert.NoError(t, err)
		assert.True(t, dialCalled)
	})

	t.Run("onDialError callback fires on each failed attempt", func(t *testing.T) {
		var mu sync.Mutex
		var capturedErrors []error

		attemptCount := 0
		dialErr := fmt.Errorf("dial failed")
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				attemptCount++
				if attemptCount < 3 {
					return nil, nil, dialErr
				}
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		config := &ConnectionConfig{
			Retry:  RetryConfig{MaxRetries: 3, InitialDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond},
			Dialer: mockDialer,
		}
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		onDialError := func(e error) {
			mu.Lock()
			defer mu.Unlock()
			capturedErrors = append(capturedErrors, e)
		}

		err = conn.Connect(context.Background(), onDialError)
		assert.NoError(t, err)
		defer conn.Close()

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []error{dialErr, dialErr}, capturedErrors)
	})

	t.Run("onDialError not called when first dial succeeds", func(t *testing.T) {
		callbackCalled := false
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}
		config := &ConnectionConfig{
			Retry:  RetryConfig{Disabled: true},
			Dialer: mockDialer,
		}
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		err = conn.Connect(context.Background(), func(error) { callbackCalled = true })
		assert.NoError(t, err)
		defer conn.Close()

		assert.False(t, callbackCalled)
	})

	t.Run("nil onDialError does not panic on failed dial", func(t *testing.T) {
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}
		config := &ConnectionConfig{
			Retry:  RetryConfig{Disabled: true},
			Dialer: mockDialer,
		}
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		assert.NotPanics(t, func() {
			conn.Connect(context.Background(), nil)
		})
	})
}

func TestConnectContextCancellation(t *testing.T) {
	t.Run("context cancelled during connect returns before retries exhaust", func(t *testing.T) {
		config := &ConnectionConfig{
			Retry: RetryConfig{
				MaxRetries:   100,
				InitialDelay: 500 * time.Millisecond,
				MaxDelay:     1 * time.Second,
				JitterFactor: 0.0,
			},
		}
		dialCount := atomic.Int32{}
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(ctx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
				dialCount.Add(1)
				return nil, nil, fmt.Errorf("dial failed")
			},
		}
		config.Dialer = mockDialer
		conn, err := NewConnection(context.Background(), "ws://test", config, nil)
		assert.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			done <- conn.Connect(ctx, nil)
		}()

		// wait for first dial
		honeybeetest.Eventually(t, func() bool {
			return dialCount.Load() >= 1
		}, "expected dial")
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
	conn, err := NewConnection(context.Background(), "ws://test", nil, nil)
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
	t.Run("clean close by peer", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{
				Code: websocket.CloseNormalClosure,
				Text: "goodbye",
			}
		}

		conn, err := NewConnectionFromSocket(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return errors.Is(err, ErrPeerClosedClean)
			default:
				return false
			}
		}, "expected clean close error")
	})

	t.Run("unexpected close", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{
				Code: websocket.CloseProtocolError,
				Text: "bad protocol",
			}
		}

		conn, err := NewConnectionFromSocket(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return errors.Is(err, ErrPeerClosedUnexpected)
			default:
				return false
			}
		}, "expected unexpected close error")
	})

	t.Run("read error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		conn, err := NewConnectionFromSocket(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return errors.Is(err, ErrReadError)
			default:
				return false
			}
		}, "expected read error")

	})
}

func TestConnectionHeartbeat(t *testing.T) {
	t.Run("pinger sends ping frames", func(t *testing.T) {
		pingCount := atomic.Int32{}
		socket, _, _ := honeybeetest.SetupTestSocket(t)
		socket.WriteControlFunc = func(mt int, d []byte, dl time.Time) error {
			if mt == websocket.PingMessage {
				pingCount.Add(1)
			}
			return nil
		}

		conf, err := NewConnectionConfig(
			WithPingInterval(10 * time.Millisecond),
		)
		assert.NoError(t, err)

		conn, _ := NewConnectionFromSocket(context.Background(), socket, conf, nil)
		defer conn.Close()

		honeybeetest.Eventually(t,
			func() bool { return pingCount.Load() >= 2 },
			"expected pinger to fire")
	})

	t.Run("pong handler triggers heartbeat channel", func(t *testing.T) {
		var handler func(string) error
		socket, _, _ := honeybeetest.SetupTestSocket(t)
		socket.SetPongHandlerFunc = func(h func(string) error) { handler = h }

		conn, _ := NewConnectionFromSocket(context.Background(), socket, nil, nil)
		defer conn.Close()

		honeybeetest.Eventually(t, func() bool {
			return handler != nil
		}, "expected Connection to register PongHandler")

		if handler == nil {
			t.Fatal("pong handler was never set")
		}

		handler("") // Simulate inbound pong

		select {
		case <-conn.Heartbeat():
		case <-time.After(time.Second):
			t.Fatal("heartbeat not signaled on pong")
		}
	})
}

// Test helpers

func setupTestConnection(t *testing.T) (
	conn *Connection,
	socket *honeybeetest.MockSocket,
	incoming chan honeybeetest.MockIncomingData,
	outgoing chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	socket, incoming, outgoing = honeybeetest.SetupTestSocket(t)

	var err error
	conn, err = NewConnectionFromSocket(context.Background(), socket, nil, nil)
	assert.NoError(t, err)
	return
}
