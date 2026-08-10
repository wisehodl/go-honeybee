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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func setupTestConnection(t *testing.T) (
	conn *Connection,
	socket *honeybeetest.MockSocket,
	incoming chan honeybeetest.MockIncomingData,
	outgoing chan honeybeetest.MockOutgoingData,
) {
	t.Helper()

	socket, incoming, outgoing = honeybeetest.SetupTestSocket(t)

	var err error
	conn, err = NewConnection(context.Background(), socket, nil, nil)
	assert.NoError(t, err)
	return
}

// ----------------------------------------------------------------------------
// Constructor
// ----------------------------------------------------------------------------

func TestNewConnection(t *testing.T) {
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
			config: func() *ConnectionConfig {
				c, _ := NewConnectionConfig(WithWriteTimeout(30 * time.Second))
				return c
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := NewConnection(
				context.Background(), tc.socket, tc.config, nil)

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
			assert.Equal(t, tc.socket, conn.socket)
			assert.NotNil(t, conn.config)
			assert.NotNil(t, conn.incoming)
			assert.NotNil(t, conn.errors)
			assert.NotNil(t, conn.done)
			assert.False(t, conn.closed)

			// Verify default config is used if nil is passed.
			gotCfg := conn.config
			if tc.config == nil {
				expected, _ := NewConnectionConfig()
				assert.Equal(t, *expected, gotCfg)
			} else {
				expected := *tc.config
				assert.Equal(t, expected, gotCfg)
			}
		})
	}
}

func TestNewConnection_ReadLimit(t *testing.T) {
	socket, _, _ := honeybeetest.SetupTestSocket(t)

	var limitWasSet bool
	var setLimit int64
	socket.SetReadLimitFunc = func(limit int64) {
		limitWasSet = true
		setLimit = limit
	}

	// when ReadLimit is nil, SetReadLimit should not be called
	config, _ := NewConnectionConfig()
	NewConnection(context.Background(), socket, config, nil)
	assert.False(t, limitWasSet)

	// when ReadLimit is set to zero, SetReadLimit should be called
	config, _ = NewConnectionConfig(
		WithReadLimit(0),
	)
	NewConnection(context.Background(), socket, config, nil)
	assert.True(t, limitWasSet)
	assert.Equal(t, int64(0), setLimit)

	// when ReadLimit is set to a positive value, SetReadLimit should be called
	limitWasSet = false
	config, _ = NewConnectionConfig(
		WithReadLimit(100),
	)
	NewConnection(context.Background(), socket, config, nil)
	assert.True(t, limitWasSet)
	assert.Equal(t, int64(100), setLimit)
}

// ----------------------------------------------------------------------------
// Accessors
// ----------------------------------------------------------------------------

func TestConnection_Incoming(t *testing.T) {
	conn, _, _, _ := setupTestConnection(t)

	incoming := conn.Incoming()
	assert.NotNil(t, incoming)

	// send data through the channel to verify they are the same
	testData := []byte("test")
	conn.incoming <- testData
	received := <-incoming
	assert.Equal(t, testData, received)
}

func TestConnection_Errors(t *testing.T) {
	t.Run("clean close by peer", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, &websocket.CloseError{
				Code: websocket.CloseNormalClosure,
				Text: "goodbye",
			}
		}

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
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

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
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

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
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

	t.Run("read limit exceeded", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, websocket.ErrReadLimit
		}

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return errors.Is(err, ErrReadLimit) &&
					errors.Is(err, ErrReadError) &&
					!errors.Is(err, websocket.ErrReadLimit)
			default:
				return false
			}
		}, "expected read limit error classified as ErrReadLimit")

	})

	t.Run("generic read error is not a read limit error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-conn.Errors():
				return errors.Is(err, ErrReadError) &&
					!errors.Is(err, ErrReadLimit)
			default:
				return false
			}
		}, "expected generic read error without ErrReadLimit")

	})
}

func TestConnection_Heartbeat(t *testing.T) {
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

		conn, _ := NewConnection(context.Background(), socket, conf, nil)
		defer conn.Close()

		honeybeetest.Eventually(t,
			func() bool { return pingCount.Load() >= 2 },
			"expected pinger to fire")
	})

	t.Run("pong handler triggers heartbeat channel", func(t *testing.T) {
		var handler func(string) error
		socket, _, _ := honeybeetest.SetupTestSocket(t)
		socket.SetPongHandlerFunc = func(h func(string) error) { handler = h }

		conn, _ := NewConnection(context.Background(), socket, nil, nil)
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

// ----------------------------------------------------------------------------
// Send
// ----------------------------------------------------------------------------

func TestConnection_Send(t *testing.T) {
	t.Run("writes message to socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		testData := []byte("test message")
		err := conn.Send(testData)
		assert.NoError(t, err)

		honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, testData)
	})

	t.Run("writes multiple message to socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		messages := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
		for _, msg := range messages {
			err := conn.Send(msg)
			assert.NoError(t, err)
		}

		for _, expected := range messages {
			honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, expected)
		}
	})

	t.Run("concurrent sends write messages to socket", func(t *testing.T) {
		conn, _, _, outgoingData := setupTestConnection(t)
		defer conn.Close()

		mu := sync.Mutex{}
		messages := []string{}
		done := make(chan struct{})

		go func() {
			for {
				select {
				case msg := <-outgoingData:
					mu.Lock()
					messages = append(messages, string(msg.Data))
					mu.Unlock()
				case <-done:
					return
				}
			}
		}()

		defer close(done)

		var wg sync.WaitGroup
		for i := range 5 {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := range 10 {
					data := fmt.Appendf(nil, "msg-%d-%d", id, j)
					for {
						// send and retry until success
						err := conn.Send(data)
						if err != nil {
							continue
						} else {
							break
						}
					}
				}
			}(i)
		}

		wg.Wait()

		honeybeetest.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(messages) == 50
		}, "should have received 50 messages")

	})

	t.Run("send fails when connection is closed", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)
		conn.Close()

		testData := []byte("test message")
		err := conn.Send(testData)
		assert.ErrorIs(t, err, ErrConnectionClosed)
	})

	t.Run("write timeout disabled when zero", func(t *testing.T) {
		config, _ := NewConnectionConfig(
			WithWriteTimeout(0),
		)

		outgoingData := make(chan honeybeetest.MockOutgoingData, 10)
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
			})
			return nil
		}

		deadlineCalled := make(chan struct{}, 1)
		mockSocket.SetWriteDeadlineFunc = func(t time.Time) error {
			deadlineCalled <- struct{}{}
			return nil
		}

		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			select {
			case outgoingData <- honeybeetest.MockOutgoingData{
				MsgType: msgType, Data: data}:
			case <-mockSocket.Closed:
				return io.EOF
			}
			return nil
		}

		conn, err := NewConnection(context.Background(), mockSocket, config, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		honeybeetest.Never(t, func() bool {
			select {
			case <-deadlineCalled:
				return true
			default:
				return false
			}
		}, "SetWriteDeadline should not be called when timeout is zero")
	})

	t.Run("write timeout sets deadline when positive", func(t *testing.T) {
		config, _ := NewConnectionConfig()

		outgoingData := make(chan honeybeetest.MockOutgoingData, 10)
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
			})
			return nil
		}

		deadlineCalled := make(chan struct{}, 1)
		mockSocket.SetWriteDeadlineFunc = func(t time.Time) error {
			deadlineCalled <- struct{}{}
			return nil
		}

		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			select {
			case outgoingData <- honeybeetest.MockOutgoingData{
				MsgType: msgType, Data: data}:
			case <-mockSocket.Closed:
				return io.EOF
			}
			return nil
		}

		conn, err := NewConnection(context.Background(), mockSocket, config, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-deadlineCalled:
				return true
			default:
				return false
			}
		}, "SetWriteDeadline should be called when timeout is positive")
	})

	t.Run("send fails on deadline error", func(t *testing.T) {
		config, _ := NewConnectionConfig(WithWriteTimeout(1 * time.Millisecond))

		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
			})
			return nil
		}

		mockSocket.SetWriteDeadlineFunc = func(t time.Time) error {
			return fmt.Errorf("test error")
		}

		conn, err := NewConnection(context.Background(), mockSocket, config, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.ErrorIs(t, err, ErrFailedWriteDeadline)

		honeybeetest.Never(t, func() bool {
			_, ok := <-conn.errors
			return !ok
		}, "write error does not close connection")
	})

	t.Run("send fails on socket write error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()

		writeErr := fmt.Errorf("test error")
		mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
			return writeErr
		}

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)
		defer conn.Close()

		err = conn.Send([]byte("test"))
		assert.ErrorIs(t, err, ErrWriteFailed)
		assert.ErrorContains(t, err, "test error")
	})
}

// ----------------------------------------------------------------------------
// Reader
// ----------------------------------------------------------------------------

func TestStartReader(t *testing.T) {
	t.Run("text messages route to incoming channel", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t)
		defer conn.Close()

		testData := []byte("hello")
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    testData,
			Err:     nil,
		}

		honeybeetest.ExpectIncoming(t, conn.Incoming(), testData)
	})

	t.Run("binary messages route to incoming channel", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t)
		defer conn.Close()

		testData := []byte{0x00, 0x01, 0x02}
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.BinaryMessage,
			Data:    testData,
			Err:     nil,
		}

		honeybeetest.ExpectIncoming(t, conn.Incoming(), testData)
	})

	t.Run("multiple messages processed sequentially", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t)
		defer conn.Close()

		messages := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
		for _, msg := range messages {
			incomingData <- honeybeetest.MockIncomingData{
				MsgType: websocket.TextMessage, Data: msg, Err: nil}
		}

		for _, expected := range messages {
			honeybeetest.ExpectIncoming(t, conn.Incoming(), expected)
		}
	})

	t.Run("reader exits on socket read error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() {
				close(mockSocket.Closed)
			})
			return nil
		}

		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			return 0, nil, io.EOF
		}

		conn, err := NewConnection(context.Background(), mockSocket, nil, nil)
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			_, ok := <-conn.errors
			return !ok
		}, "expected channel closure")
	})
}

// ----------------------------------------------------------------------------
// Close
// ----------------------------------------------------------------------------

func TestDisconnectedConnectionClose(t *testing.T) {
	t.Run("close is idempotent", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)

		conn.Close()
		conn.Close()
	})

	t.Run("channels close after close", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)

		conn.Close()

		assert.True(t, conn.closed)
		_, ok := <-conn.incoming
		assert.False(t, ok)
		_, ok = <-conn.errors
		assert.False(t, ok)
	})

	t.Run("send fails after close", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)

		conn.Close()

		err := conn.Send([]byte("test"))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionClosed)
	})

}

func TestConnectedConnectionClose(t *testing.T) {
	t.Run("blocked on ReadMessage, unblocks on closed", func(t *testing.T) {
		conn, _, incomingData, _ := setupTestConnection(t)

		// Send a message to ensure reader loop is blocking
		canary := []byte("canary")
		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage, Data: canary}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-conn.Incoming():
				return bytes.Equal(msg, canary)
			default:
				return false
			}
		}, "expected canary message")

		conn.Close()
	})
}
