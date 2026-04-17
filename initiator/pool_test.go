package initiator

import (
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestPoolConnect(t *testing.T) {
	t.Run("successfully adds connection", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)

		pool.dialer = mockDialer

		err = pool.Connect("wss://test")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			select {
			case event := <-pool.events:
				return event.ID == "wss://test" && event.Kind == EventConnected
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		_, exists := pool.peers["wss://test"]
		assert.True(t, exists)

		pool.Close()
	})

	t.Run("does not add duplicate", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		err = pool.Connect("wss://test")
		assert.NoError(t, err)

		// trailing slash normalizes to same key
		err = pool.Connect("wss://test/")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "already exists")

		pool.mu.RLock()
		assert.Len(t, pool.peers, 1)
		pool.mu.RUnlock()

		pool.Close()
	})

	t.Run("fails to add connection", func(t *testing.T) {
		pool, err := NewPool(
			&PoolConfig{
				ConnectionConfig: &transport.ConnectionConfig{
					Retry: &transport.RetryConfig{
						MaxRetries:   1,
						InitialDelay: 1 * time.Millisecond,
						MaxDelay:     5 * time.Millisecond,
					}},
			}, nil)
		assert.NoError(t, err)
		pool.dialer = &honeybeetest.MockDialer{
			DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}

		err = pool.Connect("wss://test")
		assert.Error(t, err)

		pool.mu.RLock()
		assert.Len(t, pool.peers, 0)
		pool.mu.RUnlock()

		select {
		case event := <-pool.events:
			t.Fatalf("unexpected event: %+v", event)
		default:
		}

		pool.Close()
	})
}

func TestPoolRemove(t *testing.T) {
	t.Run("removes known url", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		pool.Connect("wss://test")
		expectEvent(t, pool.events, "wss://test", EventConnected)

		err = pool.Remove("wss://test/")
		assert.NoError(t, err)

		// expect a disconnected event
		expectEvent(t, pool.events, "wss://test", EventDisconnected)

		// connection no longer in pool
		pool.mu.Lock()
		defer pool.mu.Unlock()
		_, ok := pool.peers["wss://peer2"]
		assert.False(t, ok, "connection is still in pool")
	})

	t.Run("unknown url returns error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		// remove unknown connection
		err = pool.Remove("wss://unknown")
		assert.ErrorContains(t, err, "connection not found")
	})

	t.Run("closed pool returns error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		// close pool
		pool.Close()

		// attempt to remove connection
		err = pool.Remove("wss://test")
		assert.ErrorContains(t, err, "pool is closed")
	})

}

// TODO: update worker to be responsible for send
func _TestPoolSend(t *testing.T) {
	mockSocket := honeybeetest.NewMockSocket()
	outgoingData := make(chan honeybeetest.MockOutgoingData, 10)
	mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
		outgoingData <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}
		return nil
	}
	mockDialer := &honeybeetest.MockDialer{
		DialFunc: func(string, http.Header) (types.Socket, *http.Response, error) {
			return mockSocket, nil, nil
		},
	}

	pool, err := NewPool(nil, nil)
	assert.NoError(t, err)
	pool.dialer = mockDialer

	err = pool.Connect("wss://test")
	assert.NoError(t, err)
	expectEvent(t, pool.events, "wss://test", EventConnected)

	err = pool.Send("wss://test", []byte("hello"))
	assert.NoError(t, err)

	honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, []byte("hello"))

	pool.Close()
}

func expectEvent(
	t *testing.T,
	events chan PoolEvent,
	expectedURL string,
	expectedKind PoolEventKind,
) {
	t.Helper()
	assert.Eventually(t, func() bool {
		select {
		case e := <-events:
			return e.ID == expectedURL && e.Kind == expectedKind
		default:
			return false
		}
	}, honeybeetest.TestTimeout, honeybeetest.TestTick,
		fmt.Sprintf("expected event: URL=%q, Kind=%q",
			expectedURL, expectedKind))
}
