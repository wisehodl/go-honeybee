package initiatorpool

import (
	"context"
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

// TODO: Worker must connect and emit events.
func _TestPoolConnect(t *testing.T) {
	t.Run("successfully adds connection", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(context.Background(), nil, nil)
		assert.NoError(t, err)

		pool.dialer = mockDialer

		err = pool.Connect("wss://test")
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case event := <-pool.events:
				return event.ID == "wss://test" && event.Kind == EventConnected
			default:
				return false
			}
		}, "expected event")

		_, exists := pool.peers["wss://test"]
		assert.True(t, exists)

		pool.Close()
	})

	t.Run("does not add duplicate", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(context.Background(), nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		err = pool.Connect("wss://test")
		assert.NoError(t, err)

		// trailing slash normalizes to same key
		err = pool.Connect("wss://test/")
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPeerExists)

		pool.mu.RLock()
		assert.Len(t, pool.peers, 1)
		pool.mu.RUnlock()

		pool.Close()
	})

	t.Run("fails to add connection", func(t *testing.T) {
		pool, err := NewPool(
			context.Background(),
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
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
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

// TODO: Worker must stop connection and emit events
func _TestPoolRemove(t *testing.T) {
	t.Run("removes known url", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(context.Background(), nil, nil)
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
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(context.Background(), nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		// remove unknown connection
		err = pool.Remove("wss://unknown")
		assert.ErrorIs(t, err, ErrPeerNotFound)
	})

	t.Run("closed pool returns error", func(t *testing.T) {
		mockSocket := honeybeetest.NewMockSocket()
		mockDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(context.Background(), nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		// close pool
		pool.Close()

		// attempt to remove connection
		err = pool.Remove("wss://test")
		assert.ErrorIs(t, err, ErrPoolClosed)
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
		DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
			return mockSocket, nil, nil
		},
	}

	pool, err := NewPool(context.Background(), nil, nil)
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
	honeybeetest.Eventually(t, func() bool {
		select {
		case e := <-events:
			return e.ID == expectedURL && e.Kind == expectedKind
		default:
			return false
		}
	}, fmt.Sprintf("expected event: URL=%q, Kind=%q", expectedURL, expectedKind))
}
