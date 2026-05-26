package honeybee

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
)

// Helpers

func setupPool(t *testing.T) (*Pool, *honeybeetest.MockDialer) {
	t.Helper()
	dialer := &honeybeetest.MockDialer{
		DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
			return honeybeetest.NewMockSocket(), nil, nil
		},
	}
	cc := *transport.GetDefaultConnectionConfig()
	cc.Dialer = dialer
	pool, err := NewPool(context.Background(), &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		ConnectionConfig: cc,
		WorkerConfig:     *GetDefaultWorkerConfig(),
	}, nil)
	assert.NoError(t, err)
	return pool, dialer
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
			return e.ID == expectedURL && e.Kind == expectedKind && !e.At.IsZero()
		default:
			return false
		}
	}, fmt.Sprintf("expected event: URL=%q, Kind=%q", expectedURL, expectedKind))
}

// Tests

func TestPoolConnect(t *testing.T) {
	t.Run("successfully adds connection", func(t *testing.T) {
		pool, _ := setupPool(t)

		err := pool.Connect("wss://test")
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case event := <-pool.events:
				return event.ID == "wss://test" && event.Kind == EventConnected
			default:
				return false
			}
		}, "expected event")

		assert.Contains(t, pool.Peers(), "wss://test")

		pool.Close()
	})

	t.Run("does not add duplicate", func(t *testing.T) {
		pool, _ := setupPool(t)

		err := pool.Connect("wss://test")
		assert.NoError(t, err)

		// trailing slash normalizes to same key
		err = pool.Connect("wss://test/")
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPeerExists)

		assert.Len(t, pool.Peers(), 1)

		pool.Close()
	})
}

func TestPoolConnectWithDialer(t *testing.T) {
	t.Run("per-call dialer is used instead of pool dialer", func(t *testing.T) {
		perCallUsed := false
		perCallDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(ctx context.Context, url string, h http.Header) (types.Socket, *http.Response, error) {
				perCallUsed = true
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}

		// pool dialer should NOT be called
		poolDialer := &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				t.Error("pool dialer should not be called when per-call dialer is provided")
				return nil, nil, fmt.Errorf("unexpected call")
			},
		}

		cc := *transport.GetDefaultConnectionConfig()
		cc.Dialer = poolDialer
		pool, err := NewPool(context.Background(), &PoolConfig{
			InboxBufferSize:  256,
			EventsBufferSize: 10,
			ConnectionConfig: cc,
			WorkerConfig:     *GetDefaultWorkerConfig(),
		}, nil)
		assert.NoError(t, err)

		err = pool.Connect("wss://test", WithDialer(perCallDialer))
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-pool.events:
				return e.ID == "wss://test" && e.Kind == EventConnected
			default:
				return false
			}
		}, "expected connected event")

		assert.True(t, perCallUsed, "per-call dialer was not used")
		pool.Close()
	})
}

func TestPoolClose(t *testing.T) {
	t.Run("channels close after pool close", func(t *testing.T) {
		pool, _ := NewPool(context.Background(), nil, nil)
		pool.Close()
		_, ok := <-pool.Inbox()
		assert.False(t, ok)
		_, ok = <-pool.Events()
		assert.False(t, ok)
	})

	t.Run("connect after close returns error", func(t *testing.T) {
		pool, _ := NewPool(context.Background(), nil, nil)
		pool.Close()
		err := pool.Connect("wss://test")
		assert.ErrorIs(t, err, ErrPoolClosed)
	})
}

func TestPoolRemove(t *testing.T) {
	t.Run("removes known url", func(t *testing.T) {
		pool, _ := setupPool(t)

		pool.Connect("wss://test")
		expectEvent(t, pool.events, "wss://test", EventConnected)

		err := pool.Remove("wss://test/")
		assert.NoError(t, err)

		// expect a disconnected event
		expectEvent(t, pool.events, "wss://test", EventDisconnected)

		// connection no longer in pool
		assert.NotContains(t, pool.Peers(), "wss://test")
	})

	t.Run("unknown url returns error", func(t *testing.T) {
		pool, _ := setupPool(t)

		// remove unknown connection
		err := pool.Remove("wss://unknown")
		assert.ErrorIs(t, err, ErrPeerNotFound)
	})

	t.Run("closed pool returns error", func(t *testing.T) {
		pool, _ := setupPool(t)

		// close pool
		pool.Close()

		// attempt to remove connection
		err := pool.Remove("wss://test")
		assert.ErrorIs(t, err, ErrPoolClosed)
	})

}

func TestPoolSend(t *testing.T) {
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

	cc := *transport.GetDefaultConnectionConfig()
	cc.Dialer = mockDialer
	pool, err := NewPool(context.Background(), &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		ConnectionConfig: cc,
		WorkerConfig:     *GetDefaultWorkerConfig(),
	}, nil)
	assert.NoError(t, err)

	err = pool.Connect("wss://test")
	assert.NoError(t, err)
	expectEvent(t, pool.events, "wss://test", EventConnected)

	err = pool.Send("wss://test", []byte("hello"))
	assert.NoError(t, err)

	honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, []byte("hello"))

	pool.Close()
}
