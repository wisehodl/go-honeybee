package inbound

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
	"time"
)

// Helpers

func setupPool(t *testing.T) *Pool {
	t.Helper()
	pool, err := NewPool(context.Background(), nil, nil)
	assert.NoError(t, err)
	return pool
}

func expectEvent(
	t *testing.T,
	events <-chan PoolEvent,
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

// Tests

func TestPoolAdd(t *testing.T) {
	t.Run("successfully adds peer", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.NoError(t, err)
	})

	t.Run("peer appears in Peers after add", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.NoError(t, err)

		assert.Contains(t, pool.Peers(), "peer-1")
	})

	t.Run("duplicate id returns ErrPeerExists", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket1, _, _ := setupTestSocket(t)
		socket2, _, _ := setupTestSocket(t)

		err := pool.Add("peer-1", socket1)
		assert.NoError(t, err)

		err = pool.Add("peer-1", socket2)
		assert.ErrorIs(t, err, ErrPeerExists)
	})

	t.Run("closed pool returns ErrPoolClosed", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.ErrorIs(t, err, ErrPoolClosed)
	})
}

func TestPoolReplace(t *testing.T) {
	t.Run("replaces existing peer", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket1, _, _ := setupTestSocket(t)
		socket2, _, _ := setupTestSocket(t)

		err := pool.Add("peer-1", socket1)
		assert.NoError(t, err)

		err = pool.Replace("peer-1", socket2)
		assert.NoError(t, err)

		assert.Contains(t, pool.Peers(), "peer-1")
	})

	t.Run("unknown id returns ErrPeerNotFound", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Replace("unknown", socket)
		assert.ErrorIs(t, err, ErrPeerNotFound)
	})

	t.Run("closed pool returns ErrPoolClosed", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Replace("peer-1", socket)
		assert.ErrorIs(t, err, ErrPoolClosed)
	})

	t.Run("no event emitted for replaced peer", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket1, _, _ := setupTestSocket(t)
		socket2, _, _ := setupTestSocket(t)

		err := pool.Add("peer-1", socket1)
		assert.NoError(t, err)

		err = pool.Replace("peer-1", socket2)
		assert.NoError(t, err)

		honeybeetest.Never(t, func() bool {
			select {
			case <-pool.Events():
				return true
			default:
				return false
			}
		}, "no event expected on replace")
	})
}

func TestPoolRemove(t *testing.T) {
	t.Run("removes known peer", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.NoError(t, err)

		err = pool.Remove("peer-1")
		assert.NoError(t, err)

		assert.NotContains(t, pool.Peers(), "peer-1")
	})

	t.Run("unknown id returns ErrPeerNotFound", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		err := pool.Remove("unknown")
		assert.ErrorIs(t, err, ErrPeerNotFound)
	})

	t.Run("closed pool returns ErrPoolClosed", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()

		err := pool.Remove("peer-1")
		assert.ErrorIs(t, err, ErrPoolClosed)
	})

	t.Run("no event emitted on remove", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.NoError(t, err)

		err = pool.Remove("peer-1")
		assert.NoError(t, err)

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-pool.Events():
				fmt.Printf("got event: %v", e)
				return true
			default:
				return false
			}
		}, "no event expected on remove")
	})
}

func TestPoolSend(t *testing.T) {
	t.Run("data reaches socket", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, outgoing := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.NoError(t, err)

		err = pool.Send("peer-1", []byte("hello"))
		assert.NoError(t, err)

		honeybeetest.ExpectWrite(t, outgoing, websocket.TextMessage, []byte("hello"))
	})

	t.Run("unknown id returns ErrPeerNotFound", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		err := pool.Send("unknown", []byte("hello"))
		assert.ErrorIs(t, err, ErrPeerNotFound)
	})

	t.Run("closed pool returns ErrPoolClosed", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()

		err := pool.Send("peer-1", []byte("hello"))
		assert.ErrorIs(t, err, ErrPoolClosed)
	})
}

func TestPoolClose(t *testing.T) {
	t.Run("inbox and events channels close after pool close", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()

		_, ok := <-pool.Inbox()
		assert.False(t, ok)
		_, ok = <-pool.Events()
		assert.False(t, ok)
	})

	t.Run("add after close returns ErrPoolClosed", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()

		socket, _, _ := setupTestSocket(t)
		err := pool.Add("peer-1", socket)
		assert.ErrorIs(t, err, ErrPoolClosed)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		pool := setupPool(t)
		pool.Close()
		pool.Close()
	})
}

func TestPoolPeers(t *testing.T) {
	t.Run("reflects active peers after add", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket1, _, _ := setupTestSocket(t)
		socket2, _, _ := setupTestSocket(t)

		pool.Add("peer-1", socket1)
		pool.Add("peer-2", socket2)

		peers := pool.Peers()
		assert.Contains(t, peers, "peer-1")
		assert.Contains(t, peers, "peer-2")
	})

	t.Run("loses entry after remove", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		pool.Add("peer-1", socket)
		pool.Remove("peer-1")

		assert.NotContains(t, pool.Peers(), "peer-1")
	})

	t.Run("loses entry after peer self-disconnects", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, incoming, _ := setupTestSocket(t)
		pool.Add("peer-1", socket)

		close(incoming)

		honeybeetest.Eventually(t, func() bool {
			return !slices.Contains(pool.Peers(), "peer-1")
		}, "expected peer to be removed after self-disconnect")
	})
}

func TestPoolEvents(t *testing.T) {
	t.Run("EventPeerDisconnected emitted on clean close", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, incoming, _ := setupTestSocket(t)
		pool.Add("peer-1", socket)

		incoming <- honeybeetest.MockIncomingData{
			Err: &websocket.CloseError{Code: websocket.CloseNormalClosure},
		}

		expectEvent(t, pool.Events(), "peer-1", EventPeerDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return !slices.Contains(pool.Peers(), "peer-1")
		}, "expected peer auto-removed")
	})

	t.Run("EventPeerDropped emitted on unexpected close", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, incoming, _ := setupTestSocket(t)
		pool.Add("peer-1", socket)

		incoming <- honeybeetest.MockIncomingData{
			Err: &websocket.CloseError{Code: websocket.CloseProtocolError},
		}

		expectEvent(t, pool.Events(), "peer-1", EventPeerDropped)

		honeybeetest.Eventually(t, func() bool {
			return !slices.Contains(pool.Peers(), "peer-1")
		}, "expected peer auto-removed")
	})

	t.Run("EventPeerEvicted emitted on watchdog timeout", func(t *testing.T) {
		config, err := NewPoolConfig(
			WithWorkerConfig(&WorkerConfig{DeadTimeout: 20 * time.Millisecond}),
		)
		assert.NoError(t, err)

		pool, err := NewPool(context.Background(), config, nil)
		assert.NoError(t, err)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		pool.Add("peer-1", socket)

		expectEvent(t, pool.Events(), "peer-1", EventPeerEvicted)

		honeybeetest.Eventually(t, func() bool {
			return !slices.Contains(pool.Peers(), "peer-1")
		}, "expected peer auto-removed")
	})

	t.Run("no event emitted on Remove", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket, _, _ := setupTestSocket(t)
		pool.Add("peer-1", socket)
		pool.Remove("peer-1")

		honeybeetest.Never(t, func() bool {
			select {
			case <-pool.Events():
				return true
			default:
				return false
			}
		}, "no event expected on Remove")
	})

	t.Run("no event emitted on Replace of old peer", func(t *testing.T) {
		pool := setupPool(t)
		defer pool.Close()

		socket1, _, _ := setupTestSocket(t)
		socket2, _, _ := setupTestSocket(t)

		pool.Add("peer-1", socket1)
		pool.Replace("peer-1", socket2)

		honeybeetest.Never(t, func() bool {
			select {
			case <-pool.Events():
				return true
			default:
				return false
			}
		}, "no event expected on Replace")
	})
}
