package honeybee

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestPoolAdd(t *testing.T) {
	t.Run("successfully adds connection", func(t *testing.T) {
		mockSocket := NewMockSocket()
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)

		pool.dialer = mockDialer

		err = pool.Add("wss://test")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			select {
			case event := <-pool.events:
				return event.URL == "wss://test" && event.Kind == EventConnected
			default:
				return false
			}
		}, testTimeout, testTick)

		pool.Close()
	})

	t.Run("does not add duplicate", func(t *testing.T) {
		mockSocket := NewMockSocket()
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		err = pool.Add("wss://test")
		assert.NoError(t, err)

		// trailing slash normalizes to same key
		err = pool.Add("wss://test/")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "already exists")

		pool.mu.RLock()
		assert.Len(t, pool.connections, 1)
		pool.mu.RUnlock()

		pool.Close()
	})

	t.Run("fails to add connection", func(t *testing.T) {
		pool, err := NewPool(&Config{
			Retry: &RetryConfig{
				MaxRetries:   1,
				InitialDelay: 1 * time.Millisecond,
				MaxDelay:     5 * time.Millisecond,
			},
		}, nil)
		assert.NoError(t, err)
		pool.dialer = &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}

		err = pool.Add("wss://test")
		assert.Error(t, err)

		pool.mu.RLock()
		assert.Len(t, pool.connections, 0)
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
		mockSocket := NewMockSocket()
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		pool.Add("wss://peer1")
		pool.Add("wss://peer2")

		// expect two connection events
		counter := 2
		assert.Eventually(t, func() bool {
			if counter == 0 {
				return true
			}
			select {
			case e := <-pool.events:
				counter = counter - 1
				assert.Equal(t, EventConnected, e.Kind)
			}
			return false
		}, testTimeout, testTick, "expected connection events")

		// remove a connection
		err = pool.Remove("wss://peer2/")
		assert.NoError(t, err)

		// expect a disconnected event
		assert.Eventually(t, func() bool {
			select {
			case e := <-pool.events:
				return assert.Equal(t, EventDisconnected, e.Kind)
			default:
				return false
			}
		}, testTimeout, testTick, "expected disconnected event")

		// connection no longer in pool
		pool.mu.Lock()
		defer pool.mu.Unlock()
		_, ok := pool.connections["wss://peer2"]
		assert.False(t, ok, "connection is still in pool")
	})

	t.Run("unknown url returns error", func(t *testing.T) {
		mockSocket := NewMockSocket()
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		pool.Add("wss://peer1")
		pool.Add("wss://peer2")

		// remove unknown connection
		err = pool.Remove("wss://unknown")
		assert.ErrorContains(t, err, "connection not found")
	})

	t.Run("closed pool returns error", func(t *testing.T) {
		mockSocket := NewMockSocket()
		mockDialer := &MockDialer{
			DialFunc: func(string, http.Header) (Socket, *http.Response, error) {
				return mockSocket, nil, nil
			},
		}

		pool, err := NewPool(nil, nil)
		assert.NoError(t, err)
		pool.dialer = mockDialer

		pool.Add("wss://peer1")
		pool.Add("wss://peer2")

		// close pool
		pool.Close()

		// attempt to remove connection
		err = pool.Remove("wss://peer1")
		assert.ErrorContains(t, err, "pool is closed")
	})

}
