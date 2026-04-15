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

		select {
		case event := <-pool.events:
			assert.Equal(t, "wss://test", event.URL)
			assert.Equal(t, EventConnected, event.Kind)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for Connected event")
		}

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
