package honeybee

import (
	"context"
	"errors"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"net/http"
	"slices"
	"testing"
	"time"
)

// Helpers

func setupPool(t *testing.T) (*Pool, *honeybeetest.MockDialer) {
	t.Helper()
	dialer := &honeybeetest.MockDialer{
		DialContextFunc: func(
			context.Context, string, http.Header,
		) (types.Socket, *http.Response, error) {
			return honeybeetest.NewMockSocket(), nil, nil
		},
	}
	config, _ := NewPoolConfig()
	pool, _ := NewPool(context.Background(), config, nil)
	pool.dialer = dialer
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
	t.Run("per-call dial function is used instead of pool dialer", func(t *testing.T) {
		config, _ := NewPoolConfig()
		pool, err := NewPool(context.Background(), config, nil)
		assert.NoError(t, err)

		perCallUsed := false
		dialFn := func(_ context.Context) (types.Socket, error) {
			perCallUsed = true
			return honeybeetest.NewMockSocket(), nil
		}

		// pool dialer should NOT be called
		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				context.Context, string, http.Header,
			) (types.Socket, *http.Response, error) {
				t.Error("pool dialer should not be called when per-call dialer is provided")
				return nil, nil, fmt.Errorf("unexpected call")
			},
		}

		err = pool.Connect("wss://test", WithDialFunc(dialFn))
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

func TestPoolDialFailed(t *testing.T) {
	t.Run("EventDialFailed received after failed dial", func(t *testing.T) {
		wc, _ := NewSessionConfig(WithRetryDisabled())
		poolCfg, _ := NewPoolConfig(
			WithInboxBufferSize(256),
			WithEventsBufferSize(10),
			WithSessionConfig(*wc),
		)
		pool, err := NewPool(context.Background(), poolCfg, nil)
		assert.NoError(t, err)

		dialErr := fmt.Errorf("connection refused")
		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				context.Context, string, http.Header,
			) (types.Socket, *http.Response, error) {
				return nil, nil, dialErr
			},
		}

		err = pool.Connect("wss://test")
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-pool.Events():
				return e.Kind == EventDialFailed &&
					e.ID == "wss://test" &&
					e.Err == dialErr &&
					!e.At.IsZero()
			default:
				return false
			}
		}, "expected EventDialFailed on pool.Events()")
	})

	t.Run("no EventDialFailed when dialer succeeds", func(t *testing.T) {
		pool, _ := setupPool(t)

		err := pool.Connect("wss://test")
		assert.NoError(t, err)

		expectEvent(t, pool.events, "wss://test", EventConnected)

		honeybeetest.Never(t, func() bool {
			select {
			case e := <-pool.Events():
				return e.Kind == EventDialFailed
			default:
				return false
			}
		}, "expected no EventDialFailed when dialer succeeds")

		pool.Close()
	})
}

func TestPoolRetire(t *testing.T) {
	t.Run("peer absent and EventRetired emitted after self-retire", func(t *testing.T) {
		wc, _ := NewSessionConfig(WithRetryDisabled())
		poolCfg, _ := NewPoolConfig(WithSessionConfig(*wc))
		pool, _ := NewPool(context.Background(), poolCfg, nil)

		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				context.Context, string, http.Header,
			) (types.Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("connection refused")
			},
		}

		err := pool.Connect("wss://test")
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			return !slices.Contains(pool.Peers(), "wss://test")
		}, "expected peer to be absent after self-retire")

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-pool.Events():
				return e.Kind == EventRetired && e.ID == "wss://test"
			default:
				return false
			}
		}, "expected EventRetired on pool.Events()")

		assert.ErrorIs(t, pool.Remove("wss://test"), ErrPeerNotFound)
	})

	t.Run("concurrent Remove and self-retire is safe", func(t *testing.T) {
		wc, _ := NewSessionConfig(WithRetryDisabled())
		poolCfg, _ := NewPoolConfig(WithSessionConfig(*wc))
		pool, _ := NewPool(context.Background(), poolCfg, nil)

		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				ctx context.Context, _ string, _ http.Header,
			) (types.Socket, *http.Response, error) {
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(5 * time.Millisecond):
					return nil, nil, fmt.Errorf("connection refused")
				}
			},
		}

		err := pool.Connect("wss://test")
		assert.NoError(t, err)

		removeErr := pool.Remove("wss://test")
		assert.True(t,
			removeErr == nil || errors.Is(removeErr, ErrPeerNotFound),
			"expected nil or ErrPeerNotFound, got: %v", removeErr,
		)
	})
}

func TestPoolRequestHeader(t *testing.T) {
	t.Run("default pool passes User-Agent header to DialContext", func(t *testing.T) {
		config, _ := NewPoolConfig()
		pool, _ := NewPool(context.Background(), config, nil)

		var capturedHeader http.Header
		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				_ context.Context, _ string, h http.Header,
			) (types.Socket, *http.Response, error) {
				capturedHeader = h
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}

		pool.Connect("wss://test")
		expectEvent(t, pool.events, "wss://test", EventConnected)

		assert.Equal(t, "honeybee/0.1.0", capturedHeader.Get("User-Agent"))
		pool.Close()
	})

	t.Run("WithRequestHeader passes header to DialContext", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-Custom", "value")
		config, _ := NewPoolConfig(WithRequestHeader(h))
		pool, _ := NewPool(context.Background(), config, nil)

		var capturedHeader http.Header
		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				_ context.Context, _ string, h http.Header,
			) (types.Socket, *http.Response, error) {
				capturedHeader = h
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}

		pool.Connect("wss://test")
		expectEvent(t, pool.events, "wss://test", EventConnected)

		assert.Equal(t, "value", capturedHeader.Get("X-Custom"))
		pool.Close()
	})

	t.Run("pool stores a clone - external mutation does not affect dial calls", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-Custom", "original")
		config, _ := NewPoolConfig(WithRequestHeader(h))

		// mutate original after config is built
		h.Set("X-Custom", "mutated")

		pool, _ := NewPool(context.Background(), config, nil)

		var capturedHeader http.Header
		pool.dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(
				_ context.Context, _ string, h http.Header,
			) (types.Socket, *http.Response, error) {
				capturedHeader = h
				return honeybeetest.NewMockSocket(), nil, nil
			},
		}

		pool.Connect("wss://test")
		expectEvent(t, pool.events, "wss://test", EventConnected)

		assert.Equal(t, "original", capturedHeader.Get("X-Custom"))
		pool.Close()
	})

	t.Run("WithRequestHeader(nil) stores nil cleanly", func(t *testing.T) {
		cfg, err := NewPoolConfig(WithRequestHeader(nil))
		assert.NoError(t, err)
		assert.Nil(t, cfg.RequestHeader)
	})
}

func TestPoolSend(t *testing.T) {
	mockSocket := honeybeetest.NewMockSocket()
	outgoingData := make(chan honeybeetest.MockOutgoingData, 10)
	mockSocket.WriteMessageFunc = func(msgType int, data []byte) error {
		outgoingData <- honeybeetest.MockOutgoingData{MsgType: msgType, Data: data}
		return nil
	}

	config, _ := NewPoolConfig()
	pool, err := NewPool(context.Background(), config, nil)
	assert.NoError(t, err)

	pool.dialer = &honeybeetest.MockDialer{
		DialContextFunc: func(
			context.Context, string, http.Header,
		) (types.Socket, *http.Response, error) {
			return mockSocket, nil, nil
		},
	}

	err = pool.Connect("wss://test")
	assert.NoError(t, err)
	expectEvent(t, pool.events, "wss://test", EventConnected)

	err = pool.Send("wss://test", []byte("hello"))
	assert.NoError(t, err)

	honeybeetest.ExpectWrite(t, outgoingData, websocket.TextMessage, []byte("hello"))

	pool.Close()
}
