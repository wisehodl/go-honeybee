package initiatorpool

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunDialer(t *testing.T) {
	t.Run("successful dial delivers connection to newConn", func(t *testing.T) {
		url := "wss://test"
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockSocket := honeybeetest.NewMockSocket()
		pool := PoolPlugin{
			Errors: make(chan error, 1),
			Dialer: &honeybeetest.MockDialer{
				DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
					return mockSocket, nil, nil
				},
			},
		}

		go RunDialer(url, ctx, pool, dial, newConn)
		dial <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-newConn:
				return true
			default:
				return false
			}
		}, "expected new connection")
	})

	t.Run("concurrent dial signals are drained; only one connection produced.",
		func(t *testing.T) {
			url := "wss://test"
			dial := make(chan struct{}, 1)
			newConn := make(chan *transport.Connection, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gate := make(chan struct{})
			dialCount := atomic.Int32{}

			mockSocket := honeybeetest.NewMockSocket()
			connConfig := &transport.ConnectionConfig{Retry: nil} // disable retry
			started := make(chan struct{})
			startOnce := sync.Once{}
			pool := PoolPlugin{
				Errors: make(chan error, 1),
				Dialer: &honeybeetest.MockDialer{
					DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
						dialCount.Add(1)
						startOnce.Do(func() { close(started) })
						<-gate
						return mockSocket, nil, nil
					},
				},
				ConnectionConfig: connConfig,
			}

			go RunDialer(url, ctx, pool, dial, newConn)
			dial <- struct{}{}

			// wait for dial to start blocking on gate
			<-started

			// flood dial while dialer is blocked
			for i := 0; i < 5; i++ {
				select {
				case dial <- struct{}{}:
				default:
				}
			}

			close(gate)

			// connection is cleared to connect
			honeybeetest.Eventually(t, func() bool {
				select {
				case <-newConn:
					return true
				default:
					return false
				}
			}, "expected new connection")

			// connection was only dialed once
			assert.Equal(t, int32(1), dialCount.Load())

			// dial channel still writable
			select {
			case dial <- struct{}{}:
			default:
				t.Fatal("dial channel should still accept sends")
			}
		})

	t.Run("dial failure emits error, succeeds on next signal", func(t *testing.T) {
		url := "wss://test"
		errors := make(chan error, 1)
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// use atomic counter to fail first dial and pass second
		dialCount := atomic.Int32{}
		mockSocket := honeybeetest.NewMockSocket()
		connConfig := &transport.ConnectionConfig{Retry: nil} // disable retry
		pool := PoolPlugin{
			Errors: errors,
			Dialer: &honeybeetest.MockDialer{
				DialContextFunc: func(
					context.Context, string, http.Header,
				) (types.Socket, *http.Response, error) {
					if dialCount.Add(1) == 1 {
						// fail first
						return nil, nil, fmt.Errorf("dial failed")
					}
					// pass second
					return mockSocket, nil, nil
				},
			},
			ConnectionConfig: connConfig,
		}

		go RunDialer(url, ctx, pool, dial, newConn)
		dial <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-errors:
				return err != nil
			default:
				return false
			}
		}, "expected error")

		dial <- struct{}{}

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-newConn:
				return true
			default:
				return false
			}
		}, "expected new connection")
	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		url := "wss://test"
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())

		pool := PoolPlugin{Errors: make(chan error, 1)}

		done := make(chan struct{})
		go func() {
			RunDialer(url, ctx, pool, dial, newConn)
			close(done)
		}()

		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected done signal")
	})

	t.Run("context cancelled during in-progress dial exits without delivering connection", func(t *testing.T) {
		url := "wss://test"
		dial := make(chan struct{}, 1)
		newConn := make(chan *transport.Connection, 1)
		ctx, cancel := context.WithCancel(context.Background())

		pool := PoolPlugin{
			Errors:           make(chan error, 1),
			ConnectionConfig: &transport.ConnectionConfig{Retry: nil},
			Dialer: &honeybeetest.MockDialer{
				DialContextFunc: func(ctx context.Context, _ string, _ http.Header) (types.Socket, *http.Response, error) {
					// block until context is cancelled
					select {
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					}
				},
			},
		}

		done := make(chan struct{})
		go func() {
			RunDialer(url, ctx, pool, dial, newConn)
			close(done)
		}()

		dial <- struct{}{}

		// wait for dialer to block
		time.Sleep(20 * time.Millisecond)
		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected done signal")

		// no connection was sent
		assert.Empty(t, newConn)
	})
}
