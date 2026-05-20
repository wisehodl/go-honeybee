package transport

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdleWatchdog(t *testing.T) {
	t.Run("heartbeat resets timer, onTimeout not called", func(t *testing.T) {
		activity := make(chan struct{})
		ctx := t.Context()

		called := atomic.Bool{}
		go IdleWatchdog(
			ctx, activity, 200*time.Millisecond, func() { called.Store(true) },
		)

		for range 5 {
			time.Sleep(20 * time.Millisecond)
			activity <- struct{}{}
		}

		honeybeetest.Never(t, func() bool {
			return called.Load()
		}, "unexpected onTimeout call")
	})

	t.Run("timeout fires onTimeout exactly once", func(t *testing.T) {
		activity := make(chan struct{})
		ctx := t.Context()

		count := atomic.Int32{}
		done := make(chan struct{})
		go IdleWatchdog(
			ctx, activity, 20*time.Millisecond, func() {
				// will panic on second close
				count.Add(1)
				close(done)
			},
		)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onTimeout")

		assert.Equal(t, 1, int(count.Load()))
	})

	t.Run("ctx.Done exits without calling onTimeout", func(t *testing.T) {
		activity := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			IdleWatchdog(
				ctx, activity, 20*time.Second, func() { called.Store(true) },
			)
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
		}, "expected RunWatchdog to exit")

		assert.False(t, called.Load())
	})

	t.Run("zero timeout exits on ctx.Done without firing onTimeout", func(t *testing.T) {
		activity := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			IdleWatchdog(
				ctx, activity, 0, func() { called.Store(true) },
			)
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
		}, "expected RunWatchdog to exit")

		assert.False(t, called.Load())
	})

	t.Run("zero timeout drains activity without blocking", func(t *testing.T) {
		activity := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			IdleWatchdog(ctx, activity, 0, func() {})
			close(done)
		}()

		// these must not block
		for range 5 {
			activity <- struct{}{}
		}

		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected RunWatchdog to exit")
	})
}
