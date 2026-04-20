package responderpool

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunWatchdog(t *testing.T) {
	t.Run("heartbeat resets timer, onInactive not called", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		called := atomic.Bool{}
		go RunWatchdog(ctx, func() { called.Store(true) }, heartbeat, 200*time.Millisecond)

		for i := 0; i < 5; i++ {
			time.Sleep(20 * time.Millisecond)
			heartbeat <- struct{}{}
		}

		honeybeetest.Never(t, func() bool {
			return called.Load()
		}, "unexpected onInactive call")
	})

	t.Run("timeout fires onInactive exactly once", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		count := atomic.Int32{}
		done := make(chan struct{})
		go RunWatchdog(ctx, func() {
			count.Add(1)
			close(done)
		}, heartbeat, 20*time.Millisecond)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected onInactive")

		assert.Equal(t, int32(1), count.Load())
	})

	t.Run("ctx.Done exits without calling onInactive", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			RunWatchdog(ctx, func() { called.Store(true) }, heartbeat, 20*time.Second)
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

	t.Run("zero timeout exits on ctx.Done without firing", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			RunWatchdog(ctx, func() { called.Store(true) }, heartbeat, 0)
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
}
