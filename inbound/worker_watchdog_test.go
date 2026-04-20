package inbound

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
		go RunWatchdog(ctx, func(WorkerExitKind) { called.Store(true) }, heartbeat, 200*time.Millisecond)

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

		var gotKind WorkerExitKind
		count := atomic.Int32{}
		done := make(chan struct{})
		go RunWatchdog(ctx, func(kind WorkerExitKind) {
			count.Add(1)
			gotKind = kind
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
		assert.Equal(t, ExitPolicy, gotKind)
	})

	t.Run("ctx.Done exits without calling onInactive", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			RunWatchdog(ctx, func(WorkerExitKind) { called.Store(true) }, heartbeat, 20*time.Second)
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

	t.Run("zero timeout exits on ctx.Done without firing onInactive", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		called := atomic.Bool{}
		done := make(chan struct{})
		go func() {
			RunWatchdog(ctx, func(WorkerExitKind) { called.Store(true) }, heartbeat, 0)
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

	t.Run("disabled keepalive drains heartbeats without blocking", func(t *testing.T) {
		heartbeat := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			RunWatchdog(ctx, func(WorkerExitKind) {}, heartbeat, 0)
			close(done)
		}()

		// these must not block
		for i := 0; i < 5; i++ {
			heartbeat <- struct{}{}
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
