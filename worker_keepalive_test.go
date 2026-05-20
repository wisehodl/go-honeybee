package honeybee

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"testing"
	"time"
)

func TestRunKeepalive(t *testing.T) {
	t.Run("heartbeat resets timer, no keepalive signal fired", func(t *testing.T) {
		heartbeat := make(chan struct{})
		keepalive := make(chan struct{}, 1)
		timeout := 200 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunKeepalive(ctx, heartbeat, keepalive, timeout, nil)

		// send heartbeats faster than the timeout
		for i := 0; i < 5; i++ {
			time.Sleep(20 * time.Millisecond)
			heartbeat <- struct{}{}
		}

		// because the timer is being reset, keepalive signal should not be sent
		honeybeetest.Never(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, "unexpected keepalive signal")
	})

	t.Run("keepalive timeout fires signal", func(t *testing.T) {
		heartbeat := make(chan struct{}, 1)
		keepalive := make(chan struct{}, 1)
		timeout := 20 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunKeepalive(ctx, heartbeat, keepalive, timeout, nil)

		// send no heartbeats, wait for timeout and keepalive signal
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, "expected keepalive signal")
	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		heartbeat := make(chan struct{}, 1)
		keepalive := make(chan struct{}, 1)
		timeout := 20 * time.Second
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			RunKeepalive(ctx, heartbeat, keepalive, timeout, nil)
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

	t.Run("disabled keepalive drains heartbeats without blocking", func(t *testing.T) {
		heartbeat := make(chan struct{})
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunKeepalive(ctx, heartbeat, keepalive, 0, nil)

		// these must not block
		for i := 0; i < 5; i++ {
			heartbeat <- struct{}{}
		}

		honeybeetest.Never(t, func() bool {
			select {
			case <-keepalive:
				return true
			default:
				return false
			}
		}, "keepalive signal should not fire when disabled")
	})
}
