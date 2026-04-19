package initiatorpool

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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &DefaultWorker{
			Config:    &WorkerConfig{KeepaliveTimeout: 100 * time.Millisecond},
			Heartbeat: heartbeat,
		}
		go w.RunKeepalive(ctx, keepalive)

		// send heartbeats faster than the timeout
		for i := 0; i < 5; i++ {
			time.Sleep(30 * time.Millisecond)
			w.Heartbeat <- struct{}{}
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
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &DefaultWorker{Config: &WorkerConfig{KeepaliveTimeout: 20 * time.Millisecond}}
		go w.RunKeepalive(ctx, keepalive)

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
		keepalive := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())

		w := &DefaultWorker{Config: &WorkerConfig{KeepaliveTimeout: 20 * time.Second}}
		done := make(chan struct{})
		go func() {
			w.RunKeepalive(ctx, keepalive)
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
}
