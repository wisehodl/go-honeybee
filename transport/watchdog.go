package transport

import (
	"context"
	"time"
)

func IdleWatchdog(
	ctx context.Context,
	activity <-chan struct{},
	timeout time.Duration,
	onTimeout func(),
) {
	// disable watchdog timeout if not configured
	if timeout <= 0 {
		for {
			select {
			case <-activity:
			case <-ctx.Done():
				return
			}
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-activity:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		case <-timer.C:
			onTimeout()
			return
		}
	}
}
