package responderpool

import (
	"container/list"
	"context"
	"errors"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"time"
)

type onEventFunc func(kind PoolEventKind)

type ReceivedMessage struct {
	data       []byte
	receivedAt time.Time
}

func RunReader(
	ctx context.Context,
	onPeerClose onEventFunc,

	conn *transport.Connection,
	messages chan<- ReceivedMessage,
	heartbeat chan<- struct{},
) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-conn.Incoming():
			if !ok {
				// determine exit kind
				// by default, the peer dropped unexpectedly
				kind := EventPeerDropped
				select {
				case err := <-conn.Errors():
					if errors.Is(err, transport.ErrPeerClosedClean) {
						kind = EventPeerDisconnected
					}
				default:
				}

				onPeerClose(kind)
				return
			}

			messages <- ReceivedMessage{data: data, receivedAt: time.Now()}

			select {
			case heartbeat <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func RunForwarder(
	id string,
	ctx context.Context,
	messages <-chan ReceivedMessage,
	inbox chan<- InboxMessage,
	maxQueueSize int,
) {
	queue := list.New()

	for {
		var out chan<- InboxMessage
		var next ReceivedMessage

		// enable inbox if it is populated
		if queue.Len() > 0 {
			out = inbox

			// read the first message in the queue
			next = queue.Front().Value.(ReceivedMessage)
		}

		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			// limit queue size if maximum is configured
			if maxQueueSize > 0 && queue.Len() >= maxQueueSize {
				// drop oldest message
				queue.Remove(queue.Front())
			}
			// add new message
			queue.PushBack(msg)
		// send next message to inbox
		case out <- InboxMessage{
			ID:         id,
			Data:       next.data,
			ReceivedAt: next.receivedAt,
		}:
			// drop message from queue
			queue.Remove(queue.Front())
		}
	}
}

func RunWatchdog(
	ctx context.Context,
	onTimeout onEventFunc,
	heartbeat <-chan struct{},
	timeout time.Duration,
) {
	// disable watchdog timeout if not configured
	if timeout <= 0 {
		// wait for cancel and exit
		select {
		case <-ctx.Done():
		}
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat:
			// drain the timer channel and reset
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		// timer completed
		case <-timer.C:
			// signal peer is inactive
			onTimeout(EventPeerInactive)
			return
		}
	}
}
