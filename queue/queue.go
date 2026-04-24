package queue

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"sync/atomic"
)

func RunQueue(
	id string,
	ctx context.Context,
	in <-chan types.ReceivedMessage,
	out chan<- types.ReceivedMessage,
	maxQueueSize int,
	droppedCount *atomic.Uint64,
) {
	var next types.ReceivedMessage
	var queue messageQueue
	if maxQueueSize > 0 {
		queue = newBoundedRing(maxQueueSize)
	} else {
		queue = newUnboundedRing(64)
	}

	for {
		var outOrNil chan<- types.ReceivedMessage

		// enable out channel if queue is populated
		if queue.len() > 0 {
			outOrNil = out
			next = queue.peek()
		}

		select {
		case <-ctx.Done():
			return
		case msg := <-in:
			// limit queue size if maximum is configured
			if maxQueueSize > 0 && queue.len() >= maxQueueSize {
				// drop oldest message
				_ = queue.pop()
				droppedCount.Add(1)
			}
			// add new message
			queue.push(msg)
		// send next message to out channel
		case outOrNil <- next:
			_ = queue.pop()
		}
	}
}

// Ring Buffer Queue

type messageQueue interface {
	push(types.ReceivedMessage)
	pop() types.ReceivedMessage
	peek() types.ReceivedMessage
	len() int
}

type ring struct {
	buf  []types.ReceivedMessage
	head int
	size int
}

func (r *ring) len() int { return r.size }

func (r *ring) pop() types.ReceivedMessage {
	m := r.buf[r.head]
	var zero types.ReceivedMessage
	r.buf[r.head] = zero // release reference for GC
	r.head = (r.head + 1) % len(r.buf)
	r.size--
	return m
}

func (r *ring) peek() types.ReceivedMessage {
	m := r.buf[r.head]
	return m
}

// shared write at logical tail; caller guarantees space exists
func (r *ring) writeTail(m types.ReceivedMessage) {
	r.buf[(r.head+r.size)%len(r.buf)] = m
	r.size++
}

// Bounded ring

type boundedRing struct{ ring }

func newBoundedRing(cap int) *boundedRing {
	return &boundedRing{ring{buf: make([]types.ReceivedMessage, cap)}}
}

func (b *boundedRing) push(m types.ReceivedMessage) {
	if b.size == len(b.buf) {
		b.buf[b.head] = m
		b.head = (b.head + 1) % len(b.buf)
		return
	}
	b.writeTail(m)
}

// Unbounded Ring

type unboundedRing struct{ ring }

func newUnboundedRing(initialCap int) *unboundedRing {
	if initialCap < 1 {
		initialCap = 1
	}
	return &unboundedRing{ring{buf: make([]types.ReceivedMessage, initialCap)}}
}

func (u *unboundedRing) push(m types.ReceivedMessage) {
	if u.size == len(u.buf) {
		bigger := make([]types.ReceivedMessage, len(u.buf)*2)
		for i := 0; i < u.size; i++ {
			bigger[i] = u.buf[(u.head+i)%len(u.buf)]
		}
		u.buf = bigger
		u.head = 0
	}
	u.writeTail(m)
}
