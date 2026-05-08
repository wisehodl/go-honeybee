package queue

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunQueue(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		id := "wss://test"
		inChan := make(chan types.ReceivedMessage, 1)
		outChan := make(chan types.ReceivedMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunQueue(id, ctx, inChan, outChan, 0, &atomic.Uint64{}, &atomic.Int64{})

		inChan <- types.ReceivedMessage{Data: []byte("hello"), ReceivedAt: time.Now()}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-outChan:
				return string(msg.Data) == "hello"
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("oldest message dropped when queue is full", func(t *testing.T) {
		id := "wss://test"
		inChan := make(chan types.ReceivedMessage, 1)
		outChan := make(chan types.ReceivedMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gate := make(chan struct{})
		gatedInbox := make(chan types.ReceivedMessage)

		// gate the inbox from receiving messages until the gate is opened
		go func() {
			<-gate
			for msg := range gatedInbox {
				outChan <- msg
			}
		}()

		go RunQueue(id, ctx, inChan, gatedInbox, 2, &atomic.Uint64{}, &atomic.Int64{})

		// send three messages while the gated inbox is blocked
		inChan <- types.ReceivedMessage{Data: []byte("first"), ReceivedAt: time.Now()}
		inChan <- types.ReceivedMessage{Data: []byte("second"), ReceivedAt: time.Now()}
		inChan <- types.ReceivedMessage{Data: []byte("third"), ReceivedAt: time.Now()}

		// allow time for the first message to be dropped
		time.Sleep(20 * time.Millisecond)

		// close the gate, draining messages into the inbox
		close(gate)

		// receive messages from the inbox
		var received []string
		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-outChan:
				received = append(received, string(msg.Data))
			default:
			}
			return len(received) == 2
		}, "expected messages")

		// first message was dropped
		assert.Equal(t, []string{"second", "third"}, received)

	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		id := "wss://test"
		inChan := make(chan types.ReceivedMessage, 1)
		outChan := make(chan types.ReceivedMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			RunQueue(id, ctx, inChan, outChan, 0, &atomic.Uint64{}, &atomic.Int64{})
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
