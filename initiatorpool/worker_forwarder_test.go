package initiatorpool

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRunForwarder(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		messages := make(chan ReceivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &DefaultWorker{Id: "wss://test"}
		go w.RunForwarder(ctx, messages, inbox, 0)

		messages <- ReceivedMessage{data: []byte("hello"), receivedAt: time.Now()}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				return string(msg.Data) == "hello" && msg.ID == "wss://test"
			default:
				return false
			}
		}, "expected message")
	})

	t.Run("oldest message dropped when queue is full", func(t *testing.T) {
		messages := make(chan ReceivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gate := make(chan struct{})
		gatedInbox := make(chan InboxMessage)

		// gate the inbox from receiving messages until the gate is opened
		go func() {
			<-gate
			for msg := range gatedInbox {
				inbox <- msg
			}
		}()

		w := &DefaultWorker{Id: "wss://test"}
		go w.RunForwarder(ctx, messages, gatedInbox, 2)

		// send three messages while the gated inbox is blocked
		messages <- ReceivedMessage{data: []byte("first"), receivedAt: time.Now()}
		messages <- ReceivedMessage{data: []byte("second"), receivedAt: time.Now()}
		messages <- ReceivedMessage{data: []byte("third"), receivedAt: time.Now()}

		// allow time for the first message to be dropped
		time.Sleep(20 * time.Millisecond)

		// close the gate, draining messages into the inbox
		close(gate)

		// receive messages from the inbox
		var received []string
		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				received = append(received, string(msg.Data))
			default:
			}
			return len(received) == 2
		}, "expected messages")

		// first message was dropped
		assert.Equal(t, []string{"second", "third"}, received)

	})

	t.Run("exits on context cancellation", func(t *testing.T) {
		messages := make(chan ReceivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := &DefaultWorker{Id: "wss://test"}
		done := make(chan struct{})
		go func() {
			w.RunForwarder(ctx, messages, inbox, 0)
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
