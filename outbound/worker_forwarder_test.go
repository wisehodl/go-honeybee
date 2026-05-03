package outbound

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunForwarder(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		id := "wss://test"
		messages := make(chan types.ReceivedMessage, 1)
		inbox := make(chan types.InboxMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go RunForwarder(id, ctx, messages, inbox, &atomic.Uint64{}, &atomic.Uint64{})

		messages <- types.ReceivedMessage{Data: []byte("hello"), ReceivedAt: time.Now()}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				return string(msg.Data) == "hello" && msg.ID == "wss://test"
			default:
				return false
			}
		}, "expected message")
	})
}
