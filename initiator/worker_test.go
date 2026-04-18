package initiator

import (
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	// "git.wisehodl.dev/jay/go-honeybee/transport"
	// "git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/stretchr/testify/assert"
	// "net/http"
	"testing"
	"time"
)

// Forwarder

func TestRunForwarder(t *testing.T) {
	t.Run("message passes through to inbox", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		stop := make(chan struct{})
		defer close(stop)

		w := &Worker{id: "wss://test"}
		go w.runForwarder(messages, inbox, stop, nil, 0)

		messages <- receivedMessage{data: []byte("hello"), receivedAt: time.Now()}

		assert.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				return string(msg.Data) == "hello" && msg.ID == "wss://test"
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

	t.Run("oldest message dropped when queue is full", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		stop := make(chan struct{})
		defer close(stop)

		gate := make(chan struct{})
		gatedInbox := make(chan InboxMessage)

		// gate the inbox from receiving messages until the gate is opened
		go func() {
			<-gate
			for msg := range gatedInbox {
				inbox <- msg
			}
		}()

		w := &Worker{id: "wss://test"}
		go w.runForwarder(messages, gatedInbox, stop, nil, 2)

		// send three messages while the gated inbox is blocked
		messages <- receivedMessage{data: []byte("first"), receivedAt: time.Now()}
		messages <- receivedMessage{data: []byte("second"), receivedAt: time.Now()}
		messages <- receivedMessage{data: []byte("third"), receivedAt: time.Now()}

		// allow time for the first message to be dropped
		time.Sleep(20 * time.Millisecond)

		// close the gate, draining messages into the inbox
		close(gate)

		// receive messages from the inbox
		var received []string
		assert.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				received = append(received, string(msg.Data))
			default:
			}
			return len(received) == 2
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)

		// first message was dropped
		assert.Equal(t, []string{"second", "third"}, received)

	})

	t.Run("exits on stop", func(t *testing.T) {
		messages := make(chan receivedMessage, 1)
		inbox := make(chan InboxMessage, 1)
		stop := make(chan struct{})

		w := &Worker{id: "wss://test"}
		done := make(chan struct{})
		go func() {
			w.runForwarder(messages, inbox, stop, nil, 0)
			close(done)
		}()

		close(stop)
		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})
}

func TestRunHealthMonitor(t *testing.T) {
	t.Run("heartbeat resets timer, no reconnect fired", func(t *testing.T) {
		heartbeat := make(chan struct{}, 3)
		reconnect := make(chan struct{}, 1)
		stop := make(chan struct{})
		defer close(stop)

		w := &Worker{config: &WorkerConfig{ReconnectTimeout: 100 * time.Millisecond}}
		go w.runHealthMonitor(heartbeat, reconnect, stop, nil)

		// send heartbeats faster than the timeout
		for i := 0; i < 5; i++ {
			time.Sleep(30 * time.Millisecond)
			heartbeat <- struct{}{}
		}

		// because the timer is being reset, reconnect should never occur
		assert.Never(t, func() bool {
			select {
			case <-reconnect:
				return true
			default:
				return false
			}
		}, honeybeetest.NegativeTestTimeout, honeybeetest.TestTick)
	})

	t.Run("reconnect timeout fires reconnect", func(t *testing.T) {
		heartbeat := make(chan struct{})
		reconnect := make(chan struct{}, 1)
		stop := make(chan struct{})
		defer close(stop)

		w := &Worker{config: &WorkerConfig{ReconnectTimeout: 20 * time.Millisecond}}
		go w.runHealthMonitor(heartbeat, reconnect, stop, nil)

		// send no heartbeats, wait for timeout and reconnect signal
		assert.Eventually(t, func() bool {
			select {
			case <-reconnect:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

	t.Run("exits on stop", func(t *testing.T) {
		heartbeat := make(chan struct{})
		reconnect := make(chan struct{}, 1)
		stop := make(chan struct{})

		w := &Worker{config: &WorkerConfig{ReconnectTimeout: 20 * time.Second}}
		done := make(chan struct{})
		go func() {
			w.runHealthMonitor(heartbeat, reconnect, stop, nil)
			close(done)
		}()

		// send stop signal
		close(stop)
		assert.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, honeybeetest.TestTimeout, honeybeetest.TestTick)
	})

}

func TestRunReconnector(t *testing.T) {
	t.Run("reconnect emits events, new connection", func(t *testing.T) {})
	t.Run("dial failure emits error", func(t *testing.T) {})
	t.Run("exits on stop", func(t *testing.T) {})
}
