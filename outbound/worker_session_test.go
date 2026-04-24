package outbound

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"sync/atomic"
	"testing"
)

func drainEvent(t *testing.T, events <-chan PoolEvent, kind PoolEventKind) {
	t.Helper()
	honeybeetest.Eventually(t, func() bool {
		select {
		case e := <-events:
			return e.Kind == kind
		default:
			return false
		}
	}, fmt.Sprintf("expected %s event", kind))
}

type testVars struct {
	id string

	dial      chan struct{}
	keepalive chan struct{}
	heartbeat chan struct{}
	newConn   chan *transport.Connection
	messages  chan types.ReceivedMessage

	conn         *transport.Connection
	mockSocket   *honeybeetest.MockSocket
	incomingData chan honeybeetest.MockIncomingData
	outgoingData chan honeybeetest.MockOutgoingData

	connPtr *atomic.Pointer[transport.Connection]
}

func setup(t *testing.T) (
	ctx context.Context,
	cancel context.CancelFunc,
	vars testVars,
) {
	t.Helper()
	ctx, cancel = context.WithCancel(context.Background())
	conn, mockSocket, incomingData, outgoingData := setupTestConnection(t)
	vars = testVars{
		id:           "wss://test",
		dial:         make(chan struct{}, 1),
		keepalive:    make(chan struct{}, 1),
		heartbeat:    make(chan struct{}, 1),
		newConn:      make(chan *transport.Connection, 1),
		messages:     make(chan types.ReceivedMessage, 256),
		conn:         conn,
		mockSocket:   mockSocket,
		incomingData: incomingData,
		outgoingData: outgoingData,
		connPtr:      &atomic.Pointer[transport.Connection]{},
	}
	return
}

func expectDial(t *testing.T, dial <-chan struct{}) {
	t.Helper()
	honeybeetest.Eventually(t, func() bool {
		select {
		case <-dial:
			return true
		default:
			return false
		}
	}, "expected dial signal")
}

func TestRunSessionDial(t *testing.T) {
	t.Run("fires dial immediately on entry", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		pool := PoolPlugin{Events: make(chan PoolEvent, 10)}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		go session.Start(ctx, pool)

		expectDial(t, v.dial)
	})

	t.Run("keepalive fires dial", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		pool := PoolPlugin{Events: make(chan PoolEvent, 10)}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		go session.Start(ctx, pool)

		// drain initial dial
		expectDial(t, v.dial)

		v.keepalive <- struct{}{}
		expectDial(t, v.dial)
	})

	t.Run("multiple keepalive signals each fire dial", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		pool := PoolPlugin{Events: make(chan PoolEvent, 10)}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		go session.Start(ctx, pool)

		// drain initial dial
		expectDial(t, v.dial)

		for i := 0; i < 3; i++ {
			v.keepalive <- struct{}{}
			expectDial(t, v.dial)
		}
	})
}

func TestRunSessionConnect(t *testing.T) {
	t.Run("connection pointer set after newConn received", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		pool := PoolPlugin{Events: make(chan PoolEvent, 10)}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		go session.Start(ctx, pool)

		v.newConn <- v.conn

		honeybeetest.Eventually(t, func() bool {
			return v.connPtr.Load() != nil
		}, "expected connection pointer to be set")
	})

	t.Run("EventConnected emitted", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		go session.Start(ctx, pool)

		v.newConn <- v.conn

		honeybeetest.Eventually(t, func() bool {
			select {
			case event := <-events:
				return event.ID == v.id && event.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")
	})
}

func TestRunSessionDisconnect(t *testing.T) {
	t.Run("EventDisconnected emitted on connection close", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:           v.id,
			connPtr:      v.connPtr,
			messages:     v.messages,
			heartbeat:    v.heartbeat,
			dial:         v.dial,
			keepalive:    v.keepalive,
			newConn:      v.newConn,
			restartCount: &atomic.Uint64{},
		}

		go session.Start(ctx, pool)

		v.newConn <- v.conn
		drainEvent(t, events, EventConnected)

		close(v.incomingData)

		drainEvent(t, events, EventDisconnected)
	})

	t.Run("connection pointer cleared after disconnect", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:           v.id,
			connPtr:      v.connPtr,
			messages:     v.messages,
			heartbeat:    v.heartbeat,
			dial:         v.dial,
			keepalive:    v.keepalive,
			newConn:      v.newConn,
			restartCount: &atomic.Uint64{},
		}

		go session.Start(ctx, pool)

		v.newConn <- v.conn
		drainEvent(t, events, EventConnected)

		close(v.incomingData)
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return v.connPtr.Load() == nil
		}, "expected connection pointer to be nil")
	})

	t.Run("dial fires again after disconnect", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:           v.id,
			connPtr:      v.connPtr,
			messages:     v.messages,
			heartbeat:    v.heartbeat,
			dial:         v.dial,
			keepalive:    v.keepalive,
			newConn:      v.newConn,
			restartCount: &atomic.Uint64{},
		}

		go session.Start(ctx, pool)

		v.newConn <- v.conn
		drainEvent(t, events, EventConnected)

		// drain the initial dial signal before disconnecting
		<-v.dial

		close(v.incomingData)
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-v.dial:
				return true
			default:
				return false
			}
		}, "expected dial signal after disconnect")
	})

	t.Run("second connection cycle emits EventConnected", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		defer cancel()

		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:           v.id,
			connPtr:      v.connPtr,
			messages:     v.messages,
			heartbeat:    v.heartbeat,
			dial:         v.dial,
			keepalive:    v.keepalive,
			newConn:      v.newConn,
			restartCount: &atomic.Uint64{},
		}

		go session.Start(ctx, pool)

		v.newConn <- v.conn
		drainEvent(t, events, EventConnected)

		close(v.incomingData)
		drainEvent(t, events, EventDisconnected)

		conn2, _, _, _ := setupTestConnection(t)
		v.newConn <- conn2
		drainEvent(t, events, EventConnected)
	})
}

func TestRunSessionCancellation(t *testing.T) {
	t.Run("ctx cancelled pre-connection exits without emitting events", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			session.Start(ctx, pool)
		}()

		cancel()

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected runSession to exit")

		honeybeetest.Never(t, func() bool {
			select {
			case <-events:
				return true
			default:
				return false
			}
		}, "expected no events emitted")
	})

	t.Run("ctx cancelled post-connection emits EventDisconnected", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			session.Start(ctx, pool)
		}()

		v.newConn <- v.conn
		drainEvent(t, events, EventConnected)

		cancel()
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected runSession to exit")
	})

	t.Run("ctx cancelled post-connection clears connection pointer", func(t *testing.T) {
		ctx, cancel, v := setup(t)
		events := make(chan PoolEvent, 10)
		pool := PoolPlugin{Events: events}
		session := &Session{
			id:        v.id,
			connPtr:   v.connPtr,
			messages:  v.messages,
			heartbeat: v.heartbeat,
			dial:      v.dial,
			keepalive: v.keepalive,
			newConn:   v.newConn,
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			session.Start(ctx, pool)
		}()

		v.newConn <- v.conn
		drainEvent(t, events, EventConnected)

		cancel()
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return v.connPtr.Load() == nil
		}, "expected connection pointer to be nil")
	})
}
