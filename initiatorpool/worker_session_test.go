package initiatorpool

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
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

func TestRunSessionDial(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		return
	}

	expectDial := func(t *testing.T, dial <-chan struct{}) {
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

	t.Run("fires dial immediately on entry", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn := setup(t)
		defer cancel()

		messages := make(chan receivedMessage, 1)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		expectDial(t, dial)
	})

	t.Run("keepalive fires dial", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn := setup(t)
		defer cancel()

		messages := make(chan receivedMessage, 1)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		// drain initial dial
		expectDial(t, dial)

		keepalive <- struct{}{}
		expectDial(t, dial)
	})

	t.Run("multiple keepalive signals each fire dial", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn := setup(t)
		defer cancel()

		messages := make(chan receivedMessage, 1)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		// drain initial dial
		expectDial(t, dial)

		for i := 0; i < 3; i++ {
			keepalive <- struct{}{}
			expectDial(t, dial)
		}
	})
}

func TestRunSessionConnect(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
		messages chan receivedMessage,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		messages = make(chan receivedMessage, 256)
		return
	}

	t.Run("w.conn set after newConn received", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		wctx := WorkerContext{Events: make(chan PoolEvent, 10)}
		defer cancel()

		conn, _, _, _ := setupWorkerTestConnection(t)
		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		newConn <- conn

		honeybeetest.Eventually(t, func() bool {
			return w.conn.Load() != nil
		}, "expected w.conn to be set")
	})

	t.Run("EventConnected emitted", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		conn, _, _, _ := setupWorkerTestConnection(t)
		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)

		newConn <- conn

		honeybeetest.Eventually(t, func() bool {
			select {
			case event := <-events:
				return event.ID == w.id && event.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")
	})
}

func TestRunSessionDisconnect(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
		messages chan receivedMessage,
		conn *transport.Connection,
		incomingData chan honeybeetest.MockIncomingData,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		messages = make(chan receivedMessage, 256)
		conn, _, incomingData, _ = setupWorkerTestConnection(t)
		return
	}

	t.Run("EventDisconnected emitted on connection close", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		close(incomingData)

		drainEvent(t, events, EventDisconnected)
	})

	t.Run("w.conn cleared after disconnect", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		close(incomingData)
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return w.conn.Load() == nil
		}, "expected w.conn to be cleared")
	})

	t.Run("dial fires again after disconnect", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		// drain the initial dial signal before disconnecting
		<-dial

		close(incomingData)
		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			select {
			case <-dial:
				return true
			default:
				return false
			}
		}, "expected dial signal after disconnect")
	})

	t.Run("second connection cycle emits EventConnected", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages, conn, incomingData := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}
		defer cancel()

		go w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		newConn <- conn
		drainEvent(t, events, EventConnected)

		close(incomingData)
		drainEvent(t, events, EventDisconnected)

		conn2, _, _, _ := setupWorkerTestConnection(t)
		newConn <- conn2

		drainEvent(t, events, EventConnected)
	})
}

func TestRunSessionCancellation(t *testing.T) {
	setup := func(t *testing.T) (
		w *Worker,
		ctx context.Context,
		cancel context.CancelFunc,
		dial chan struct{},
		keepalive chan struct{},
		newConn chan *transport.Connection,
		messages chan receivedMessage,
	) {
		t.Helper()
		ctx, cancel = context.WithCancel(context.Background())
		w = &Worker{
			ctx:       ctx,
			cancel:    cancel,
			id:        "wss://test",
			config:    GetDefaultWorkerConfig(),
			heartbeat: make(chan struct{}),
		}
		dial = make(chan struct{}, 1)
		keepalive = make(chan struct{}, 1)
		newConn = make(chan *transport.Connection, 1)
		messages = make(chan receivedMessage, 256)
		return
	}

	t.Run("ctx cancelled pre-connection exits without emitting events", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}

		done := make(chan struct{})
		go func() {
			defer close(done)
			w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
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
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}

		conn, _, _, _ := setupWorkerTestConnection(t)

		done := make(chan struct{})
		go func() {
			defer close(done)
			w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		}()

		newConn <- conn

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

	t.Run("ctx cancelled post-connection clears w.conn", func(t *testing.T) {
		w, ctx, cancel, dial, keepalive, newConn, messages := setup(t)
		events := make(chan PoolEvent, 10)
		wctx := WorkerContext{Events: events}

		conn, _, _, _ := setupWorkerTestConnection(t)

		done := make(chan struct{})
		go func() {
			defer close(done)
			w.runSession(ctx, wctx, messages, dial, keepalive, newConn)
		}()

		newConn <- conn

		drainEvent(t, events, EventConnected)

		cancel()

		drainEvent(t, events, EventDisconnected)

		honeybeetest.Eventually(t, func() bool {
			return w.conn.Load() == nil
		}, "expected w.conn to clear")
	})
}
