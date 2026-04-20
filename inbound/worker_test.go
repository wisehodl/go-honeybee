package inbound

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type workerTestVars struct {
	worker   *DefaultWorker
	conn     *transport.Connection
	incoming chan honeybeetest.MockIncomingData
	outgoing chan honeybeetest.MockOutgoingData
	pool     PoolPlugin
	inbox    chan InboxMessage
	events   chan PoolEvent
	exitKind *atomic.Value
	wg       *sync.WaitGroup
}

func setupWorkerTest(t *testing.T) workerTestVars {
	t.Helper()

	conn, _, incoming, outgoing := setupTestConnection(t)

	ctx, cancel := context.WithCancel(context.Background())
	var err error
	worker, err := NewWorker(ctx, "peer-1", conn, nil)
	assert.NoError(t, err)
	worker.cancel = cancel

	inbox := make(chan InboxMessage, 256)
	events := make(chan PoolEvent, 10)
	exitKind := &atomic.Value{}

	var once sync.Once
	pool := PoolPlugin{
		Inbox:  inbox,
		Events: events,
		OnExit: func(kind WorkerExitKind) {
			once.Do(func() { exitKind.Store(kind) })
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	return workerTestVars{
		worker:   worker,
		conn:     conn,
		incoming: incoming,
		outgoing: outgoing,
		pool:     pool,
		inbox:    inbox,
		events:   events,
		exitKind: exitKind,
		wg:       wg,
	}
}

func TestWorkerStart(t *testing.T) {
	t.Run("socket data arrives on inbox", func(t *testing.T) {
		v := setupWorkerTest(t)
		defer v.worker.Stop()

		go v.worker.Start(v.pool, v.wg)

		v.incoming <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    []byte("hello"),
		}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-v.inbox:
				return msg.ID == "peer-1" && string(msg.Data) == "hello"
			default:
				return false
			}
		}, "expected message on inbox")
	})

	t.Run("clean peer close calls OnExit with ExitCleanDisconnect", func(t *testing.T) {
		v := setupWorkerTest(t)
		defer v.worker.Stop()

		go v.worker.Start(v.pool, v.wg)

		v.incoming <- honeybeetest.MockIncomingData{
			Err: &websocket.CloseError{Code: websocket.CloseNormalClosure},
		}

		honeybeetest.Eventually(t, func() bool {
			val := v.exitKind.Load()
			return val != nil && val.(WorkerExitKind) == ExitDisconnected
		}, "expected ExitCleanDisconnect")
	})

	t.Run("unexpected peer close calls OnExit with ExitUnexpectedDrop", func(t *testing.T) {
		v := setupWorkerTest(t)
		defer v.worker.Stop()

		go v.worker.Start(v.pool, v.wg)

		v.incoming <- honeybeetest.MockIncomingData{
			Err: &websocket.CloseError{Code: websocket.CloseProtocolError},
		}

		honeybeetest.Eventually(t, func() bool {
			val := v.exitKind.Load()
			return val != nil && val.(WorkerExitKind) == ExitError
		}, "expected ExitUnexpectedDrop")
	})

	t.Run("watchdog timeout calls OnExit with ExitInactive", func(t *testing.T) {
		conn, _, _, _ := setupTestConnection(t)

		ctx, cancel := context.WithCancel(context.Background())
		worker, err := NewWorker(ctx, "peer-1", conn, &WorkerConfig{
			InactivityTimeout: 20 * time.Millisecond,
		})
		assert.NoError(t, err)
		worker.cancel = cancel
		defer worker.Stop()

		exitKind := &atomic.Value{}
		var once sync.Once
		pool := PoolPlugin{
			Inbox:  make(chan InboxMessage, 256),
			Events: make(chan PoolEvent, 10),
			OnExit: func(kind WorkerExitKind) {
				once.Do(func() { exitKind.Store(kind) })
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go worker.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			val := exitKind.Load()
			return val != nil && val.(WorkerExitKind) == ExitPolicy
		}, "expected ExitInactive")
	})
}

func TestWorkerStop(t *testing.T) {
	v := setupWorkerTest(t)

	go v.worker.Start(v.pool, v.wg)

	v.worker.Stop()

	done := make(chan struct{})
	go func() { v.wg.Wait(); close(done) }()

	honeybeetest.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, "expected wg to drain")

	// does not call onExit
	assert.Nil(t, v.exitKind.Load())
}

func TestWorkerSend(t *testing.T) {
	t.Run("Send delivers data to socket", func(t *testing.T) {
		v := setupWorkerTest(t)
		defer v.worker.Stop()

		go v.worker.Start(v.pool, v.wg)

		err := v.worker.Send([]byte("hello"))
		assert.NoError(t, err)

		honeybeetest.ExpectWrite(t, v.outgoing, websocket.TextMessage, []byte("hello"))
	})

	t.Run("Send produces heartbeats", func(t *testing.T) {
		v := setupWorkerTest(t)
		defer v.worker.Stop()

		count := atomic.Int32{}
		go func() {
			for range v.worker.heartbeat {
				count.Add(1)
			}
		}()

		// do not start the worker, allow heartbeats to be drained manually

		for i := 0; i < 3; i++ {
			err := v.worker.Send([]byte(fmt.Sprintf("msg-%d", i)))
			assert.NoError(t, err)
		}

		honeybeetest.Eventually(t, func() bool {
			return count.Load() == 3
		}, "expected heartbeats")
	})

	t.Run("Send returns error after connection closed", func(t *testing.T) {
		v := setupWorkerTest(t)
		defer v.worker.Stop()

		go v.worker.Start(v.pool, v.wg)

		v.conn.Close()

		honeybeetest.Eventually(t, func() bool {
			return v.conn.State() == transport.StateClosed
		}, "expected connection closed")

		err := v.worker.Send([]byte("hello"))
		assert.Error(t, err)
	})
}
