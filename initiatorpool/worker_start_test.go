package initiatorpool

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/honeybeetest"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"testing"
)

func makeWorkerContext(t *testing.T) (
	inbox chan InboxMessage,
	events chan PoolEvent,
	errors chan error,
	pool PoolPlugin,
) {
	t.Helper()
	inbox = make(chan InboxMessage, 256)
	events = make(chan PoolEvent, 10)
	errors = make(chan error, 10)
	pool = PoolPlugin{
		Inbox:  inbox,
		Events: events,
		Errors: errors,
	}
	return
}

func makeWorker(t *testing.T, ctx context.Context, cancel context.CancelFunc) *DefaultWorker {
	t.Helper()
	return &DefaultWorker{
		Ctx:       ctx,
		Cancel:    cancel,
		Id:        "wss://test",
		Config:    GetDefaultWorkerConfig(),
		Heartbeat: make(chan struct{}),
	}
}

func mockDialer(socket *honeybeetest.MockSocket) *honeybeetest.MockDialer {
	return &honeybeetest.MockDialer{
		DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
			return socket, nil, nil
		},
	}
}

func TestWorkerStart(t *testing.T) {
	t.Run("EventConnected emitted after dial succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, _, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.ID == w.Id && e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")
	})

	t.Run("Send delivers data to socket", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, _, pool := makeWorkerContext(t)
		_, mockSocket, _, outgoingData := setupWorkerTestConnection(t)
		pool.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		err := w.Send([]byte("hello"))
		assert.NoError(t, err)

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-outgoingData:
				return string(msg.Data) == "hello"
			default:
				return false
			}
		}, "expected data on socket")
	})

	t.Run("socket data arrives on Inbox", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		inbox, events, _, pool := makeWorkerContext(t)

		incomingData := make(chan honeybeetest.MockIncomingData, 10)
		mockSocket := honeybeetest.NewMockSocket()

		mockSocket.CloseFunc = func() error {
			mockSocket.Once.Do(func() { close(mockSocket.Closed) })
			return nil
		}

		mockSocket.ReadMessageFunc = func() (int, []byte, error) {
			select {
			case data := <-incomingData:
				return data.MsgType, data.Data, data.Err
			}
		}

		pool.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		incomingData <- honeybeetest.MockIncomingData{
			MsgType: websocket.TextMessage,
			Data:    []byte("hello"),
		}

		honeybeetest.Eventually(t, func() bool {
			select {
			case msg := <-inbox:
				return msg.ID == w.Id && string(msg.Data) == "hello"
			default:
				return false
			}
		}, "expected message on Inbox")
	})

	t.Run("socket close produces EventDisconnected then EventConnected", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, _, pool := makeWorkerContext(t)
		_, mockSocket, incomingData, _ := setupWorkerTestConnection(t)
		pool.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		close(incomingData)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected EventDisconnected")

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected second EventConnected")
	})

	t.Run("Stop produces EventDisconnected and wg drains", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, events, _, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		w.Stop()

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventDisconnected
			default:
				return false
			}
		}, "expected EventDisconnected")

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected wg to drain")
	})

	t.Run("parent context cancel exits cleanly and wg drains", func(t *testing.T) {
		parentCtx, parentCancel := context.WithCancel(context.Background())
		workerCtx, workerCancel := context.WithCancel(parentCtx)

		w := makeWorker(t, workerCtx, workerCancel)
		_, events, _, pool := makeWorkerContext(t)
		mockSocket := honeybeetest.NewMockSocket()
		pool.Dialer = mockDialer(mockSocket)

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case e := <-events:
				return e.Kind == EventConnected
			default:
				return false
			}
		}, "expected EventConnected")

		// drain events after parent cancel — we don't assert what they are,
		// only that the worker exits
		parentCancel()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		honeybeetest.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, "expected wg to drain after parent cancel")
	})

	t.Run("dial failure emits to Errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := makeWorker(t, ctx, cancel)
		_, _, errors, pool := makeWorkerContext(t)
		pool.ConnectionConfig = &transport.ConnectionConfig{Retry: nil}
		pool.Dialer = &honeybeetest.MockDialer{
			DialContextFunc: func(context.Context, string, http.Header) (types.Socket, *http.Response, error) {
				return nil, nil, fmt.Errorf("dial failed")
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go w.Start(pool, &wg)

		honeybeetest.Eventually(t, func() bool {
			select {
			case err := <-errors:
				return err != nil
			default:
				return false
			}
		}, "expected error on Errors channel")
	})
}
