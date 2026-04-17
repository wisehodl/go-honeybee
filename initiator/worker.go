package initiator

import (
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"log/slog"
	"sync"
	"time"
)

// Types

type WorkerContext struct {
	Inbox    chan<- InboxMessage
	Events   chan<- PoolEvent
	Errors   chan<- error
	Stop     <-chan struct{}
	PoolDone <-chan struct{}
	Logger   *slog.Logger
}

// Worker

type Worker struct {
	id          string
	config      *WorkerConfig
	onReconnect func() (*transport.Connection, error)
}

func NewWorker(
	id string,
	config *WorkerConfig,
	onReconnect func() (*transport.Connection, error),
	logger *slog.Logger,

) (*Worker, error) {
	w := &Worker{
		id:          id,
		config:      config,
		onReconnect: onReconnect,
	}

	return w, nil
}

func (w *Worker) Start(
	inbox chan<- InboxMessage,
	events chan<- PoolEvent,
	stop <-chan struct{},
	poolDone <-chan struct{},
	wg *sync.WaitGroup,
) {
}

func (w *Worker) runReader(conn *transport.Connection,
	messages chan<- []byte,
	heartbeat chan<- time.Time,
	reconnect chan<- struct{},
	newConn <-chan *transport.Connection,
	stop <-chan struct{},
	poolDone <-chan struct{},

) {
}

func (w *Worker) runForwarder(
	messages <-chan []byte,
	inbox chan<- []byte,
	stop <-chan struct{},
	poolDone <-chan struct{},
	maxQueueSize int,
) {
}

func (w *Worker) runHealthMonitor(
	heartbeat <-chan time.Time,
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
}

func (w *Worker) runReconnector(
	reconnect <-chan struct{},
	newConn chan<- *transport.Connection,
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
}
