package honeybee

import (
	"log/slog"
	"sync"
	"time"
)

// Types

// Worker Implementation

type Worker interface {
	Start(
		ctx *WorkerContext,
		wg *sync.WaitGroup,
	)
}

type WorkerContext struct {
	Inbox    chan<- InboxMessage
	Events   chan<- PoolEvent
	Errors   chan<- error
	Stop     <-chan struct{}
	PoolDone <-chan struct{}
	Logger   *slog.Logger
}

// Base Struct

type worker struct {
	id string
}

func (w *worker) runForwarder(
	messages <-chan []byte,
	inbox chan<- []byte,
	stop <-chan struct{},
	poolDone <-chan struct{},
	maxQueueSize int,
) {
}

// Initiator Worker

type InitiatorWorker struct {
	*worker
	config      *InitiatorWorkerConfig
	onReconnect func() (*Connection, error)
}

func newInitiatorWorker(
	id string,
	config *InitiatorWorkerConfig,
	onReconnect func() (*Connection, error),
	logger *slog.Logger,

) (*InitiatorWorker, error) {
	w := &InitiatorWorker{
		worker: &worker{
			id: id,
		},
		config:      config,
		onReconnect: onReconnect,
	}

	return w, nil
}

func (w *InitiatorWorker) Start(
	inbox chan<- InboxMessage,
	events chan<- PoolEvent,
	stop <-chan struct{},
	poolDone <-chan struct{},
	wg *sync.WaitGroup,
) {
}

func runReader(conn *Connection,
	messages chan<- []byte,
	heartbeat chan<- time.Time,
	reconnect chan<- struct{},
	newConn <-chan *Connection,
	stop <-chan struct{},
	poolDone <-chan struct{},

) {
}

func runHealthMonitor(
	heartbeat <-chan time.Time,
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
}

func runReconnector(
	reconnect <-chan struct{},
	newConn chan<- *Connection,
	stop <-chan struct{},
	poolDone <-chan struct{},
) {
}

// Responder Worker

type ResponderWorker struct{}
