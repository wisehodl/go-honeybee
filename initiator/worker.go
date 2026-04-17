package initiator

import (
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"sync"
	"time"
)

// Worker

type Worker struct {
	id     string
	stop   <-chan struct{}
	config *WorkerConfig
	conn   *transport.Connection
}

func NewWorker(
	id string,
	stop <-chan struct{},
	config *WorkerConfig,

) (*Worker, error) {
	w := &Worker{
		id:     id,
		stop:   stop,
		config: config,
	}

	return w, nil
}

func (w *Worker) Send(data []byte) error {
	return w.conn.Send(data)
}

func (w *Worker) Start(
	ctx WorkerContext,
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
