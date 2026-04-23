package inbound

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/logging"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"time"
)

// Types

type PoolEventKind int

const (
	EventDisconnected PoolEventKind = iota
	EventDropped
	EventEvicted
)

var workerToPoolEvent = map[WorkerExitKind]PoolEventKind{
	ExitDisconnected: EventDisconnected,
	ExitError:        EventDropped,
	ExitPolicy:       EventEvicted,
}

type OnExitFunction func(kind WorkerExitKind)

type PoolEvent struct {
	ID   string
	Kind PoolEventKind
}

type InboxMessage struct {
	ID         string
	Data       []byte
	ReceivedAt time.Time
}

type PoolPlugin struct {
	Inbox   chan<- InboxMessage
	Events  chan<- PoolEvent
	Errors  chan<- error
	OnExit  OnExitFunction
	Handler slog.Handler
}

// Pool

type Peer struct {
	id     string
	conn   *transport.Connection
	worker Worker
	done   chan struct{}
}

type Pool struct {
	ctx    context.Context
	cancel context.CancelFunc

	id string

	peers  map[string]*Peer
	inbox  chan InboxMessage
	events chan PoolEvent
	errors chan error

	config  *PoolConfig
	handler slog.Handler
	logger  *slog.Logger

	mu     sync.RWMutex
	wg     sync.WaitGroup
	closed bool
}

func NewPool(ctx context.Context, id string, config *PoolConfig, handler slog.Handler) (*Pool, error) {
	if id == "" {
		return nil, ErrInvalidPoolID
	}

	if config == nil {
		config = GetDefaultPoolConfig()
	}

	// If a custom factory is supplied, config.WorkerConfig is not used.
	// The factory function should be non-blocking or else Connect() may cause
	// deadlocks.
	if config.WorkerFactory == nil {
		// TODO: Construct worker logger
		config.WorkerFactory = func(
			ctx context.Context,
			id string,
			conn *transport.Connection,
			config *WorkerConfig,
			logger *slog.Logger,
		) (Worker, error) {
			return NewWorker(ctx, id, conn, config, logger)
		}
	}

	if err := ValidatePoolConfig(config); err != nil {
		return nil, err
	}

	pctx, cancel := context.WithCancel(ctx)

	var logger *slog.Logger
	if handler != nil {
		logger = logging.NewInboundPoolLogger(handler, id)
	}

	return &Pool{
		ctx:     pctx,
		cancel:  cancel,
		id:      id,
		peers:   make(map[string]*Peer),
		inbox:   make(chan InboxMessage, config.InboxBufferSize),
		events:  make(chan PoolEvent, config.EventsBufferSize),
		errors:  make(chan error, config.ErrorsBufferSize),
		config:  config,
		handler: handler,
		logger:  logger,
	}, nil
}

func (p *Pool) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]string, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}

	return ids
}

func (p *Pool) Inbox() <-chan InboxMessage {
	return p.inbox
}

func (p *Pool) Events() <-chan PoolEvent {
	return p.events
}

func (p *Pool) Errors() <-chan error {
	return p.errors
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	p.cancel()

	// close all connections
	for _, peer := range p.peers {
		peer.worker.Stop()
		peer.conn.Close()
	}

	// remove all peers
	p.peers = make(map[string]*Peer)

	p.mu.Unlock()

	go func() {
		p.wg.Wait()
		close(p.inbox)
		close(p.events)
		close(p.errors)
	}()
}

func (p *Pool) Add(id string, socket types.Socket) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	if _, exists := p.peers[id]; exists {
		return ErrPeerExists
	}

	return p.addLocked(id, socket)
}

func (p *Pool) Replace(id string, socket types.Socket) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	if peer, exists := p.peers[id]; exists {
		p.removeLocked(peer)
	} else {
		return ErrPeerNotFound
	}

	return p.addLocked(id, socket)
}

func (p *Pool) Remove(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	peer, exists := p.peers[id]
	if !exists {
		return ErrPeerNotFound
	}

	p.removeLocked(peer)

	return nil
}

func (p *Pool) Send(id string, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrPoolClosed
	}

	peer, exists := p.peers[id]
	if !exists {
		return ErrPeerNotFound
	}

	return peer.worker.Send(data)
}

// addLocked constructs and registers a peer. Caller must hold p.mu write lock.
func (p *Pool) addLocked(id string, socket types.Socket) error {
	var logger *slog.Logger
	if p.handler != nil {
		logger = logging.NewConnectionLogger(p.handler, p.id, id)
	}

	conn, err := transport.NewConnectionFromSocket(
		socket, p.config.ConnectionConfig, logger)
	if err != nil {
		return err
	}

	// The worker factory must be non-blocking to avoid deadlocks
	wctx, cancel := context.WithCancel(p.ctx)
	if p.handler != nil {
		logger = logging.NewInboundWorkerLogger(p.handler, p.id, id)
	}

	worker, err := p.config.WorkerFactory(wctx, id, conn, p.config.WorkerConfig, logger)
	if err != nil {
		cancel()
		conn.Close()
		return fmt.Errorf("%w: %w", PoolError, err)
	}

	var once sync.Once
	onExit := func(kind WorkerExitKind) {
		once.Do(func() {
			p.mu.Lock()
			delete(p.peers, id)
			p.mu.Unlock()

			conn.Close()

			select {
			case p.events <- PoolEvent{ID: id, Kind: workerToPoolEvent[kind]}:
			case <-p.ctx.Done():
				return
			}
		})
	}

	pool := PoolPlugin{
		Inbox:   p.inbox,
		Events:  p.events,
		Errors:  p.errors,
		OnExit:  onExit,
		Handler: p.handler,
	}

	peer := &Peer{
		id:     id,
		conn:   conn,
		worker: worker,
		done:   make(chan struct{}),
	}

	p.wg.Add(1)
	go func() {
		defer cancel()
		defer close(peer.done)
		worker.Start(pool)
		p.wg.Done()
	}()

	p.peers[id] = peer

	return nil
}

// removeLocked closes and unregisters a peer. Caller must hold p.mu write lock.
func (p *Pool) removeLocked(peer *Peer) {
	delete(p.peers, peer.id)
	peer.worker.Stop()
	go func() {
		<-peer.done
		peer.conn.Close()
	}()
}
