package inbound

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"time"
)

// Types

type PoolEventKind string

const (
	EventPeerDisconnected PoolEventKind = "disconnected"
	EventPeerDropped      PoolEventKind = "dropped"
	EventPeerEvicted      PoolEventKind = "evicted"
)

var workerToPoolEvent = map[WorkerExitKind]PoolEventKind{
	ExitCleanDisconnect: EventPeerDisconnected,
	ExitUnexpectedDrop:  EventPeerDropped,
	ExitInactive:        EventPeerEvicted,
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
	Inbox  chan<- InboxMessage
	Events chan<- PoolEvent
	Logger *slog.Logger
	OnExit OnExitFunction
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

	peers  map[string]*Peer
	inbox  chan InboxMessage
	events chan PoolEvent

	config *PoolConfig
	logger *slog.Logger

	mu     sync.RWMutex
	wg     sync.WaitGroup
	closed bool
}

func NewPool(ctx context.Context, config *PoolConfig, logger *slog.Logger) (*Pool, error) {
	if config == nil {
		config = GetDefaultPoolConfig()
	}

	// If a custom factory is supplied, config.WorkerConfig is not used.
	// The factory function should be non-blocking or else Connect() may cause
	// deadlocks.
	if config.WorkerFactory == nil {
		config.WorkerFactory = func(
			ctx context.Context,
			id string,
			conn *transport.Connection,
			config *WorkerConfig,
		) (Worker, error) {
			return NewWorker(ctx, id, conn, config)
		}
	}

	if err := ValidatePoolConfig(config); err != nil {
		return nil, err
	}

	pctx, cancel := context.WithCancel(ctx)

	return &Pool{
		ctx:    pctx,
		cancel: cancel,
		peers:  make(map[string]*Peer),
		inbox:  make(chan InboxMessage, 256),
		events: make(chan PoolEvent, 10),
		config: config,
		logger: logger,
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

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	p.cancel()

	// remove all peers
	p.peers = make(map[string]*Peer)

	// close all connections
	for _, peer := range p.peers {
		peer.worker.Stop()
		peer.conn.Close()
	}

	p.mu.Unlock()

	go func() {
		p.wg.Wait()
		close(p.inbox)
		close(p.events)
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
	conn, err := transport.NewConnectionFromSocket(
		socket, p.config.ConnectionConfig, p.logger)
	if err != nil {
		return err
	}

	// The worker factory must be non-blocking to avoid deadlocks
	wctx, cancel := context.WithCancel(p.ctx)
	worker, err := p.config.WorkerFactory(wctx, id, conn, p.config.WorkerConfig)
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

	var logger *slog.Logger
	if p.logger != nil {
		logger = p.logger.With("id", id)
	}

	pool := PoolPlugin{
		Inbox:  p.inbox,
		Events: p.events,
		Logger: logger,
		OnExit: onExit,
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
		worker.Start(pool, &p.wg)
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
