package inbound

import (
	"context"
	"fmt"
	"git.wisehodl.dev/jay/go-honeybee/logging"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Types

type PoolEventKind string

const (
	EventDisconnected  PoolEventKind = "disconnected"
	EventDroppedClose  PoolEventKind = "dropped_close"
	EventDroppedError  PoolEventKind = "dropped_error"
	EventEvictedPolicy PoolEventKind = "evicted_policy"
)

var workerToPoolEvent = map[WorkerExitKind]PoolEventKind{
	ExitDisconnected:    EventDisconnected,
	ExitUnexpectedClose: EventDroppedClose,
	ExitReadError:       EventDroppedError,
	ExitPolicy:          EventEvictedPolicy,
}

type OnExitFunction func(kind WorkerExitKind)

type PoolEvent struct {
	ID   string
	Kind PoolEventKind
	At   time.Time
}

type PoolStats struct {
	ChanInbox  int
	ChanEvents int

	TotalReceived uint64
	TotalSent     uint64

	PeerCount int
	PeerStats []PeerStats
}

type PeerStats struct {
	ID         string
	Worker     WorkerStats
	Connection transport.ConnectionStats
}

type PoolPlugin struct {
	Inbox        chan<- types.InboxMessage
	Events       chan<- PoolEvent
	InboxCounter *atomic.Uint64
	OnExit       OnExitFunction
	Handler      slog.Handler
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
	inbox  chan types.InboxMessage
	events chan PoolEvent

	inboxCounter  *atomic.Uint64
	outgoingCount *atomic.Uint64

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
	if handler != nil && config.LoggingEnabled {
		logger = logging.NewInboundPoolLogger(
			logging.WrapOrDefault(config.LogLevel, handler), id)
	}

	return &Pool{
		ctx:           pctx,
		cancel:        cancel,
		id:            id,
		peers:         make(map[string]*Peer),
		inbox:         make(chan types.InboxMessage, config.InboxBufferSize),
		events:        make(chan PoolEvent, config.EventsBufferSize),
		inboxCounter:  &atomic.Uint64{},
		outgoingCount: &atomic.Uint64{},
		config:        config,
		handler:       handler,
		logger:        logger,
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

func (p *Pool) Inbox() <-chan types.InboxMessage {
	return p.inbox
}

func (p *Pool) Events() <-chan PoolEvent {
	return p.events
}

func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	count := len(p.peers)
	peerStats := make([]PeerStats, 0, count)
	for id, peer := range p.peers {
		peerStats = append(peerStats, PeerStats{
			ID:         id,
			Worker:     peer.worker.Stats(),
			Connection: peer.conn.Stats(),
		})
	}

	return PoolStats{
		ChanInbox:  len(p.inbox),
		ChanEvents: len(p.events),

		TotalReceived: p.inboxCounter.Load(),
		TotalSent:     p.outgoingCount.Load(),

		PeerCount: len(p.peers),
		PeerStats: peerStats,
	}
}

func (p *Pool) PeerStats(id string) (PeerStats, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peer, exists := p.peers[id]
	if !exists {
		return PeerStats{}, ErrPeerNotFound
	}

	return PeerStats{
		ID:         id,
		Worker:     peer.worker.Stats(),
		Connection: peer.conn.Stats(),
	}, nil
}

func (p *Pool) Close() {
	if p.logger != nil {
		p.logger.Debug("closing")
	}

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

		if p.logger != nil {
			p.logger.Info("closed")
		}
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

		if p.logger != nil {
			p.logger.Info("removed peer", "peer", id)
		}

	} else {
		return ErrPeerNotFound
	}

	return p.addLocked(id, socket)
}

func (p *Pool) Remove(id string) error {
	if p.logger != nil {
		p.logger.Debug("removing peer", "peer", id)
	}

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

	if p.logger != nil {
		p.logger.Info("removed peer", "peer", id)
	}

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

	err := peer.worker.Send(data)
	if err != nil {
		return err
	}

	p.outgoingCount.Add(1)
	return nil
}

// addLocked constructs and registers a peer. Caller must hold p.mu write lock.
func (p *Pool) addLocked(id string, socket types.Socket) error {
	var logger *slog.Logger
	if p.handler != nil && p.config.ConnectionConfig.LoggingEnabled {
		logger = logging.NewConnectionLogger(
			logging.WrapOrDefault(p.config.ConnectionConfig.LogLevel, p.handler), p.id, id)
	}

	conn, err := transport.NewConnectionFromSocket(
		socket, p.config.ConnectionConfig, logger)
	if err != nil {
		return err
	}

	wctx, cancel := context.WithCancel(p.ctx)
	if p.handler != nil && p.config.WorkerConfig.LoggingEnabled {
		logger = logging.NewInboundWorkerLogger(
			logging.WrapOrDefault(p.config.WorkerConfig.LogLevel, p.handler), p.id, id)
	}

	// The worker factory must be non-blocking to avoid deadlocks
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
			case p.events <- PoolEvent{ID: id, Kind: workerToPoolEvent[kind], At: time.Now()}:
			case <-p.ctx.Done():
				return
			}
		})
	}

	pool := PoolPlugin{
		Inbox:        p.inbox,
		Events:       p.events,
		InboxCounter: p.inboxCounter,
		OnExit:       onExit,
		Handler:      p.handler,
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

	if p.logger != nil {
		p.logger.Info("added peer", "peer", id)
	}

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
