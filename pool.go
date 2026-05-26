package honeybee

import (
	"context"
	"log/slog"

	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"git.wisehodl.dev/jay/go-mana-component"
	"sync"
	"sync/atomic"
	"time"
)

// Re-exported types for consumer convenience

type InboxMessage = types.InboxMessage
type Dialer = types.Dialer

var NormalizeURL = transport.NormalizeURL

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type PoolEventKind string

const (
	EventConnected    PoolEventKind = "connected"
	EventDisconnected PoolEventKind = "disconnected"
)

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
	ID     string
	Worker WorkerStats
}

type PoolPlugin struct {
	Inbox            chan<- types.InboxMessage
	Events           chan<- PoolEvent
	InboxCounter     *atomic.Uint64
	Dialer           types.Dialer
	ConnectionConfig transport.ConnectionConfig
}

// ----------------------------------------------------------------------------
// Pool
// ----------------------------------------------------------------------------

type Peer struct {
	id     string
	worker Worker
}

type Pool struct {
	peers  map[string]*Peer
	inbox  chan types.InboxMessage
	events chan PoolEvent
	closed bool

	dialer  types.Dialer
	config  *PoolConfig
	handler slog.Handler
	logger  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	wg     sync.WaitGroup

	inboxCounter  *atomic.Uint64
	outgoingCount *atomic.Uint64
}

func NewPool(ctx context.Context, config *PoolConfig, handler slog.Handler,
) (*Pool, error) {
	if config == nil {
		config = GetDefaultPoolConfig()
	}

	// If a custom factory is supplied, config.WorkerConfig is not used.
	// The factory function should be non-blocking or else Connect() may cause
	// deadlocks.
	if config.WorkerFactory == nil {
		config.WorkerFactory = func(
			ctx context.Context, id string, handler slog.Handler) (Worker, error) {
			return NewWorker(ctx, id, config.WorkerConfig, handler)
		}
	}

	if err := ValidatePoolConfig(config); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(component.MustNew(ctx, "honeybee", "pool"))

	var logger *slog.Logger
	if handler != nil {
		c := component.FromContext(ctx)
		logger = slog.New(handler).With(slog.Any("component", c))
	}

	return &Pool{
		peers:  make(map[string]*Peer),
		inbox:  make(chan types.InboxMessage, config.InboxBufferSize),
		events: make(chan PoolEvent, config.EventsBufferSize),

		dialer:  transport.NewDialer(),
		config:  config,
		handler: handler,
		logger:  logger,

		ctx:    ctx,
		cancel: cancel,

		inboxCounter:  &atomic.Uint64{},
		outgoingCount: &atomic.Uint64{},
	}, nil
}

func (p *Pool) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]string, 0, len(p.peers))
	for i := range p.peers {
		ids = append(ids, i)
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
			ID:     id,
			Worker: peer.worker.Stats(),
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
		ID:     id,
		Worker: peer.worker.Stats(),
	}, nil
}

func (p *Pool) SetDialer(d types.Dialer) {
	if d == nil {
		panic("dialer cannot be nil")
	}
	p.dialer = d
}

func (p *Pool) Close() {
	if p.logger != nil {
		p.logger.Info("closing")
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	p.cancel() // closes all workers

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

func (p *Pool) Connect(id string) error {
	if p.logger != nil {
		p.logger.Info("connecting", "peer", id)
	}

	id, err := transport.NormalizeURL(id)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return NewPoolError(ErrPoolClosed)
	}

	if _, exists := p.peers[id]; exists {
		return NewPoolError(ErrPeerExists)
	}

	// The worker factory must be non-blocking to avoid deadlocks
	worker, err := p.config.WorkerFactory(p.ctx, id, p.handler)
	if err != nil {
		return err
	}

	pool := PoolPlugin{
		Inbox:            p.inbox,
		Events:           p.events,
		InboxCounter:     p.inboxCounter,
		Dialer:           p.dialer,
		ConnectionConfig: p.config.ConnectionConfig,
		// ConnectionConfig is assigned by value — each worker gets its own copy
	}

	p.wg.Go(func() {
		worker.Start(pool)
	})

	p.peers[id] = &Peer{id: id, worker: worker}

	if p.logger != nil {
		p.logger.Debug("registered peer", "peer", id)
	}

	return nil
}

func (p *Pool) Remove(id string) error {
	if p.logger != nil {
		p.logger.Info("disconnecting", "peer", id)
	}

	id, err := transport.NormalizeURL(id)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return NewPoolError(ErrPoolClosed)
	}

	peer, exists := p.peers[id]
	if !exists {
		return NewPoolError(ErrPeerNotFound)
	}
	delete(p.peers, id)

	peer.worker.Stop()

	if p.logger != nil {
		p.logger.Debug("disconnected from peer", "peer", id)
	}

	return nil
}

func (p *Pool) Send(id string, data []byte) error {
	id, err := transport.NormalizeURL(id)
	if err != nil {
		return err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return NewPoolError(ErrPoolClosed)
	}

	peer, exists := p.peers[id]
	if !exists {
		return NewPoolError(ErrPeerNotFound)
	}

	err = peer.worker.Send(data)
	if err != nil {
		return err
	}

	p.outgoingCount.Add(1)
	return nil
}
