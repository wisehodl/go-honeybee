package outbound

import (
	"context"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"time"
)

// Types

type PoolEventKind string

const (
	EventConnected    PoolEventKind = "connected"
	EventDisconnected PoolEventKind = "disconnected"
)

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
	Inbox            chan<- InboxMessage
	Events           chan<- PoolEvent
	Errors           chan<- error
	Logger           *slog.Logger
	Dialer           types.Dialer
	ConnectionConfig *transport.ConnectionConfig
}

// Pool

type Peer struct {
	id     string
	worker Worker
}

type Pool struct {
	ctx    context.Context
	cancel context.CancelFunc

	peers  map[string]*Peer
	inbox  chan InboxMessage
	events chan PoolEvent
	errors chan error

	dialer types.Dialer
	config *PoolConfig
	logger *slog.Logger

	mu     sync.RWMutex
	wg     sync.WaitGroup
	closed bool
}

func NewPool(ctx context.Context, config *PoolConfig, logger *slog.Logger,
) (*Pool, error) {
	if config == nil {
		config = GetDefaultPoolConfig()
	}

	// If a custom factory is supplied, config.WorkerConfig is not used.
	// The factory function should be non-blocking or else Connect() may cause
	// deadlocks.
	if config.WorkerFactory == nil {
		config.WorkerFactory = func(
			ctx context.Context, id string) (Worker, error) {
			return NewWorker(ctx, id, config.WorkerConfig)
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
		errors: make(chan error, 10),
		dialer: transport.NewDialer(),
		config: config,
		logger: logger,
	}, nil
}

func (p *Pool) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]string, 0, len(p.peers))
	for i, _ := range p.peers {
		ids = append(ids, i)
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

func (p *Pool) SetDialer(d types.Dialer) {
	if d == nil {
		panic("dialer cannot be nil")
	}
	p.dialer = d
}

func (p *Pool) Close() {
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
		close(p.errors)
	}()
}

func (p *Pool) Connect(id string) error {
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
	worker, err := p.config.WorkerFactory(p.ctx, id)
	if err != nil {
		return err
	}

	var logger *slog.Logger
	if p.logger != nil {
		logger = p.logger.With("id", id)
	}

	pool := PoolPlugin{
		Inbox:            p.inbox,
		Events:           p.events,
		Errors:           p.errors,
		Logger:           logger,
		Dialer:           p.dialer,
		ConnectionConfig: p.config.ConnectionConfig,
	}

	p.wg.Add(1)
	go func() {
		worker.Start(pool)
		p.wg.Done()
	}()

	p.peers[id] = &Peer{id: id, worker: worker}

	return nil
}

func (p *Pool) Remove(id string) error {
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

	return peer.worker.Send(data)
}
