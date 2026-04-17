package initiator

import (
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"time"
)

// Types

type Peer struct {
	id     string
	worker *Worker
	stop   chan struct{}
}

type WorkerContext struct {
	Inbox            chan<- InboxMessage
	Events           chan<- PoolEvent
	Errors           chan<- error
	PoolDone         <-chan struct{}
	Logger           *slog.Logger
	Dialer           types.Dialer
	ConnectionConfig *transport.ConnectionConfig
}

type InboxMessage struct {
	ID         string
	Data       []byte
	ReceivedAt time.Time
}

type PoolEventKind string

const (
	EventConnected    PoolEventKind = "connected"
	EventDisconnected               = "disconnected"
)

type PoolEvent struct {
	ID   string
	Kind PoolEventKind
}

// Pool

type Pool struct {
	peers  map[string]*Peer
	inbox  chan InboxMessage
	events chan PoolEvent
	errors chan error
	done   chan struct{}

	dialer types.Dialer
	config *PoolConfig
	logger *slog.Logger

	mu     sync.RWMutex
	wg     sync.WaitGroup
	closed bool
}

func NewPool(config *PoolConfig, logger *slog.Logger) (*Pool, error) {
	if config == nil {
		config = GetDefaultPoolConfig()
	}

	// if a custom factory is supplied, config.WorkerConfig is not used
	if config.WorkerFactory == nil {
		config.WorkerFactory = func(id string, stop <-chan struct{}) (*Worker, error) {
			return NewWorker(id, stop, config.WorkerConfig)
		}
	}

	if err := ValidatePoolConfig(config); err != nil {
		return nil, err
	}

	p := &Pool{
		peers:  make(map[string]*Peer),
		inbox:  make(chan InboxMessage, 256),
		events: make(chan PoolEvent, 10),
		errors: make(chan error, 10),
		done:   make(chan struct{}),
		dialer: transport.NewDialer(),
		config: config,
		logger: logger,
	}

	return p, nil
}

func (p *Pool) Peers() map[string]*Peer {
	return p.peers
}

func (p *Pool) Inbox() chan InboxMessage {
	return p.inbox
}

func (p *Pool) Events() chan PoolEvent {
	return p.events
}

func (p *Pool) Errors() chan error {
	return p.errors
}

func (p *Pool) SetDialer(d types.Dialer) {
	p.dialer = d
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	close(p.done)

	peers := p.peers
	p.peers = make(map[string]*Peer)

	p.mu.Unlock()

	for _, p := range peers {
		close(p.stop)
	}

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

	// Check for existing connection in pool
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return NewPoolError("pool is closed")
	}
	_, exists := p.peers[id]

	if exists {
		return NewPoolError("connection already exists")
	}

	// Create new worker
	stop := make(chan struct{})

	worker, err := p.config.WorkerFactory(id, stop)
	if err != nil {
		close(stop)
		return err
	}

	var logger *slog.Logger
	if p.logger != nil {
		logger = p.logger.With("id", id)
	}
	ctx := WorkerContext{
		Inbox:            p.inbox,
		Events:           p.events,
		Errors:           p.errors,
		PoolDone:         p.done,
		Logger:           logger,
		Dialer:           p.dialer,
		ConnectionConfig: p.config.ConnectionConfig,
	}

	p.wg.Add(1)
	go worker.Start(ctx, &p.wg)

	p.peers[id] = &Peer{id: id, worker: worker, stop: stop}

	return nil
}

func (p *Pool) Remove(id string) error {
	id, err := transport.NormalizeURL(id)
	if err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return NewPoolError("pool is closed")
	}

	peer, exists := p.peers[id]
	if !exists {
		p.mu.Unlock()
		return NewPoolError("connection not found")
	}
	delete(p.peers, id)
	p.mu.Unlock()

	close(peer.stop)

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
		return NewPoolError("pool is closed")
	}

	peer, exists := p.peers[id]
	if !exists {
		return NewPoolError("connection not found")
	}

	return peer.worker.Send(data)
}
