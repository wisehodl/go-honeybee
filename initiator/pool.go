package initiator

import (
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
	"log/slog"
	"sync"
	"time"
)

// Types

type peer struct {
	conn *transport.Connection
	stop chan struct{}
}

type InboxMessage struct {
	ID         string
	Data       []byte
	ReceivedAt time.Time
}

type PoolEventKind int

const (
	EventConnected PoolEventKind = iota
	EventDisconnected
)

func (s PoolEventKind) String() string {
	switch s {
	case EventConnected:
		return "connected"
	case EventDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

type PoolEvent struct {
	ID   string
	Kind PoolEventKind
}

// Pool

type Pool struct {
	peers  map[string]*peer
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

	if err := ValidatePoolConfig(config); err != nil {
		return nil, err
	}

	p := &Pool{
		peers:  make(map[string]*peer),
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

func (p *Pool) Peers() map[string]*peer {
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

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	close(p.done)

	peers := p.peers
	p.peers = make(map[string]*peer)

	p.mu.Unlock()

	for _, conn := range peers {
		conn.conn.Close()
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
	if p.closed {
		p.mu.Unlock()
		return NewPoolError("pool is closed")
	}
	_, exists := p.peers[id]
	p.mu.Unlock()

	if exists {
		return NewPoolError("connection already exists")
	}

	// Create new connection
	var logger *slog.Logger
	if p.logger != nil {
		logger = p.logger.With("id", id)
	}
	conn, err := transport.NewConnection(id, p.config.ConnectionConfig, logger)
	if err != nil {
		return err
	}
	conn.SetDialer(p.dialer)

	// Attempt to connect
	if err := conn.Connect(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		// The pool closed while this connection was established.
		p.mu.Unlock()
		conn.Close()
		return NewPoolError("pool is closed")
	}

	// Add connection to pool
	stop := make(chan struct{})
	if _, exists := p.peers[id]; exists {
		// Another process connected to this url while this one was connecting
		// Discard this connection and retain the existing one
		p.mu.Unlock()
		conn.Close()
		return NewPoolError("connection already exists")
	}
	p.peers[id] = &peer{conn: conn, stop: stop}
	p.mu.Unlock()

	// TODO: start this connection's incoming message forwarder

	select {
	case p.events <- PoolEvent{ID: id, Kind: EventConnected}:
	case <-p.done:
		return nil
	}

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
	peer.conn.Close()

	select {
	case p.events <- PoolEvent{ID: id, Kind: EventDisconnected}:
	case <-p.done:
		return nil
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
		return NewPoolError("pool is closed")
	}

	peer, exists := p.peers[id]
	if !exists {
		return NewPoolError("connection not found")
	}

	return peer.conn.Send(data)
}
