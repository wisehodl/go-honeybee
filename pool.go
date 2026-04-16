package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"log/slog"
	"sync"
	"time"
)

// Types

type peer struct {
	conn *Connection
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

// Pool Implementation

type Pool interface {
	Send(id string, data []byte) error
	Inbox() <-chan InboxMessage
	Events() <-chan PoolEvent
	Errors() <-chan error
	Close()
}

// Base Struct

type pool struct {
	peers  map[string]*peer
	inbox  chan InboxMessage
	events chan PoolEvent
	errors chan error
	done   chan struct{}

	config *PoolConfig
	logger *slog.Logger

	mu     sync.RWMutex
	wg     sync.WaitGroup
	closed bool
}

func (p *pool) closeAll() {
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

func (p *pool) removePeer(id string) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.NewPoolError("pool is closed")
	}

	peer, exists := p.peers[id]
	if !exists {
		p.mu.Unlock()
		return errors.NewPoolError("connection not found")
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

// Outbound Pool

type OutboundPool struct {
	*pool
	dialer Dialer
}

func NewOutboundPool(config *PoolConfig, logger *slog.Logger) (*OutboundPool, error) {
	if config == nil {
		config = GetDefaultPoolConfig()
	}

	if err := validatePoolConfig(config); err != nil {
		return nil, err
	}

	p := &OutboundPool{
		pool: &pool{
			peers:  make(map[string]*peer),
			inbox:  make(chan InboxMessage, 256),
			events: make(chan PoolEvent, 10),
			errors: make(chan error, 10),
			done:   make(chan struct{}),
			config: config,
			logger: logger,
		},
		dialer: NewDialer(),
	}

	return p, nil
}

func (p *OutboundPool) Peers() map[string]*peer {
	return p.peers
}

func (p *OutboundPool) Inbox() chan InboxMessage {
	return p.inbox
}

func (p *OutboundPool) Events() chan PoolEvent {
	return p.events
}

func (p *OutboundPool) Errors() chan error {
	return p.errors
}

func (p *OutboundPool) Close() {
	p.closeAll()
}

func (p *OutboundPool) Connect(url string) error {
	url, err := NormalizeURL(url)
	if err != nil {
		return err
	}

	// Check for existing connection in pool
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.NewPoolError("pool is closed")
	}
	_, exists := p.peers[url]
	p.mu.Unlock()

	if exists {
		return errors.NewPoolError("connection already exists")
	}

	// Create new connection
	var logger *slog.Logger
	if p.logger != nil {
		logger = p.logger.With("url", url)
	}
	conn, err := NewConnection(url, p.config.ConnectionConfig, logger)
	if err != nil {
		return err
	}
	conn.dialer = p.dialer

	// Attempt to connect
	if err := conn.Connect(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		// The pool closed while this connection was established.
		p.mu.Unlock()
		conn.Close()
		return errors.NewPoolError("pool is closed")
	}

	// Add connection to pool
	stop := make(chan struct{})
	if _, exists := p.peers[url]; exists {
		// Another process connected to this url while this one was connecting
		// Discard this connection and retain the existing one
		p.mu.Unlock()
		conn.Close()
		return errors.NewPoolError("connection already exists")
	}
	p.peers[url] = &peer{conn: conn, stop: stop}
	p.mu.Unlock()

	// TODO: start this connection's incoming message forwarder

	select {
	case p.events <- PoolEvent{ID: url, Kind: EventConnected}:
	case <-p.done:
		return nil
	}

	return nil
}

func (p *OutboundPool) Remove(url string) error {
	url, err := NormalizeURL(url)
	if err != nil {
		return err
	}

	return p.removePeer(url)
}

// Inbound Pool

type InboundPool struct {
	*pool
	idGenerator func() string
}

func (p *InboundPool) Peers() map[string]*peer {
	return p.peers
}

func (p *InboundPool) Inbox() chan InboxMessage {
	return p.inbox
}

func (p *InboundPool) Events() chan PoolEvent {
	return p.events
}

func (p *InboundPool) Errors() chan error {
	return p.errors
}

func (p *InboundPool) Close() {
	p.closeAll()
}
