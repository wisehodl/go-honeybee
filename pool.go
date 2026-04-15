package honeybee

import (
	"git.wisehodl.dev/jay/go-honeybee/errors"
	"log/slog"
	"sync"
	"time"
)

type poolConnection struct {
	inner *Connection
	stop  chan struct{}
}

type InboundMessage struct {
	URL        string
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
	URL  string
	Kind PoolEventKind
}

type Pool struct {
	mu          sync.RWMutex
	wg          sync.WaitGroup
	closed      bool
	connections map[string]*poolConnection
	inbound     chan InboundMessage
	events      chan PoolEvent
	errors      chan error
	done        chan struct{}
	config      *Config
	dialer      Dialer
	logger      *slog.Logger
}

func NewPool(config *Config, logger *slog.Logger) (*Pool, error) {
	if config == nil {
		config = GetDefaultConfig()
	}

	if err := ValidateConfig(config); err != nil {
		return nil, err
	}

	pool := &Pool{
		connections: make(map[string]*poolConnection),
		inbound:     make(chan InboundMessage, 256),
		events:      make(chan PoolEvent, 10),
		errors:      make(chan error, 10),
		done:        make(chan struct{}),
		config:      config,
		dialer:      NewDialer(),
		logger:      logger,
	}

	return pool, nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	close(p.done)

	connections := p.connections
	p.connections = make(map[string]*poolConnection)

	p.mu.Unlock()

	for _, conn := range connections {
		conn.inner.Close()
	}

	go func() {
		p.wg.Wait()
		close(p.inbound)
		close(p.events)
		close(p.errors)
	}()
}

func (p *Pool) Add(rawURL string) error {
	url, err := NormalizeURL(rawURL)
	if err != nil {
		return err
	}

	// Check for existing connection in pool
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.NewPoolError("pool is closed")
	}
	_, exists := p.connections[url]
	p.mu.Unlock()

	if exists {
		return errors.NewPoolError("connection already exists")
	}

	// Create new connection
	var logger *slog.Logger
	if p.logger != nil {
		logger = p.logger.With("url", url)
	}
	conn, err := NewConnection(url, p.config, logger)
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
	if _, exists := p.connections[url]; exists {
		// Another process connected to this url while this one was connecting
		// Discard this connection and retain the existing one
		p.mu.Unlock()
		conn.Close()
		return errors.NewPoolError("connection already exists")
	}
	p.connections[url] = &poolConnection{inner: conn, stop: stop}
	p.mu.Unlock()

	// TODO: start this connection's incoming message forwarder

	select {
	case p.events <- PoolEvent{URL: url, Kind: EventConnected}:
	case <-p.done:
		return nil
	}

	return nil
}
