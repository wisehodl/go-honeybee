package honeybee

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

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
// Errors
// ----------------------------------------------------------------------------

var (
	ErrPoolClosed   = errors.New("pool is closed")
	ErrPeerNotFound = errors.New("peer not found")
	ErrPeerExists   = errors.New("peer already exists")
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type PoolEventKind string

const (
	EventConnected    PoolEventKind = "connected"
	EventDisconnected PoolEventKind = "disconnected"
	EventDialFailed   PoolEventKind = "dial_failed"
	EventRetired      PoolEventKind = "retired"
)

type PoolEvent struct {
	ID   string
	Kind PoolEventKind
	At   time.Time
	Err  error
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
	Inbox        chan<- types.InboxMessage
	Events       chan<- PoolEvent
	InboxCounter *atomic.Uint64
	Retire       func(error)
}

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type PoolConfig struct {
	InboxBufferSize  int
	EventsBufferSize int
	WorkerConfig     WorkerConfig
	RequestHeader    http.Header
}

type PoolOption func(*PoolConfig)

func NewPoolConfig(opts ...PoolOption) (*PoolConfig, error) {
	workerCfg, _ := NewWorkerConfig()
	defaultHeader := http.Header{}
	defaultHeader.Set("User-Agent", "honeybee/0.1.0")
	cfg := &PoolConfig{
		InboxBufferSize:  256,
		EventsBufferSize: 10,
		WorkerConfig:     *workerCfg,
		RequestHeader:    defaultHeader,
	}
	for _, o := range opts {
		o(cfg)
	}
	if err := ValidatePoolConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func WithInboxBufferSize(value int) PoolOption {
	return func(c *PoolConfig) {
		c.InboxBufferSize = value
	}
}

func WithEventsBufferSize(value int) PoolOption {
	return func(c *PoolConfig) {
		c.EventsBufferSize = value
	}
}

func WithWorkerConfig(wc WorkerConfig) PoolOption {
	return func(c *PoolConfig) {
		c.WorkerConfig = wc
	}
}

func WithRequestHeader(h http.Header) PoolOption {
	return func(c *PoolConfig) {
		if h == nil {
			c.RequestHeader = nil
		} else {
			c.RequestHeader = h.Clone()
		}
	}
}

func ValidatePoolConfig(c *PoolConfig) error {
	if c.InboxBufferSize < 1 {
		return fmt.Errorf("invalid inbox buffer size: %d", c.InboxBufferSize)
	}
	if c.EventsBufferSize < 1 {
		return fmt.Errorf("invalid events buffer size: %d", c.EventsBufferSize)
	}
	if err := ValidateWorkerConfig(&c.WorkerConfig); err != nil {
		return err
	}
	return nil
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
		config, _ = NewPoolConfig()
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

// ConnectOption configures a single Connect call.
type ConnectOption func(*connectOptions)

type connectOptions struct {
	dialFn func(context.Context) (types.Socket, error)
}

// WithDialFunc returns a ConnectOption that overrides the dial function for
// this connection.
func WithDialFunc(f func(context.Context) (types.Socket, error)) ConnectOption {
	return func(o *connectOptions) {
		o.dialFn = f
	}
}

func (p *Pool) Connect(id string, opts ...ConnectOption) error {
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
		return ErrPoolClosed
	}

	if _, exists := p.peers[id]; exists {
		return ErrPeerExists
	}

	o := &connectOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var hdr http.Header
	if p.config.RequestHeader != nil {
		hdr = p.config.RequestHeader.Clone()
	}
	dialFn := func(ctx context.Context) (types.Socket, error) {
		socket, _, err := p.dialer.DialContext(ctx, id, hdr)
		return socket, err
	}

	if o.dialFn != nil {
		dialFn = o.dialFn
	}

	wc := p.config.WorkerConfig
	worker, err := NewWorker(p.ctx, id, dialFn, &wc, p.handler)
	if err != nil {
		return err
	}

	pool := PoolPlugin{
		Inbox:        p.inbox,
		Events:       p.events,
		InboxCounter: p.inboxCounter,
		Retire: func(err error) {
			p.mu.Lock()
			if p.closed {
				return
			}
			delete(p.peers, id)
			p.mu.Unlock()
			p.events <- PoolEvent{ID: id, Kind: EventRetired, Err: err, At: time.Now()}
		},
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
		return ErrPoolClosed
	}

	peer, exists := p.peers[id]
	if !exists {
		return ErrPeerNotFound
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
		return ErrPoolClosed
	}

	peer, exists := p.peers[id]
	if !exists {
		return ErrPeerNotFound
	}

	err = peer.worker.Send(data)
	if err != nil {
		return err
	}

	p.outgoingCount.Add(1)
	return nil
}
