package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/types"
	"git.wisehodl.dev/jay/go-mana-component"
	"github.com/gorilla/websocket"
)

// ----------------------------------------------------------------------------
// Errors
// ----------------------------------------------------------------------------

var (
	ErrConnectionClosed     = errors.New("connection closed")
	ErrWriteFailed          = errors.New("write failed")
	ErrNilSocket            = errors.New("socket cannot be nil")
	ErrFailedWriteDeadline  = errors.New("failed to set write deadline")
	ErrPeerClosedClean      = errors.New("peer closed connection cleanly")
	ErrPeerClosedUnexpected = errors.New("peer closed connection unexpectedly")
	ErrReadError            = errors.New("read error")
	ErrReadLimit            = errors.New("read limit exceeded")
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type ConnectionStats struct {
	ChanIncoming int
	ChanErrors   int

	TotalReceived   uint64
	TotalSent       uint64
	TotalHeartbeats uint64
}

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type ConnectionConfig struct {
	WriteTimeout       time.Duration
	PingInterval       time.Duration
	IncomingBufferSize int
	ErrorsBufferSize   int
	ReadLimit          *int64
}

func NewConnectionConfig(opts ...ConnectionOption) (*ConnectionConfig, error) {
	cfg := &ConnectionConfig{
		WriteTimeout:       30 * time.Second,
		PingInterval:       20 * time.Second,
		IncomingBufferSize: 100,
		ErrorsBufferSize:   10,
		ReadLimit:          nil,
	}
	for _, o := range opts {
		o(cfg)
	}
	if err := ValidateConnectionConfig(*cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

type ConnectionOption func(*ConnectionConfig)

// When WriteTimeout is set to zero, read timeouts are disabled.
func WithWriteTimeout(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) {
		c.WriteTimeout = value
	}
}

// When PingInterval is set to zero, ping frames are disabled.
func WithPingInterval(value time.Duration) ConnectionOption {
	return func(c *ConnectionConfig) {
		c.PingInterval = value
	}
}

func WithIncomingBufferSize(value int) ConnectionOption {
	return func(c *ConnectionConfig) {
		c.IncomingBufferSize = value
	}
}

func WithErrorsBufferSize(value int) ConnectionOption {
	return func(c *ConnectionConfig) {
		c.ErrorsBufferSize = value
	}
}

// When ReadLimit is set to zero, reads are unlimited.
func WithReadLimit(value int64) ConnectionOption {
	return func(c *ConnectionConfig) {
		ptr := &value
		c.ReadLimit = ptr
	}
}

func ValidateConnectionConfig(c ConnectionConfig) error {
	if c.WriteTimeout < 0 {
		return fmt.Errorf("invalid write timeout: %v", c.WriteTimeout)
	}
	if c.PingInterval < 0 {
		return fmt.Errorf("invalid ping interval: %v", c.PingInterval)
	}
	if c.IncomingBufferSize < 1 {
		return fmt.Errorf("invalid incoming buffer size: %d",
			c.IncomingBufferSize)
	}
	if c.ErrorsBufferSize < 1 {
		return fmt.Errorf("invalid errors buffer size: %d",
			c.ErrorsBufferSize)
	}
	if c.ReadLimit != nil && *c.ReadLimit < 0 {
		return fmt.Errorf("invalid read limit: %d", *c.ReadLimit)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Connection
// ----------------------------------------------------------------------------

// ---------------------------/
// Constructors
// -------------------------/

type Connection struct {
	socket types.Socket
	config ConnectionConfig
	logger *slog.Logger

	incoming  chan []byte
	heartbeat chan struct{}
	errors    chan error
	done      chan struct{}

	incomingCount  *atomic.Uint64
	outgoingCount  *atomic.Uint64
	heartbeatCount *atomic.Uint64

	wg          sync.WaitGroup
	closed      bool
	mu          sync.RWMutex
	writeMu     sync.Mutex
	doneOnce    sync.Once
	cleanupOnce sync.Once
}

func NewConnection(
	ctx context.Context,
	socket types.Socket,
	config *ConnectionConfig,
	handler slog.Handler,
) (*Connection, error) {
	if socket == nil {
		return nil, ErrNilSocket
	}

	if config == nil {
		config, _ = NewConnectionConfig()
	}

	err := ValidateConnectionConfig(*config)
	if err != nil {
		return nil, err
	}

	if component.FromContext(ctx) == nil {
		ctx = component.MustNew(ctx, "honeybee", "connection")
	} else {
		ctx = component.MustExtend(ctx, "connection")
	}

	conn := &Connection{
		socket:         socket,
		config:         *config,
		incoming:       make(chan []byte, config.IncomingBufferSize),
		heartbeat:      make(chan struct{}, 1),
		errors:         make(chan error, config.ErrorsBufferSize),
		incomingCount:  &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		heartbeatCount: &atomic.Uint64{},
		done:           make(chan struct{}),
	}

	if config.ReadLimit != nil {
		conn.socket.SetReadLimit(*config.ReadLimit)
	}

	if handler != nil {
		comp := component.FromContext(ctx)
		conn.logger = slog.New(handler).With(slog.Any("component", comp))
	}

	conn.setupPongHandler()

	if conn.config.PingInterval > 0 {
		conn.wg.Go(conn.startPinger)
	}

	conn.wg.Go(conn.startReader)

	return conn, nil
}

// ---------------------------/
// Methods
// -------------------------/

func (c *Connection) Send(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return ErrConnectionClosed
	}

	// setup
	if c.config.WriteTimeout > 0 {
		if err := c.socket.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout)); err != nil {
			if c.logger != nil {
				c.logger.Error("write deadline error", "error", err)
			}
			return fmt.Errorf("%w: %w", ErrFailedWriteDeadline, err)
		}
	}

	// send
	err := c.socket.WriteMessage(websocket.TextMessage, data)

	if err != nil {
		if c.logger != nil {
			c.logger.Error("write error", "error", err)
		}
		return fmt.Errorf("%w: %w", ErrWriteFailed, err)
	}

	c.outgoingCount.Add(1)

	return nil
}

func (c *Connection) Incoming() <-chan []byte {
	return c.incoming
}

func (c *Connection) Heartbeat() <-chan struct{} {
	return c.heartbeat
}

func (c *Connection) Errors() <-chan error {
	return c.errors
}

func (c *Connection) Stats() ConnectionStats {
	return ConnectionStats{
		ChanIncoming:    len(c.incoming),
		ChanErrors:      len(c.errors),
		TotalReceived:   c.incomingCount.Load(),
		TotalSent:       c.outgoingCount.Load(),
		TotalHeartbeats: c.heartbeatCount.Load(),
	}
}

// ---------------------------/
// Reader loop
// -------------------------/

func (c *Connection) startReader() {
	defer c.shutdownInternal()

	for {
		select {
		case <-c.done:
			return
		default:
			messageType, data, err := c.socket.ReadMessage()
			if err != nil {
				select {
				case <-c.done:
				case c.errors <- c.classifyCloseError(err):
				}
				return
			}

			if messageType == websocket.TextMessage ||
				messageType == websocket.BinaryMessage {
				select {
				case <-c.done:
					return
				case c.incoming <- data:
					c.incomingCount.Add(1)
				}
			}
		}
	}
}

func (c *Connection) classifyCloseError(err error) error {
	var classifiedError error
	var closeErr *websocket.CloseError

	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway:
			if c.logger != nil {
				c.logger.Debug("connection closed by peer",
					"code", closeErr.Code,
					"text", closeErr.Text,
				)
			}
			classifiedError = fmt.Errorf("%w: %w", ErrPeerClosedClean, err)

		default:
			if c.logger != nil {
				c.logger.Warn("unexpected close",
					"code", closeErr.Code,
					"text", closeErr.Text,
				)
			}
			classifiedError = fmt.Errorf("%w: %w", ErrPeerClosedUnexpected, err)
		}

	} else {
		isLocalClose := false

		select {
		case <-c.done:
			isLocalClose = true
		default:
		}

		if c.logger != nil {
			if isLocalClose {
				c.logger.Debug("read loop terminated", "error", err)
			} else {
				c.logger.Error("read error", "error", err)
			}
		}

		if errors.Is(err, websocket.ErrReadLimit) {
			classifiedError = fmt.Errorf("%w: %w: %v",
				ErrReadError, ErrReadLimit, err)
		} else {
			classifiedError = fmt.Errorf("%w: %w", ErrReadError, err)
		}
	}

	return classifiedError
}

// ---------------------------/
// Heartbeat Handling
// -------------------------/

func (c *Connection) setupPongHandler() {
	c.socket.SetPongHandler(func(appData string) error {
		select {
		case c.heartbeat <- struct{}{}:
			c.heartbeatCount.Add(1)
		default:
		}
		return nil
	})
}

func (c *Connection) startPinger() {
	defer c.shutdownInternal()

	// Calculate 10% jitter window
	jitter := c.config.PingInterval / 10

	for {
		offset := time.Duration(rand.Int63n(int64(jitter*2))) - jitter
		next := c.config.PingInterval + offset
		timer := time.NewTimer(next)
		select {
		case <-c.done:
			timer.Stop()
			return
		case <-timer.C:
			var deadline time.Time
			if c.config.WriteTimeout > 0 {
				deadline = time.Now().Add(c.config.WriteTimeout)
			} else {
				deadline = time.Now().Add(15 * time.Second)
			}
			err := c.socket.WriteControl(websocket.PingMessage, nil, deadline)
			if err != nil {
				return
			}
		}
	}

}

// ---------------------------/
// Shutdown
// -------------------------/

func (c *Connection) Close() {
	c.shutdownExternal()
}

func (c *Connection) shutdownExternal() {
	// set closed
	c.mu.Lock()
	if c.closed {
		// idempotent shutdown
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	// perform shutdown
	c.shutdownInner()
	c.shutdownCleanup()
}

// shutdownInternal defers final cleanup to allow it to return.
// Otherwise, a deadlock occurs where startReader triggers a shutdown and
// must wait for itself to exit.
func (c *Connection) shutdownInternal() {
	// set closed
	c.mu.Lock()
	if c.closed {
		// idempotent shutdown
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	// perform shutdown
	c.shutdownInner()

	// defer cleanup to avoid deadlock
	go func() {
		c.shutdownCleanup()
	}()
}

func (c *Connection) shutdownInner() {
	c.doneOnce.Do(func() {
		close(c.done)
	})

	if c.logger != nil {
		c.logger.Debug("closing")
	}

	if c.socket != nil {
		// force unblock of any network operations immediately
		expired := time.Now().Add(-1 * time.Minute)
		c.socket.SetReadDeadline(expired)
		c.socket.SetWriteDeadline(expired)

		// close socket
		err := c.socket.Close()

		if err != nil && c.logger != nil {
			c.logger.Error("socket close failed", "error", err)
		}
	}
}

func (c *Connection) shutdownCleanup() {
	c.cleanupOnce.Do(func() {
		c.wg.Wait()

		close(c.incoming)
		close(c.errors)

		if c.logger != nil {
			c.logger.Debug("closed")
		}
	})
}
