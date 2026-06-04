package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/types"
	"git.wisehodl.dev/jay/go-mana-component"
	"github.com/gorilla/websocket"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateClosed
)

func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

type ConnectionStats struct {
	ChanIncoming int
	ChanErrors   int

	TotalReceived   uint64
	TotalSent       uint64
	TotalHeartbeats uint64
}

// ----------------------------------------------------------------------------
// Connection
// ----------------------------------------------------------------------------

// ---------------------------/
// Constructors
// -------------------------/

type Connection struct {
	url    *url.URL
	dialer types.Dialer
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

	state ConnectionState

	wg          sync.WaitGroup
	closed      bool
	mu          sync.RWMutex
	writeMu     sync.Mutex
	doneOnce    sync.Once
	cleanupOnce sync.Once
}

func NewConnection(ctx context.Context, urlStr string, config *ConnectionConfig, handler slog.Handler) (*Connection, error) {
	if config == nil {
		config = GetDefaultConnectionConfig()
	}

	if err := ValidateConnectionConfig(config); err != nil {
		return nil, err
	}

	url, err := ParseURL(urlStr)
	if err != nil {
		return nil, err
	}

	if component.FromContext(ctx) == nil {
		ctx = component.MustNew(ctx, "honeybee", "connection")
	} else {
		ctx = component.MustExtend(ctx, "connection")
	}

	// Clone config to ensure full ownership of all fields.
	cc := config.Clone()
	if cc.Dialer == nil {
		cc.Dialer = NewDialer()
	}

	conn := &Connection{
		url:            url,
		dialer:         cc.Dialer,
		socket:         nil,
		config:         cc,
		incoming:       make(chan []byte, cc.IncomingBufferSize),
		heartbeat:      make(chan struct{}, 1),
		errors:         make(chan error, cc.ErrorsBufferSize),
		incomingCount:  &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		heartbeatCount: &atomic.Uint64{},
		state:          StateDisconnected,
		done:           make(chan struct{}),
	}

	if handler != nil {
		comp := component.FromContext(ctx)
		conn.logger = slog.New(handler).With(slog.Any("component", comp))
	}

	return conn, nil
}

func NewConnectionFromSocket(
	ctx context.Context, socket types.Socket, config *ConnectionConfig, handler slog.Handler,
) (*Connection, error) {
	if socket == nil {
		return nil, NewConnectionError(ErrNilSocket)
	}

	if config == nil {
		config = GetDefaultConnectionConfig()
	}

	if err := ValidateConnectionConfig(config); err != nil {
		return nil, err
	}

	if component.FromContext(ctx) == nil {
		ctx = component.MustNew(ctx, "honeybee", "connection")
	} else {
		ctx = component.MustExtend(ctx, "connection")
	}

	// Clone config to ensure full ownership of all fields.
	cc := config.Clone()

	conn := &Connection{
		url:            nil,
		dialer:         nil,
		socket:         socket,
		config:         cc,
		incoming:       make(chan []byte, cc.IncomingBufferSize),
		heartbeat:      make(chan struct{}, 1),
		errors:         make(chan error, cc.ErrorsBufferSize),
		incomingCount:  &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		heartbeatCount: &atomic.Uint64{},
		state:          StateConnected,
		done:           make(chan struct{}),
	}

	if handler != nil {
		comp := component.FromContext(ctx)
		conn.logger = slog.New(handler).With(slog.Any("component", comp))
	}

	// initialize
	if config.CloseHandler != nil {
		socket.SetCloseHandler(config.CloseHandler)
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

func (c *Connection) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.socket != nil {
		return NewConnectionError(ErrSocketExists)
	}

	if c.closed {
		return NewConnectionError(ErrConnectionClosed)
	}

	// begin connecting
	if c.logger != nil {
		c.logger.Debug("connecting")
	}

	c.state = StateConnecting

	// obtain socket
	retryMgr := NewRetryManager(c.config.Retry)
	socket, _, err := AcquireSocket(
		ctx, retryMgr, c.dialer, c.url.String(), c.config.RequestHeader, c.logger, nil)

	if err != nil {
		// socket acquisition failed
		c.state = StateDisconnected
		if c.logger != nil {
			c.logger.Warn("connection failed", "error", err)
		}
		return NewConnectionError(err)
	}

	// got socket
	c.socket = socket

	// initialize
	if c.config.CloseHandler != nil {
		c.socket.SetCloseHandler(c.config.CloseHandler)
	}

	c.setupPongHandler()

	if c.config.PingInterval > 0 {
		c.wg.Go(c.startPinger)
	}

	c.wg.Go(c.startReader)

	// connected
	c.state = StateConnected

	if c.logger != nil {
		c.logger.Debug("connected")
	}

	return nil
}

func (c *Connection) Send(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return NewConnectionError(ErrConnectionClosed)
	}

	// setup
	if c.config.WriteTimeout > 0 {
		if err := c.socket.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout)); err != nil {
			if c.logger != nil {
				c.logger.Error("write deadline error", "error", err)
			}
			return NewConnectionError(fmt.Errorf("%w: %w", ErrFailedWriteDeadline, err))
		}
	}

	// send
	err := c.socket.WriteMessage(websocket.TextMessage, data)

	if err != nil {
		if c.logger != nil {
			c.logger.Error("write error", "error", err)
		}
		return NewConnectionError(fmt.Errorf("%w: %w", ErrWriteFailed, err))
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

func (c *Connection) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
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

		classifiedError = fmt.Errorf("%w: %w", ErrReadError, err)
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
			deadline := time.Now().Add(c.config.WriteTimeout)
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
	c.state = StateClosed
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
	c.state = StateClosed
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
