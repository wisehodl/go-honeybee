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
	"github.com/gorilla/websocket"
)

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

type Connection struct {
	url    *url.URL
	dialer types.Dialer
	socket types.Socket
	config *ConnectionConfig
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

func NewConnection(urlStr string, config *ConnectionConfig, logger *slog.Logger) (*Connection, error) {
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

	conn := &Connection{
		url:            url,
		dialer:         NewDialer(),
		socket:         nil,
		config:         config,
		logger:         logger,
		incoming:       make(chan []byte, config.IncomingBufferSize),
		heartbeat:      make(chan struct{}, 1),
		errors:         make(chan error, config.ErrorsBufferSize),
		incomingCount:  &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		heartbeatCount: &atomic.Uint64{},
		state:          StateDisconnected,
		done:           make(chan struct{}),
	}

	return conn, nil
}

func NewConnectionFromSocket(
	socket types.Socket, config *ConnectionConfig, logger *slog.Logger,
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

	conn := &Connection{
		url:            nil,
		dialer:         nil,
		socket:         socket,
		config:         config,
		logger:         logger,
		incoming:       make(chan []byte, config.IncomingBufferSize),
		heartbeat:      make(chan struct{}, 1),
		errors:         make(chan error, config.ErrorsBufferSize),
		incomingCount:  &atomic.Uint64{},
		outgoingCount:  &atomic.Uint64{},
		heartbeatCount: &atomic.Uint64{},
		state:          StateConnected,
		done:           make(chan struct{}),
	}

	if config.CloseHandler != nil {
		socket.SetCloseHandler(config.CloseHandler)
	}

	conn.setupPongHandler()
	conn.startPinger()
	conn.startReader()

	return conn, nil
}

func (c *Connection) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.socket != nil {
		return NewConnectionError(ErrSocketExists)
	}

	if c.closed {
		return NewConnectionError(ErrConnectionClosed)
	}

	if c.logger != nil {
		c.logger.Debug("connecting")
	}

	c.state = StateConnecting

	retryMgr := NewRetryManager(c.config.Retry)
	socket, _, err := AcquireSocket(
		ctx, retryMgr, c.dialer, c.url.String(), c.logger)

	if err != nil {
		c.state = StateDisconnected
		if c.logger != nil {
			c.logger.Error("connection failed", "error", err)
		}
		return NewConnectionError(err)
	}

	c.socket = socket
	c.state = StateConnected

	if c.config.CloseHandler != nil {
		c.socket.SetCloseHandler(c.config.CloseHandler)
	}

	if c.logger != nil {
		c.logger.Info("connected")
	}

	c.setupPongHandler()
	c.startPinger()
	c.startReader()

	return nil
}

func (c *Connection) Close() {
	c.shutdownExternal()
}

func (c *Connection) shutdownExternal() {
	err := c.shutdownSetClosed(true)
	if err != nil {
		return
	}

	c.shutdownInner()
	c.shutdownCleanup()
}

func (c *Connection) shutdownInternal() {
	err := c.shutdownSetClosed(false)
	if err != nil {
		return
	}

	c.shutdownInner()

	// defer final cleanup to allow this function to return
	// otherwise, a deadlock occurs where startReader triggers a shutdown and
	// must wait for itself to exit.
	go func() {
		c.shutdownCleanup()
	}()
}

func (c *Connection) shutdownInner() {
	c.shutdownSignalDone()
	c.shutdownLogStart()
	c.shutdownCloseSocket()
}

func (c *Connection) shutdownCleanup() {
	c.cleanupOnce.Do(func() {
		c.wg.Wait()
		c.shutdownCloseChannels()
		c.shutdownLogComplete()
	})
}

func (c *Connection) shutdownSetClosed(wait bool) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return NewConnectionError(ErrConnectionClosed)
	}
	c.closed = true
	c.state = StateClosed
	c.mu.Unlock()
	return nil
}

func (c *Connection) shutdownSignalDone() {
	c.doneOnce.Do(func() {
		close(c.done)
	})
}

func (c *Connection) shutdownLogStart() {
	if c.logger != nil {
		c.logger.Info("closing")
	}
}

func (c *Connection) shutdownCloseSocket() {
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

func (c *Connection) shutdownCloseChannels() {
	close(c.incoming)
	close(c.errors)
}

func (c *Connection) shutdownLogComplete() {
	if c.logger != nil {
		c.logger.Info("closed")
	}
}

func (c *Connection) startReader() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.shutdownInternal()

		for {
			select {
			case <-c.done:
				return
			default:
				messageType, data, err := c.socket.ReadMessage()
				if err != nil {
					var wrappedErr error
					var closeErr *websocket.CloseError
					if errors.As(err, &closeErr) {
						switch closeErr.Code {
						case websocket.CloseNormalClosure, websocket.CloseGoingAway:
							if c.logger != nil {
								c.logger.Info("connection closed by peer",
									"code", closeErr.Code,
									"text", closeErr.Text,
								)
							}
							wrappedErr = fmt.Errorf("%w: %w", ErrPeerClosedClean, err)
						default:
							if c.logger != nil {
								c.logger.Error("unexpected close",
									"code", closeErr.Code,
									"text", closeErr.Text,
								)
							}
							wrappedErr = fmt.Errorf("%w: %w", ErrPeerClosedUnexpected, err)
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
						wrappedErr = fmt.Errorf("%w: %w", ErrReadError, err)
					}

					select {
					case <-c.done:
					case c.errors <- wrappedErr:
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
	}()
}

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
	if c.config.PingInterval <= 0 {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
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
				if err := c.socket.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
					return
				}
			}
		}
	}()

}

func (c *Connection) Send(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return NewConnectionError(ErrConnectionClosed)
	}

	if c.config.WriteTimeout > 0 {
		if err := c.socket.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout)); err != nil {
			if c.logger != nil {
				c.logger.Error("write deadline error", "error", err)
			}
			return NewConnectionError(fmt.Errorf("%w: %w", ErrFailedWriteDeadline, err))
		}
	}

	if err := c.socket.WriteMessage(websocket.TextMessage, data); err != nil {
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

func (c *Connection) SetDialer(d types.Dialer) {
	c.dialer = d
}
