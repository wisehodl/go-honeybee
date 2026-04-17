package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
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

type Connection struct {
	url    *url.URL
	dialer types.Dialer
	socket types.Socket
	config *ConnectionConfig
	logger *slog.Logger

	incoming chan []byte
	outgoing chan []byte
	errors   chan error
	done     chan struct{}

	state ConnectionState

	wg     sync.WaitGroup
	closed bool
	mu     sync.RWMutex
}

func NewConnection(urlStr string, config *ConnectionConfig, logger *slog.Logger) (*Connection, error) {
	if config == nil {
		config = GetDefaultConnectionConfig()
	}

	if err := ValidateConnectionConfig(config); err != nil {
		return nil, err
	}

	parsedURL, err := ParseURL(urlStr)
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		url:      parsedURL,
		dialer:   NewDialer(),
		socket:   nil,
		config:   config,
		logger:   logger,
		incoming: make(chan []byte, 100),
		outgoing: make(chan []byte, 100),
		errors:   make(chan error, 10),
		state:    StateDisconnected,
		done:     make(chan struct{}),
	}

	return conn, nil
}

func NewConnectionFromSocket(socket types.Socket, config *ConnectionConfig, logger *slog.Logger) (*Connection, error) {
	if socket == nil {
		return nil, NewConnectionError("socket cannot be nil")
	}

	if config == nil {
		config = GetDefaultConnectionConfig()
	}

	if err := ValidateConnectionConfig(config); err != nil {
		return nil, err
	}

	conn := &Connection{
		url:      nil,
		dialer:   nil,
		socket:   socket,
		config:   config,
		logger:   logger,
		incoming: make(chan []byte, 100),
		outgoing: make(chan []byte, 100),
		errors:   make(chan error, 10),
		state:    StateConnected,
		done:     make(chan struct{}),
	}

	if config.CloseHandler != nil {
		socket.SetCloseHandler(config.CloseHandler)
	}

	conn.startReader()
	conn.startWriter()

	return conn, nil
}

func (c *Connection) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.socket != nil {
		return NewConnectionError("connection already has socket")
	}

	if c.closed {
		return NewConnectionError("connection is closed")
	}

	if c.logger != nil {
		c.logger.Info("connecting")
	}

	c.state = StateConnecting

	retryMgr := NewRetryManager(c.config.Retry)
	socket, _, err := AcquireSocket(retryMgr, c.dialer, c.url.String(), c.logger)

	if err != nil {
		c.state = StateDisconnected
		if c.logger != nil {
			c.logger.Error("connection failed", "error", err)
		}
		return err
	}

	c.socket = socket
	c.state = StateConnected

	if c.config.CloseHandler != nil {
		c.socket.SetCloseHandler(c.config.CloseHandler)
	}

	if c.logger != nil {
		c.logger.Info("connected")
	}

	c.startReader()
	c.startWriter()

	return nil
}

func (c *Connection) startReader() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			messageType, data, err := c.socket.ReadMessage()
			if err != nil {
				if c.logger != nil {
					var closeErr *websocket.CloseError
					if errors.As(err, &closeErr) {
						switch closeErr.Code {
						case websocket.CloseNormalClosure, websocket.CloseGoingAway:
							c.logger.Info("connection closed by peer",
								"code", closeErr.Code,
								"text", closeErr.Text,
							)
						default:
							c.logger.Error("unexpected close",
								"code", closeErr.Code,
								"text", closeErr.Text,
							)
						}
					} else {
						c.logger.Error("read error", "error", err)
					}
				}
				select {
				case c.errors <- err:
				case <-c.done:
				}
				c.shutdown()
				return
			}

			if messageType == websocket.TextMessage ||
				messageType == websocket.BinaryMessage {
				select {
				case c.incoming <- data:
				case <-c.done:
					c.shutdown()
					return
				}
			}

		}
	}()

}

func (c *Connection) startWriter() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-c.done:
				return
			case data := <-c.outgoing:
				if c.config.WriteTimeout > 0 {
					if err := c.socket.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout)); err != nil {
						if c.logger != nil {
							c.logger.Error("write deadline error", "error", err)
						}
						select {
						case c.errors <- fmt.Errorf("failed to set write deadline: %w", err):
						case <-c.done:
						}
						c.shutdown()
						return
					}
				}

				if err := c.socket.WriteMessage(websocket.TextMessage, data); err != nil {
					if c.logger != nil {
						c.logger.Error("write error", "error", err)
					}
					select {
					case c.errors <- err:
					case <-c.done:
					}
					c.shutdown()
					return
				}
			}
		}
	}()

}

func (c *Connection) Send(data []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return NewConnectionError("connection closed")
	}

	select {
	case c.outgoing <- data:
		return nil
	case <-c.done:
		return NewConnectionError("connection closing")
	default:
		return NewConnectionError("outgoing queue full")
	}
}

func (c *Connection) Incoming() <-chan []byte {
	return c.incoming
}

func (c *Connection) Errors() <-chan error {
	return c.errors
}

func (c *Connection) shutdown() {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}

	if c.logger != nil {
		c.logger.Info("closing", "state", c.state.String())
	}
	c.closed = true
	c.state = StateClosed
	socket := c.socket
	close(c.done)
	c.mu.Unlock()

	go func() {
		if socket != nil {
			// force immediate timeout of any blocked network I/O
			expired := time.Now().Add(-1 * time.Minute)
			socket.SetReadDeadline(expired)
			socket.SetWriteDeadline(expired)
			err := socket.Close()

			if err != nil {
				if c.logger != nil {
					c.logger.Error("socket close failed", "error", err)
				}
			} else {
				if c.logger != nil {
					c.logger.Info("closed")
				}
			}
		}

		c.wg.Wait()
		close(c.incoming)
		close(c.outgoing)
		close(c.errors)
	}()

}

func (c *Connection) Close() {
	c.shutdown()
}

func (c *Connection) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *Connection) SetDialer(d types.Dialer) {
	c.dialer = d
}
