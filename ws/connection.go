package ws

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/errors"
	"github.com/gorilla/websocket"
)

type Dialer interface {
	Dial(urlStr string, requestHeader http.Header) (Socket, *http.Response, error)
}

func NewDialer() Dialer {
	return NewGorillaDialer()
}

type GorillaDialer struct {
	*websocket.Dialer
}

func NewGorillaDialer() *GorillaDialer {
	return &GorillaDialer{
		Dialer: &websocket.Dialer{
			HandshakeTimeout: 45 * time.Second,
			ReadBufferSize:   1024,
			WriteBufferSize:  1024,
		},
	}
}

// Returns the Socket interface
func (d *GorillaDialer) Dial(
	urlStr string, requestHeader http.Header,
) (
	Socket, *http.Response, error,
) {
	conn, resp, err := d.Dialer.Dial(urlStr, requestHeader)
	return conn, resp, err
}

type Socket interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error

	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetCloseHandler(h func(code int, text string) error)
}

func AcquireSocket(
	retryMgr *RetryManager,
	dialer Dialer,
	urlStr string,
) (Socket, *http.Response, error) {
	if retryMgr == nil {
		return nil, nil, errors.NewConnectionError("retry manager cannot be nil")
	}
	if dialer == nil {
		return nil, nil, errors.NewConnectionError("dialer cannot be nil")
	}
	if urlStr == "" {
		return nil, nil, errors.NewConnectionError("URL cannot be empty")
	}

	for {
		socket, resp, err := dialer.Dial(urlStr, nil)
		if err == nil {
			return socket, resp, nil
		}

		if !retryMgr.ShouldRetry() {
			return nil, nil, err
		}

		delay := retryMgr.CalculateDelay()
		time.Sleep(delay)
		retryMgr.RecordRetry()
	}
}

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
	dialer Dialer
	socket Socket
	config *Config

	incoming chan []byte
	outgoing chan []byte
	errors   chan error
	done     chan struct{}

	state ConnectionState

	wg     sync.WaitGroup
	once   sync.Once
	closed bool
	mu     sync.RWMutex
}

func NewConnection(urlStr string, config *Config) (*Connection, error) {
	if config == nil {
		config = GetDefaultConfig()
	}

	if err := ValidateConfig(config); err != nil {
		return nil, err
	}

	parsedURL, err := ParseURL(urlStr)
	if err != nil {
		return nil, err
	}

	return &Connection{
		url:      parsedURL,
		dialer:   NewDialer(),
		socket:   nil,
		config:   config,
		incoming: make(chan []byte, 100),
		outgoing: make(chan []byte, 100),
		errors:   make(chan error, 10),
		state:    StateDisconnected,
		done:     make(chan struct{}),
	}, nil
}

func NewConnectionFromSocket(socket Socket, config *Config) (*Connection, error) {
	if socket == nil {
		return nil, errors.NewConnectionError("socket cannot be nil")
	}

	if config == nil {
		config = GetDefaultConfig()
	}

	if err := ValidateConfig(config); err != nil {
		return nil, err
	}

	conn := &Connection{
		url:      nil,
		dialer:   nil,
		socket:   socket,
		config:   config,
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
		return errors.NewConnectionError("connection already has socket")
	}

	if c.closed {
		return errors.NewConnectionError("connection is closed")
	}

	c.state = StateConnecting

	retryMgr := NewRetryManager(c.config.Retry)
	socket, _, err := AcquireSocket(retryMgr, c.dialer, c.url.String())

	if err != nil {
		c.state = StateDisconnected
		return err
	}

	c.socket = socket
	c.state = StateConnected

	if c.config.CloseHandler != nil {
		c.socket.SetCloseHandler(c.config.CloseHandler)
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
			select {
			case <-c.done:
				return
			default:
				if c.config.ReadTimeout > 0 {
					if err := c.socket.SetReadDeadline(time.Now().Add(c.config.ReadTimeout)); err != nil {
						select {
						case c.errors <- fmt.Errorf("failed to set read deadline: %w", err):
						case <-c.done:
						}
						c.Close()
						return
					}
				}
				messageType, data, err := c.socket.ReadMessage()
				if err != nil {
					select {
					case c.errors <- err:
					case <-c.done:
					}
					c.Close()
					return
				}

				if messageType == websocket.TextMessage ||
					messageType == websocket.BinaryMessage {
					select {
					case c.incoming <- data:
					case <-c.done:
						c.Close()
						return
					}
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
						select {
						case c.errors <- fmt.Errorf("failed to set write deadline: %w", err):
						case <-c.done:
						}
						c.Close()
						return
					}
				}

				if err := c.socket.WriteMessage(websocket.TextMessage, data); err != nil {
					select {
					case c.errors <- err:
					case <-c.done:
					}
					c.Close()
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
		return errors.NewConnectionError("connection closed")
	}

	select {
	case c.outgoing <- data:
		return nil
	default:
		return errors.NewConnectionError("outgoing queue full")
	}
}

func (c *Connection) Incoming() <-chan []byte {
	return c.incoming
}

func (c *Connection) Errors() <-chan error {
	return c.errors
}

// Close shuts down the connection and waits for goroutines to exit.
// If the underlying socket blocks indefinitely on read or write operations,
// Close will also block. This is expected behavior - hung sockets require
// external intervention (timeouts, process termination, etc).
func (c *Connection) Close() error {
	c.mu.Lock()

	alreadyClosed := c.closed
	if !alreadyClosed {
		c.closed = true
		c.state = StateClosed
		close(c.done)
	}

	socket := c.socket
	c.mu.Unlock()

	if alreadyClosed {
		return nil
	}

	var err error
	if socket != nil {
		err = socket.Close()
	}

	c.wg.Wait()

	close(c.incoming)
	close(c.outgoing)
	close(c.errors)

	return err
}

func (c *Connection) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}
