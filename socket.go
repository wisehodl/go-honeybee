package honeybee

import (
	"net/http"
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
