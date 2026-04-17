package transport

import (
	"log/slog"
	"net/http"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/types"
	"github.com/gorilla/websocket"
)

func NewDialer() types.Dialer {
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
	types.Socket, *http.Response, error,
) {
	conn, resp, err := d.Dialer.Dial(urlStr, requestHeader)
	return conn, resp, err
}

func AcquireSocket(
	retryMgr *RetryManager,
	dialer types.Dialer,
	urlStr string,
	logger *slog.Logger,
) (types.Socket, *http.Response, error) {
	if retryMgr == nil {
		return nil, nil, NewConnectionError("retry manager cannot be nil")
	}
	if dialer == nil {
		return nil, nil, NewConnectionError("dialer cannot be nil")
	}
	if urlStr == "" {
		return nil, nil, NewConnectionError("URL cannot be empty")
	}

	for {
		if logger != nil {
			logger.Info("dialing", "attempt", retryMgr.RetryCount()+1)
		}

		socket, resp, err := dialer.Dial(urlStr, nil)
		if err == nil {
			if logger != nil {
				logger.Info("dial successful", "attempt", retryMgr.RetryCount()+1)
			}
			return socket, resp, nil
		}

		if !retryMgr.ShouldRetry() {
			if logger != nil {
				logger.Error("dial failed, max retries reached",
					"error", err,
					"attempt", retryMgr.RetryCount()+1)
			}
			return nil, nil, err
		}

		delay := retryMgr.CalculateDelay()

		if logger != nil {
			logger.Warn("dial failed, retrying",
				"error", err,
				"attempt", retryMgr.RetryCount()+1,
				"next_delay", delay)
		}

		time.Sleep(delay)
		retryMgr.RecordRetry()
	}
}
