package transport

import (
	"context"
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
func (d *GorillaDialer) DialContext(
	ctx context.Context,
	url string,
	header http.Header,
) (
	types.Socket, *http.Response, error,
) {
	conn, resp, err := d.Dialer.DialContext(ctx, url, header)
	return conn, resp, err
}

func AcquireSocket(
	ctx context.Context,
	retryMgr *RetryManager,
	dialer types.Dialer,
	url string,
	logger *slog.Logger,
) (types.Socket, *http.Response, error) {
	if retryMgr == nil {
		return nil, nil, NewConnectionError("retry manager cannot be nil")
	}
	if dialer == nil {
		return nil, nil, NewConnectionError("dialer cannot be nil")
	}
	if url == "" {
		return nil, nil, NewConnectionError("URL cannot be empty")
	}

	for {
		if logger != nil {
			logger.Info("dialing", "attempt", retryMgr.RetryCount()+1)
		}

		socket, resp, err := dialer.DialContext(ctx, url, nil)
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

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		retryMgr.RecordRetry()
	}
}
