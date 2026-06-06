package transport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"git.wisehodl.dev/jay/go-honeybee/types"
	"git.wisehodl.dev/jay/go-mana-component"
	"github.com/gorilla/websocket"
)

// ----------------------------------------------------------------------------
// Errors
// ----------------------------------------------------------------------------

var (
	ErrNilDialFunc = errors.New("dial func cannot be nil")
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

func NewDialer() types.Dialer {
	return NewGorillaDialer()
}

type GorillaDialer struct {
	*websocket.Dialer
}

const dialTimeout = 10 * time.Second

func NewGorillaDialer() *GorillaDialer {
	netDialer := &net.Dialer{Timeout: dialTimeout}
	return &GorillaDialer{
		Dialer: &websocket.Dialer{
			NetDialContext: netDialer.DialContext,
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

// ----------------------------------------------------------------------------
// Retry Dialer
// ----------------------------------------------------------------------------

func DialWithRetry(
	ctx context.Context,
	mgr *RetryManager,
	dialFn func(context.Context) (types.Socket, error),
	onError func(error),
	handler slog.Handler,
) (types.Socket, error) {
	if component.FromContext(ctx) == nil {
		ctx = component.MustNew(ctx, "honeybee", "dialer")
	} else {
		ctx = component.MustExtend(ctx, "dialer")
	}

	var logger *slog.Logger
	if handler != nil {
		comp := component.FromContext(ctx)
		logger = slog.New(handler).With("component", comp)
	}

	if dialFn == nil {
		return nil, ErrNilDialFunc
	}

	for {
		if logger != nil {
			logger.Debug("dialing", "attempt", mgr.RetryCount()+1)
		}

		// dial
		socket, err := dialFn(ctx)
		if err == nil {
			if logger != nil {
				logger.Debug("dial successful", "attempt", mgr.RetryCount()+1)
			}
			return socket, nil
		}

		if onError != nil {
			onError(err)
		}

		if mgr == nil {
			if logger != nil {
				logger.Debug("dial failed, retry disabled", "error", err)
			}
			return nil, err
		}

		// dial failed, retry
		if !mgr.ShouldRetry() {
			// retry policy expired
			if logger != nil {
				logger.Debug("dial failed, max retries reached",
					"error", err,
					"attempt", mgr.RetryCount()+1)
			}
			return nil, err
		}

		delay := mgr.CalculateDelay()

		if logger != nil {
			logger.Warn("dial failed, retrying",
				"error", err,
				"attempt", mgr.RetryCount()+1,
				"next_delay", delay)
		}

		// context cancellable backoff
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		mgr.RecordRetry()
	}
}
