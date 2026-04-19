package transport

import "errors"
import "fmt"

var (
	// URL Errors
	InvalidProtocol = errors.New("URL must use ws:// or wss:// scheme")

	// Configuration Errors
	InvalidWriteTimeout      = errors.New("write timeout cannot be negative")
	InvalidRetryMaxRetries   = errors.New("max retry count cannot be negative")
	InvalidRetryInitialDelay = errors.New("initial delay must be positive")
	InvalidRetryMaxDelay     = errors.New("max delay must be positive")
	InvalidRetryJitterFactor = errors.New("jitter factor must be between 0.0 and 1.0")
	InvalidDelays            = errors.New("initial delay may not exceed maximum delay")

	// Socket Errors
	ErrNilRetryManager = errors.New("retry manager cannot be nil")
	ErrNilDialer       = errors.New("dialer cannot be nil")
	ErrEmptyURL        = errors.New("URL cannot be empty")

	// Connection Errors
	ErrConnectionClosed     = errors.New("connection closed")
	ErrWriteFailed          = errors.New("write failed")
	ErrNilSocket            = errors.New("socket cannot be nil")
	ErrSocketExists         = errors.New("socket already exists")
	ErrFailedWriteDeadline  = errors.New("failed to set write deadline")
	ErrPeerClosedClean      = errors.New("peer closed connection cleanly")
	ErrPeerClosedUnexpected = errors.New("peer closed connection unexpectedly")
	ErrReadError            = errors.New("read error")
)

func NewConfigError(err error) error {
	return fmt.Errorf("configuration error: %w", err)
}

func NewConnectionError(err error) error {
	return fmt.Errorf("connection error: %w", err)
}
