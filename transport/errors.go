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

	// Connection Errors
	ErrConnectionClosed = errors.New("connection closed")
	ErrWriteFailed      = errors.New("write failed")
)

func NewConfigError(text string) error {
	return fmt.Errorf("configuration error: %s", text)
}

func NewConnectionError(text string) error {
	return fmt.Errorf("connection error: %s", text)
}
