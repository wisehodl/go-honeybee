package errors

import "errors"
import "fmt"

var (
	// URL Errors
	InvalidProtocol = errors.New("URL must use ws:// or wss:// scheme")

	// Configuration Errors
	InvalidIdleTimeout       = errors.New("idle timeout cannot be negative")
	InvalidWriteTimeout      = errors.New("write timeout cannot be negative")
	InvalidRetryMaxRetries   = errors.New("max retry count cannot be negative")
	InvalidRetryInitialDelay = errors.New("initial delay must be positive")
	InvalidRetryMaxDelay     = errors.New("max delay must be positive")
	InvalidRetryJitterFactor = errors.New("jitter factor must be between 0.0 and 1.0")
)

func NewConfigError(text string) error {
	return fmt.Errorf("configuration error: %s", text)
}

func NewConnectionError(text string) error {
	return fmt.Errorf("connection error: %s", text)
}

func NewPoolError(text string) error {
	return fmt.Errorf("pool error: %s", text)
}
