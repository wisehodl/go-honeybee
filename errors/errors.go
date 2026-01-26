package errors

import "errors"
import "fmt"

var (
	// URL Errors
	InvalidProtocol = errors.New("URL must use ws:// or wss:// scheme")

	// Configuration Errors
	InvalidReadTimeout       = errors.New("read timeout must be positive")
	InvalidWriteTimeout      = errors.New("write timeout must be positive")
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
