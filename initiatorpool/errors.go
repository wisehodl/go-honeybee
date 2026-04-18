package initiatorpool

import "errors"
import "fmt"

var (
	InvalidKeepaliveTimeout = errors.New("keepalive timeout cannot be negative")
	InvalidMaxQueueSize     = errors.New("maximum queue size cannot be negative")
)

func NewConfigError(text string) error {
	return fmt.Errorf("configuration error: %s", text)
}

func NewPoolError(text string) error {
	return fmt.Errorf("pool error: %s", text)
}

func NewWorkerError(id string, text string) error {
	return fmt.Errorf("worker %q error: %s", id, text)
}
