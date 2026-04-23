package outbound

import "errors"
import "fmt"

var (
	// Config errors
	InvalidKeepaliveTimeout = errors.New("keepalive timeout cannot be negative")
	InvalidMaxQueueSize     = errors.New("maximum queue size cannot be negative")
	InvalidBufferSize       = errors.New("buffer size must be greater than zero")

	// Pool errors
	ErrInvalidPoolID = errors.New("pool id cannot be empty")
	ErrPoolClosed    = errors.New("pool is closed")
	ErrPeerNotFound  = errors.New("peer not found")
	ErrPeerExists    = errors.New("peer already exists")

	// Worker errors
	ErrConnectionUnavailable = errors.New("connection unavailable")
)

func NewConfigError(err error) error {
	return fmt.Errorf("configuration error: %w", err)
}

func NewPoolError(err error) error {
	return fmt.Errorf("pool error: %w", err)
}

func NewWorkerError(id string, err error) error {
	return fmt.Errorf("worker %q error: %w", id, err)
}
