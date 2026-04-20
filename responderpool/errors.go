package responderpool

import "errors"

var (
	// Pool errors
	ErrPoolClosed   = errors.New("pool is closed")
	ErrPeerNotFound = errors.New("peer not found")
	ErrPeerExists   = errors.New("peer already exists")

	// Config errors
	InvalidMaxQueueSize = errors.New("maximum queue size cannot be negative")
	InvalidDeadTimeout  = errors.New("dead timeout cannot be negative")
)
