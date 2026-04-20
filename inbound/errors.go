package inbound

import "errors"

var (
	// Pool errors
	PoolError       = errors.New("pool error")
	ErrPoolClosed   = errors.New("pool is closed")
	ErrPeerNotFound = errors.New("peer not found")
	ErrPeerExists   = errors.New("peer already exists")

	// Config errors
	InvalidMaxQueueSize      = errors.New("maximum queue size cannot be negative")
	InvalidInactivityTimeout = errors.New("inactivity timeout cannot be negative")
)
