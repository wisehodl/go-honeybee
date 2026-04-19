package honeybee

import (
	"context"
	"log/slog"

	"git.wisehodl.dev/jay/go-honeybee/initiatorpool"
	"git.wisehodl.dev/jay/go-honeybee/transport"
)

// Connection types

type Connection = transport.Connection
type ConnectionConfig = transport.ConnectionConfig
type RetryConfig = transport.RetryConfig
type ConnectionOption = transport.ConnectionOption

// Initator Pool types

type InitiatorPool = initiatorpool.Pool
type InitiatorPoolConfig = initiatorpool.PoolConfig
type InitiatorPoolOption = initiatorpool.PoolOption
type InitiatorWorkerConfig = initiatorpool.WorkerConfig
type InitiatorWorkerOption = initiatorpool.WorkerOption
type InitiatorInboxMessage = initiatorpool.InboxMessage
type InitiatorPoolEvent = initiatorpool.PoolEvent
type InitiatorPoolEventKind = initiatorpool.PoolEventKind

// Pool event constants

const (
	EventConnected    = initiatorpool.EventConnected
	EventDisconnected = initiatorpool.EventDisconnected
)

// Connection constructors

func NewConnection(url string, config *ConnectionConfig, logger *slog.Logger) (*Connection, error) {
	return transport.NewConnection(url, config, logger)
}

func NewConnectionConfig(opts ...ConnectionOption) (*ConnectionConfig, error) {
	return transport.NewConnectionConfig(opts...)
}

// Connection options

var (
	WithoutRetry          = transport.WithoutRetry
	WithRetryMaxRetries   = transport.WithRetryMaxRetries
	WithRetryInitialDelay = transport.WithRetryInitialDelay
	WithRetryMaxDelay     = transport.WithRetryMaxDelay
	WithRetryJitterFactor = transport.WithRetryJitterFactor
	WithWriteTimeout      = transport.WithWriteTimeout
	WithCloseHandler      = transport.WithCloseHandler
)

// Initiator Pool constructors

func NewInitiatorPool(ctx context.Context, config *InitiatorPoolConfig, logger *slog.Logger) (*InitiatorPool, error) {
	return initiatorpool.NewPool(ctx, config, logger)
}

func NewInitiatorPoolConfig(opts ...InitiatorPoolOption) (*InitiatorPoolConfig, error) {
	return initiatorpool.NewPoolConfig(opts...)
}

func NewInitiatorWorkerConfig(opts ...InitiatorWorkerOption) (*InitiatorWorkerConfig, error) {
	return initiatorpool.NewWorkerConfig(opts...)
}

// Initiator Pool options

var (
	WithConnectionConfig = initiatorpool.WithConnectionConfig
	WithWorkerConfig     = initiatorpool.WithWorkerConfig
	WithWorkerFactory    = initiatorpool.WithWorkerFactory
)

// Initiator Worker options

var (
	WithKeepaliveTimeout = initiatorpool.WithKeepaliveTimeout
	WithMaxQueueSize     = initiatorpool.WithMaxQueueSize
)
