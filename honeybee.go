package honeybee

import (
	"context"
	"log/slog"

	"git.wisehodl.dev/jay/go-honeybee/inbound"
	"git.wisehodl.dev/jay/go-honeybee/outbound"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
)

// Connection types

type Connection = transport.Connection
type ConnectionConfig = transport.ConnectionConfig
type RetryConfig = transport.RetryConfig
type ConnectionOption = transport.ConnectionOption

// Outbound Pool types

type OutboundPool = outbound.Pool
type OutboundPoolConfig = outbound.PoolConfig
type OutboundPoolOption = outbound.PoolOption
type OutboundWorkerConfig = outbound.WorkerConfig
type OutboundWorkerOption = outbound.WorkerOption
type OutboundInboxMessage = outbound.InboxMessage
type OutboundPoolEvent = outbound.PoolEvent
type OutboundPoolEventKind = outbound.PoolEventKind

// Pool event constants

const (
	OutboundEventConnected    = outbound.EventConnected
	OutboundEventDisconnected = outbound.EventDisconnected
)

// Inbound Pool types

type InboundPool = inbound.Pool
type InboundPoolConfig = inbound.PoolConfig
type InboundPoolOption = inbound.PoolOption
type InboundWorkerConfig = inbound.WorkerConfig
type InboundWorkerOption = inbound.WorkerOption
type InboundWorkerFactory = inbound.WorkerFactory
type InboundWorker = inbound.Worker
type InboundWorkerExitKind = inbound.WorkerExitKind
type InboundInboxMessage = inbound.InboxMessage
type InboundPoolEvent = inbound.PoolEvent
type InboundPoolEventKind = inbound.PoolEventKind

// Inbound Pool event constants

const (
	InboundEventDisconnected = inbound.EventDisconnected
	InboundEventDropped      = inbound.EventDropped
	InboundEventEvicted      = inbound.EventEvicted
)

// Inbound Worker exit kinds

const (
	InboundExitDisconnected = inbound.ExitDisconnected
	InboundExitError        = inbound.ExitError
	InboundExitPolicy       = inbound.ExitPolicy
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

// Outbound Pool constructors

func NewOutboundPool(ctx context.Context, config *OutboundPoolConfig, logger *slog.Logger) (*OutboundPool, error) {
	return outbound.NewPool(ctx, config, logger)
}

func NewOutboundPoolConfig(opts ...OutboundPoolOption) (*OutboundPoolConfig, error) {
	return outbound.NewPoolConfig(opts...)
}

func NewOutboundWorkerConfig(opts ...OutboundWorkerOption) (*OutboundWorkerConfig, error) {
	return outbound.NewWorkerConfig(opts...)
}

// Outbound Pool options

var (
	WithOutboundConnectionConfig = outbound.WithConnectionConfig
	WithOutboundWorkerConfig     = outbound.WithWorkerConfig
	WithOutboundWorkerFactory    = outbound.WithWorkerFactory
)

// Outbound Worker options

var (
	WithOutboundKeepaliveTimeout = outbound.WithKeepaliveTimeout
	WithOutboundMaxQueueSize     = outbound.WithMaxQueueSize
)

// Inbound Pool constructors

func NewInboundPool(ctx context.Context, config *InboundPoolConfig, logger *slog.Logger) (*InboundPool, error) {
	return inbound.NewPool(ctx, config, logger)
}

func NewInboundPoolConfig(opts ...InboundPoolOption) (*InboundPoolConfig, error) {
	return inbound.NewPoolConfig(opts...)
}

func NewInboundWorkerConfig(opts ...InboundWorkerOption) (*InboundWorkerConfig, error) {
	return inbound.NewWorkerConfig(opts...)
}

// Inbound Pool options

var (
	WithInboundConnectionConfig = inbound.WithConnectionConfig
	WithInboundWorkerConfig     = inbound.WithWorkerConfig
	WithInboundWorkerFactory    = inbound.WithWorkerFactory
)

// Inbound Worker options

var (
	WithInboundInactivityTimeout = inbound.WithInactivityTimeout
	WithInboundMaxQueueSize      = inbound.WithMaxQueueSize
)

// Socket type — needed for inbound pool.Add and pool.Replace

type Socket = types.Socket
