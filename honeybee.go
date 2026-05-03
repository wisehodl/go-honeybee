package honeybee

import (
	"context"
	"log/slog"

	"git.wisehodl.dev/jay/go-honeybee/inbound"
	"git.wisehodl.dev/jay/go-honeybee/outbound"
	"git.wisehodl.dev/jay/go-honeybee/transport"
	"git.wisehodl.dev/jay/go-honeybee/types"
)

// Functions

func NormalizeURL(input string) (string, error) {
	return transport.NormalizeURL(input)
}

// Connection types

type Connection = transport.Connection
type ConnectionConfig = transport.ConnectionConfig
type RetryConfig = transport.RetryConfig
type ConnectionOption = transport.ConnectionOption
type ConnectionStats = transport.ConnectionStats

// Common Types

type InboxMessage = types.InboxMessage

// Outbound Pool types

type OutboundPool = outbound.Pool
type OutboundPoolConfig = outbound.PoolConfig
type OutboundPoolOption = outbound.PoolOption
type OutboundWorkerConfig = outbound.WorkerConfig
type OutboundWorkerOption = outbound.WorkerOption
type OutboundPoolEvent = outbound.PoolEvent
type OutboundPoolEventKind = outbound.PoolEventKind
type OutboundPoolStats = outbound.PoolStats
type OutboundWorkerStats = outbound.WorkerStats

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
type InboundPoolEvent = inbound.PoolEvent
type InboundPoolEventKind = inbound.PoolEventKind
type InboundPoolStats = inbound.PoolStats
type InboundWorkerStats = inbound.WorkerStats

// Inbound Pool event constants

const (
	InboundEventDisconnected  = inbound.EventDisconnected
	InboundEventDroppedClose  = inbound.EventDroppedClose
	InboundEventDroppedError  = inbound.EventDroppedError
	InboundEventEvictedPolicy = inbound.EventEvictedPolicy
)

// Inbound Worker exit kinds

const (
	InboundExitDisconnected    = inbound.ExitDisconnected
	InboundExitUnexpectedClose = inbound.ExitUnexpectedClose
	InboundExitReadError       = inbound.ExitReadError
	InboundExitPolicy          = inbound.ExitPolicy
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
	WithIncomingBufferSize       = transport.WithIncomingBufferSize
	WithErrorsBufferSize         = transport.WithErrorsBufferSize
	WithoutRetry                 = transport.WithoutRetry
	WithRetryMaxRetries          = transport.WithRetryMaxRetries
	WithRetryInitialDelay        = transport.WithRetryInitialDelay
	WithRetryMaxDelay            = transport.WithRetryMaxDelay
	WithRetryJitterFactor        = transport.WithRetryJitterFactor
	WithWriteTimeout             = transport.WithWriteTimeout
	WithCloseHandler             = transport.WithCloseHandler
	WithConnectionLoggingEnabled = transport.WithLoggingEnabled
	WithConnectionLogLevel       = transport.WithLogLevel
)

// Outbound Pool constructors

func NewOutboundPool(ctx context.Context, id string, config *OutboundPoolConfig, handler slog.Handler) (*OutboundPool, error) {
	return outbound.NewPool(ctx, id, config, handler)
}

func NewOutboundPoolConfig(opts ...OutboundPoolOption) (*OutboundPoolConfig, error) {
	return outbound.NewPoolConfig(opts...)
}

func NewOutboundWorkerConfig(opts ...OutboundWorkerOption) (*OutboundWorkerConfig, error) {
	return outbound.NewWorkerConfig(opts...)
}

// Outbound Pool options

var (
	WithOutboundInboxBufferSize    = outbound.WithInboxBufferSize
	WithOutboundEventsBufferSize   = outbound.WithEventsBufferSize
	WithOutboundErrorsBufferSize   = outbound.WithErrorsBufferSize
	WithOutboundPoolLoggingEnabled = outbound.WithPoolLoggingEnabled
	WithOutboundPoolLogLevel       = outbound.WithPoolLogLevel
	WithOutboundConnectionConfig   = outbound.WithConnectionConfig
	WithOutboundWorkerConfig       = outbound.WithWorkerConfig
	WithOutboundWorkerFactory      = outbound.WithWorkerFactory
)

// Outbound Worker options

var (
	WithOutboundKeepaliveTimeout     = outbound.WithKeepaliveTimeout
	WithOutboundMaxQueueSize         = outbound.WithMaxQueueSize
	WithOutboundWorkerLoggingEnabled = outbound.WithWorkerLoggingEnabled
	WithOutboundWorkerLogLevel       = outbound.WithWorkerLogLevel
)

// Inbound Pool constructors

func NewInboundPool(ctx context.Context, id string, config *InboundPoolConfig, handler slog.Handler) (*InboundPool, error) {
	return inbound.NewPool(ctx, id, config, handler)
}

func NewInboundPoolConfig(opts ...InboundPoolOption) (*InboundPoolConfig, error) {
	return inbound.NewPoolConfig(opts...)
}

func NewInboundWorkerConfig(opts ...InboundWorkerOption) (*InboundWorkerConfig, error) {
	return inbound.NewWorkerConfig(opts...)
}

// Inbound Pool options

var (
	WithInboundInboxBufferSize    = inbound.WithInboxBufferSize
	WithInboundEventsBufferSize   = inbound.WithEventsBufferSize
	WithInboundErrorsBufferSize   = inbound.WithErrorsBufferSize
	WithInboundPoolLoggingEnabled = inbound.WithPoolLoggingEnabled
	WithInboundPoolLogLevel       = inbound.WithPoolLogLevel
	WithInboundConnectionConfig   = inbound.WithConnectionConfig
	WithInboundWorkerConfig       = inbound.WithWorkerConfig
	WithInboundWorkerFactory      = inbound.WithWorkerFactory
)

// Inbound Worker options

var (
	WithInboundInactivityTimeout    = inbound.WithInactivityTimeout
	WithInboundMaxQueueSize         = inbound.WithMaxQueueSize
	WithInboundWorkerLoggingEnabled = inbound.WithWorkerLoggingEnabled
	WithInboundWorkerLogLevel       = inbound.WithWorkerLogLevel
)

// Socket type — needed for inbound pool.Add and pool.Replace

type Socket = types.Socket
