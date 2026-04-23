package logging

import (
	"log/slog"
)

// Constants

const KEY_MODULE = "module"
const KEY_COMPONENT = "component"
const KEY_POOL_ID = "pool_id"
const KEY_PEER_ID = "peer_id"

const MODULE_NAME = "honeybee"

const COMPONENT_OUTBOUND_POOL = "outbound_pool"
const COMPONENT_OUTBOUND_WORKER = "outbound_worker"

const COMPONENT_INBOUND_POOL = "inbound_pool"
const COMPONENT_INBOUND_WORKER = "inbound_worker"

const COMPONENT_CONNECTION = "connection"

// Outbound loggers

func NewOutboundPoolLogger(handler slog.Handler, poolID string) *slog.Logger {
	return newLogger(handler,
		KEY_MODULE, MODULE_NAME,
		KEY_COMPONENT, COMPONENT_OUTBOUND_POOL,
		KEY_POOL_ID, poolID,
	)
}

func NewOutboundWorkerLogger(handler slog.Handler, poolID string, peerID string) *slog.Logger {
	return newLogger(handler,
		KEY_MODULE, MODULE_NAME,
		KEY_COMPONENT, COMPONENT_OUTBOUND_WORKER,
		KEY_POOL_ID, poolID,
		KEY_PEER_ID, peerID,
	)
}

// Inbound loggers

func NewInboundPoolLogger(handler slog.Handler, poolID string) *slog.Logger {
	return newLogger(handler,
		KEY_MODULE, MODULE_NAME,
		KEY_COMPONENT, COMPONENT_INBOUND_POOL,
		KEY_POOL_ID, poolID,
	)
}

func NewInboundWorkerLogger(handler slog.Handler, poolID string, peerID string) *slog.Logger {
	return newLogger(handler,
		KEY_MODULE, MODULE_NAME,
		KEY_COMPONENT, COMPONENT_INBOUND_WORKER,
		KEY_POOL_ID, poolID,
		KEY_PEER_ID, peerID,
	)
}

// Connection logger

func NewConnectionLogger(handler slog.Handler, poolID string, peerID string) *slog.Logger {
	return newLogger(handler,
		KEY_MODULE, MODULE_NAME,
		KEY_COMPONENT, COMPONENT_CONNECTION,
		KEY_POOL_ID, poolID,
		KEY_PEER_ID, peerID,
	)
}

// Helpers

func newLogger(handler slog.Handler, attrs ...any) *slog.Logger {
	return slog.New(handler).With(attrs...)
}
