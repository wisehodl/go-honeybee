# Configuration

All configuration is done through option functions applied at construction time. Invalid values return errors at application time; configs produced through `NewXConfig` constructors are validated at construction and cannot be saved in an invalid state.

There are three config scopes. `ConnectionConfig` governs a single WebSocket connection: its write behavior, ping interval, buffers, and retry policy. `WorkerConfig` governs a single worker's behavior, with separate types for inbound and outbound. `PoolConfig` bundles a connection config, a worker config, and an optional worker factory — it is a thin container.

Logging can be controlled independently at the pool, worker, and connection levels. Each scope has a `LoggingEnabled` flag and a `LogLevel` override. When `LoggingEnabled` is false, no logger is constructed for that scope regardless of what handler is passed to the pool. When `LogLevel` is set, it overrides the handler's own level filter for that scope only, using a wrapping handler that enforces the minimum level before delegating. When `LogLevel` is nil, the handler's own level filtering applies unchanged.

## Connection Options

These are passed to `NewConnectionConfig` or supplied via `WithInboundConnectionConfig` / `WithOutboundConnectionConfig`.

### Write Behavior

**`WithWriteTimeout(duration)`**
Sets a per-message write deadline. Applied before every call to `WriteMessage`. When set to zero, no deadline is applied. Must not be negative.

**`WithCloseHandler(func(code int, text string) error)`**
Installs a close handler on the underlying socket. Called when the remote peer sends a close frame.

### Ping

**`WithPingInterval(duration)`**
Sets the interval at which the connection sends WebSocket ping frames. A ±10% jitter is applied to each interval to avoid synchronized pings across many connections. When set to zero, pings are disabled entirely, and only data messages and outbound sends generate heartbeat signals. Must not be negative.

### Buffers

**`WithIncomingBufferSize(int)`**
Sets the capacity of the channel that buffers inbound messages between the reader goroutine and the consumer. Must be at least 1.

**`WithErrorsBufferSize(int)`**
Sets the capacity of the channel that carries connection-level errors to the consumer. Must be at least 1.

### Retry

The retry policy governs the `Connect()` call only. It does not affect outbound worker reconnection, which is controlled by `ReconnectDelay` on the worker config.

**`WithoutRetry()`**
Disables retry entirely. `Connect()` returns on the first dial failure.

**`WithRetryMaxRetries(int)`**
Caps the number of retry attempts. Zero means retry indefinitely. Must not be negative.

**`WithRetryInitialDelay(duration)`**
Sets the delay before the second dial attempt. The first retry is always immediate. Must be positive.

**`WithRetryMaxDelay(duration)`**
Caps the exponential backoff delay. Must be positive and at least as large as `InitialDelay`.

**`WithRetryJitterFactor(float64)`**
Adds randomization to each backoff delay. A value of 0.2 means the delay varies within ±10% of the base. Range: 0.0 to 1.0.

### Logging

**`WithConnectionLoggingEnabled(bool)`**
Enables or disables logging for the connection. When false, no logger is constructed regardless of the handler passed to the pool.

**`WithConnectionLogLevel(slog.Level)`**
Overrides the minimum log level for connection-scoped records. Does not affect pool or worker logging.

## Inbound Pool Options

### Pool

These are passed to `NewInboundPoolConfig`.

**`WithInboundInboxBufferSize(int)`**
Sets the capacity of the pool's shared inbox channel. Must be at least 1.

**`WithInboundEventsBufferSize(int)`**
Sets the capacity of the pool's events channel. Must be at least 1.

**`WithInboundErrorsBufferSize(int)`**
Sets the capacity of the pool's errors channel. Must be at least 1.

**`WithInboundPoolLoggingEnabled(bool)`**
Enables or disables pool-level logging.

**`WithInboundPoolLogLevel(slog.Level)`**
Overrides the minimum log level for pool-scoped records only.

### Worker

These are passed to `NewInboundWorkerConfig` or embedded in the pool config.

**`WithInboundInactivityTimeout(duration)`**
Enables the inactivity watchdog. When no data messages or pong replies arrive within this duration, the peer is evicted and `InboundEventEvictedPolicy` is emitted. When set to zero, the watchdog is disabled and connections persist until removed or remotely terminated. Must not be negative.

**`WithInboundMaxQueueSize(int)`**
Bounds the worker's internal message queue. When the queue is full, the oldest undelivered message is dropped. When set to zero, the queue is unbounded. Must not be negative.

**`WithInboundWorkerLoggingEnabled(bool)`**
Enables or disables worker-level logging.

**`WithInboundWorkerLogLevel(slog.Level)`**
Overrides the minimum log level for worker-scoped records only.

### Wiring

**`WithInboundConnectionConfig(*ConnectionConfig)`**
Supplies a connection config that is applied to every socket added to the pool.

**`WithInboundWorkerConfig(*WorkerConfig)`**
Supplies a worker config that is applied to every worker the pool creates.

**`WithInboundWorkerFactory(WorkerFactory)`**
Replaces the default worker constructor. See [EXTEND.md](EXTEND.md) for the factory contract.

## Outbound Pool Options

### Pool

These are passed to `NewOutboundPoolConfig`.

**`WithOutboundInboxBufferSize(int)`**
Sets the capacity of the pool's shared inbox channel. Must be at least 1.

**`WithOutboundEventsBufferSize(int)`**
Sets the capacity of the pool's events channel. Must be at least 1.

**`WithOutboundErrorsBufferSize(int)`**
Sets the capacity of the pool's errors channel. Must be at least 1.

**`WithOutboundPoolLoggingEnabled(bool)`**
Enables or disables pool-level logging.

**`WithOutboundPoolLogLevel(slog.Level)`**
Overrides the minimum log level for pool-scoped records only.

### Worker

These are passed to `NewOutboundWorkerConfig` or embedded in the pool config.

**`WithOutboundKeepaliveTimeout(duration)`**
Enables the keepalive mechanism. When no heartbeat (inbound data, outbound send, or pong reply) is observed within this duration, the current connection is closed and a new one is dialed. When set to zero, keepalive is disabled. Must not be negative.

**`WithReconnectDelay(duration)`**
Sets the delay between a disconnect and the next dial attempt. Applies after every session end, including those triggered by keepalive. The default of 2 seconds prevents tight reconnect loops against unavailable peers. Set to zero in tests or when immediate reconnection is required. Must not be negative.

**`WithOutboundMaxQueueSize(int)`**
Bounds the worker's internal message queue. When the queue is full, the oldest undelivered message is dropped. When set to zero, the queue is unbounded. Must not be negative.

**`WithOutboundWorkerLoggingEnabled(bool)`**
Enables or disables worker-level logging.

**`WithOutboundWorkerLogLevel(slog.Level)`**
Overrides the minimum log level for worker-scoped records only.

### Wiring

**`WithOutboundConnectionConfig(*ConnectionConfig)`**
Supplies a connection config used when dialing each peer.

**`WithOutboundWorkerConfig(*WorkerConfig)`**
Supplies a worker config applied to every worker the pool creates.

**`WithOutboundWorkerFactory(WorkerFactory)`**
Replaces the default worker constructor. See [EXTEND.md](EXTEND.md) for the factory contract.

## Defaults

| Scope | Setting | Default | Disabled by | Notes |
|---|---|---|---|---|
| Connection | `WriteTimeout` | 30s | `0` | Per-message write deadline |
| Connection | `PingInterval` | 20s | `0` | ±10% jitter applied per interval |
| Connection | `IncomingBufferSize` | 100 | — | Must be positive |
| Connection | `ErrorsBufferSize` | 10 | — | Must be positive |
| Connection | `LoggingEnabled` | true | `false` | |
| Connection | `LogLevel` | nil | — | nil defers to handler's own filter |
| Retry | enabled | yes | `WithoutRetry()` | Governs `Connect()` only |
| Retry | `MaxRetries` | 0 | — | 0 means infinite |
| Retry | `InitialDelay` | 1s | — | Must be positive |
| Retry | `MaxDelay` | 60s | — | Must be ≥ InitialDelay |
| Retry | `JitterFactor` | 0.2 | `0.0` | Range [0.0, 1.0] |
| Inbound pool | `InboxBufferSize` | 256 | — | Must be positive |
| Inbound pool | `EventsBufferSize` | 10 | — | Must be positive |
| Inbound pool | `ErrorsBufferSize` | 10 | — | Must be positive |
| Inbound pool | `LoggingEnabled` | true | `false` | |
| Inbound pool | `LogLevel` | nil | — | |
| Inbound worker | `InactivityTimeout` | 0 | `0` | 0 disables watchdog |
| Inbound worker | `MaxQueueSize` | 0 | `0` | 0 means unbounded |
| Inbound worker | `LoggingEnabled` | true | `false` | |
| Inbound worker | `LogLevel` | nil | — | |
| Outbound pool | `InboxBufferSize` | 256 | — | Must be positive |
| Outbound pool | `EventsBufferSize` | 10 | — | Must be positive |
| Outbound pool | `ErrorsBufferSize` | 10 | — | Must be positive |
| Outbound pool | `LoggingEnabled` | true | `false` | |
| Outbound pool | `LogLevel` | nil | — | |
| Outbound worker | `KeepaliveTimeout` | 60s | `0` | 0 disables keepalive |
| Outbound worker | `ReconnectDelay` | 2s | `0` | 0 means reconnect immediately |
| Outbound worker | `MaxQueueSize` | 0 | `0` | 0 means unbounded |
| Outbound worker | `LoggingEnabled` | true | `false` | |
| Outbound worker | `LogLevel` | nil | — | |
