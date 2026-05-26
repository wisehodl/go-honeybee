# Configuration

All configuration is done through option functions applied at construction time. Invalid values return errors at application time; configs produced through `NewXConfig` constructors are validated at construction and cannot be saved in an invalid state.

There are three config scopes. `ConnectionConfig` governs a single WebSocket connection: its write behavior, ping interval, buffers, and retry policy. `WorkerConfig` governs a single worker's behavior. `PoolConfig` bundles a connection config, a worker config, and an optional worker factory — it is a thin container.

Logging can be controlled independently at the pool, worker, and connection levels. Each scope has a `LoggingEnabled` flag and a `LogLevel` override. When `LoggingEnabled` is false, no logger is constructed for that scope regardless of what handler is passed to the pool. When `LogLevel` is set, it overrides the handler's own level filter for that scope only, using a wrapping handler that enforces the minimum level before delegating. When `LogLevel` is nil, the handler's own level filtering applies unchanged.

## Defaults

| Scope | Setting | Default | Disabled by | Notes |
|---|---|---|---|---|
| Connection | `WriteTimeout` | 30s | `0` | Per-message write deadline |
| Connection | `RequestHeader` | User-Agent | — | honeybee/0.1.0 |
| Connection | `PingInterval` | 20s | `0` | ±10% jitter applied per interval |
| Connection | `IncomingBufferSize` | 100 | — | Must be positive |
| Connection | `ErrorsBufferSize` | 10 | — | Must be positive |
| Connection | `LoggingEnabled` | true | `false` | |
| Connection | `LogLevel` | nil | — | nil defers to handler's own filter |
| Retry | enabled | yes | `WithRetryDisabled()` | Governs `Connect()` only |
| Retry | `MaxRetries` | 0 | — | 0 means infinite |
| Retry | `InitialDelay` | 1s | — | Must be positive |
| Retry | `MaxDelay` | 60s | — | Must be ≥ InitialDelay |
| Retry | `JitterFactor` | 0.2 | `0.0` | Range [0.0, 1.0] |
| Pool | `InboxBufferSize` | 256 | — | Must be positive |
| Pool | `EventsBufferSize` | 10 | — | Must be positive |
| Pool | `LoggingEnabled` | true | `false` | |
| Pool | `LogLevel` | nil | — | |
| Worker | `KeepaliveTimeout` | 60s | `0` | 0 disables keepalive |
| Worker | `ReconnectDelay` | 2s | `0` | 0 means reconnect immediately |
| Worker | `LoggingEnabled` | true | `false` | |
| Worker | `LogLevel` | nil | — | |

## Connection Options

`ConnectionConfig` is defined in the `transport` package. Import `git.wisehodl.dev/jay/go-honeybee/transport` to construct one, then pass it to a pool via `honeybee.WithConnectionConfig`.

These options are passed to `transport.NewConnectionConfig`.

### Write Behavior

**`WithWriteTimeout(duration)`**
Sets a per-message write deadline. Applied before every call to `WriteMessage`. When set to zero, no deadline is applied. Must not be negative.

**`WithCloseHandler(func(code int, text string) error)`**
Installs a close handler on the underlying socket. Called when the remote peer sends a close frame.

**`WithRequestHeader(http.Header)`**
Sets custom HTTP headers to be sent during the WebSocket handshake. The default includes `User-Agent: honeybee/0.1.0`.

### Ping

**`WithPingInterval(duration)`**
Sets the interval at which the connection sends WebSocket ping frames. A ±10% jitter is applied to each interval to avoid synchronized pings across many connections. When set to zero, pings are disabled entirely, and only data messages and outbound sends generate heartbeat signals. Must not be negative.

### Buffers

**`WithIncomingBufferSize(int)`**
Sets the capacity of the channel that buffers inbound messages between the reader goroutine and the consumer. Must be at least 1.

**`WithErrorsBufferSize(int)`**
Sets the capacity of the channel that carries connection-level errors to the consumer. Must be at least 1.

### Dialer

**`WithConnectionDialer(types.Dialer)`**
Overrides the dialer used to establish the WebSocket connection. When not set, the connection uses the default dialer. Useful in tests or when routing connections through a custom transport.

### Retry

The retry policy governs the `Connect()` call only. It does not affect worker reconnection, which is controlled by `ReconnectDelay` on the worker config.

**`WithRetryDisabled()`**
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

## Pool Options

Import `git.wisehodl.dev/jay/go-honeybee`.

### Pool

These are passed to `honeybee.NewPoolConfig`.

**`honeybee.WithInboxBufferSize(int)`**
Sets the capacity of the pool's shared inbox channel. Must be at least 1.

**`honeybee.WithEventsBufferSize(int)`**
Sets the capacity of the pool's events channel. Must be at least 1.

**`honeybee.WithPoolLoggingEnabled(bool)`**
Enables or disables pool-level logging.

**`honeybee.WithPoolLogLevel(slog.Level)`**
Overrides the minimum log level for pool-scoped records only.

### Worker

These are passed to `honeybee.NewWorkerConfig` or embedded in the pool config.

**`honeybee.WithKeepaliveTimeout(duration)`**
Enables the keepalive mechanism. When no heartbeat (inbound data, outbound send, or pong reply) is observed within this duration, the current connection is closed and a new one is dialed. When set to zero, keepalive is disabled. Must not be negative.

**`honeybee.WithReconnectDelay(duration)`**
Sets the delay between a disconnect and the next dial attempt. Applies after every session end, including those triggered by keepalive. The default of 2 seconds prevents tight reconnect loops against unavailable peers. Set to zero in tests or when immediate reconnection is required. Must not be negative.

**`honeybee.WithWorkerLoggingEnabled(bool)`**
Enables or disables worker-level logging.

**`honeybee.WithWorkerLogLevel(slog.Level)`**
Overrides the minimum log level for worker-scoped records only.

### Per-connection

**`honeybee.WithDialer(types.Dialer)`**
Overrides the dialer for a single `Connect` call. Passed as a variadic option: `pool.Connect(id, honeybee.WithDialer(d))`. When provided, it takes precedence over the dialer resolved from `ConnectionConfig`. Existing callers that pass no options are unaffected.

### Wiring

**`honeybee.WithConnectionConfig(transport.ConnectionConfig)`**
Supplies a connection config used when dialing each peer. Accepted by value; the pool stores its own copy.

**`honeybee.WithWorkerConfig(honeybee.WorkerConfig)`**
Supplies a worker config applied to every worker the pool creates. Accepted by value; the pool stores its own copy.

**`honeybee.WithWorkerFactory(honeybee.WorkerFactory)`**
Replaces the default worker constructor. See [EXTEND.md](EXTEND.md) for the factory contract.
