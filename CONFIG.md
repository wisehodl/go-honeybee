# Configuration

All configuration uses option functions applied at construction time. Validation occurs at constructor time; a config value returned by a constructor is guaranteed to be valid. `PoolConfig` is the outermost scope and contains a `WorkerConfig`. `WorkerConfig` contains a `ConnectionConfig` and a `RetryConfig`. `RetryConfig` validation is skipped entirely when `RetryDisabled` is set on `WorkerConfig`, making the zero value of `RetryConfig` safe in that case. Logging is not config-controlled; pass an `slog.Handler` to pool, worker, and connection constructors directly.

## Defaults

| Scope | Setting | Default | Disabled by | Notes |
|---|---|---|---|---|
| Pool | `InboxBufferSize` | 256 | — | Must be positive |
| Pool | `EventsBufferSize` | 10 | — | Must be positive |
| Pool | `RequestHeader` | `User-Agent: honeybee/0.1.0` | `WithRequestHeader(nil)` | Cloned per dial call |
| Worker | `KeepaliveTimeout` | 60s | `0` | 0 disables keepalive |
| Worker | `ReconnectDelay` | 2s | `0` | 0 means reconnect immediately |
| Worker | Retry | enabled | `WithRetryDisabled()` | Skips RetryConfig validation when disabled |
| Connection | `WriteTimeout` | 30s | `0` | Per-message write deadline |
| Connection | `PingInterval` | 20s | `0` | ±10% jitter applied per interval |
| Connection | `IncomingBufferSize` | 100 | — | Must be positive |
| Connection | `ErrorsBufferSize` | 10 | — | Must be positive |
| Retry | `MaxRetries` | 0 | — | 0 means infinite |
| Retry | `InitialDelay` | 1s | — | Must be positive |
| Retry | `MaxDelay` | 60s | — | Must be ≥ `InitialDelay` |
| Retry | `JitterFactor` | 0.2 | `0.0` | Range [0.0, 1.0] |

## Pool Options

These are passed to `honeybee.NewPoolConfig`. Import `git.wisehodl.dev/jay/go-honeybee`.

**`WithInboxBufferSize(int)`** — sets the capacity of the shared inbox channel. Must be at least 1.

**`WithEventsBufferSize(int)`** — sets the capacity of the events channel. Must be at least 1.

**`WithWorkerConfig(WorkerConfig)`** — supplies the worker configuration applied to every peer the pool manages. Accepted by value.

**`WithRequestHeader(http.Header)`** — sets HTTP headers sent during the WebSocket handshake for every dial. The pool clones the header at option application time, so subsequent mutations to the original do not affect dial behavior. Pass `nil` to send no custom headers.

**`WithDialFunc(func(context.Context) (types.Socket, error))`** — a per-call option passed to `pool.Connect`, not to `NewPoolConfig`. Overrides the pool's default dialer for a single connection. When provided, the pool's dialer and request header are not used for that connection.

## Worker Options

These are passed to `honeybee.NewWorkerConfig`. Import `git.wisehodl.dev/jay/go-honeybee`.

**`WithConnectionConfig(transport.ConnectionConfig)`** — supplies the connection-level configuration (timeouts, buffers, ping interval). Import `git.wisehodl.dev/jay/go-honeybee/transport` to construct one.

**`WithRetryConfig(transport.RetryConfig)`** — supplies the retry policy used when dialing. Has no effect when `WithRetryDisabled` is also applied.

**`WithRetryDisabled()`** — disables dial retry. The first dial failure is terminal for that connection attempt.

**`WithKeepaliveTimeout(duration)`** — enables the keepalive mechanism. When no heartbeat is observed within this duration, the connection is closed and a new dial is attempted. Set to zero to disable. Must not be negative.

**`WithReconnectDelay(duration)`** — sets the wait between a disconnect and the next dial attempt. The default prevents tight reconnect loops against unavailable peers. Set to zero in tests or when immediate reconnection is required. Must not be negative.

## Connection Options

These are passed to `transport.NewConnectionConfig`. Import `git.wisehodl.dev/jay/go-honeybee/transport`.

**`WithWriteTimeout(duration)`** — sets a per-message write deadline. Applied before every `WriteMessage` call. Set to zero to disable. Must not be negative.

**`WithPingInterval(duration)`** — sets the interval between WebSocket ping frames. A ±10% jitter is applied to each interval. Set to zero to disable pings entirely. Must not be negative.

**`WithIncomingBufferSize(int)`** — sets the capacity of the channel buffering inbound messages. Must be at least 1.

**`WithErrorsBufferSize(int)`** — sets the capacity of the channel carrying connection-level errors. Must be at least 1.

## Retry Options

These are passed to `transport.NewRetryConfig`. Import `git.wisehodl.dev/jay/go-honeybee/transport`. The resulting `RetryConfig` is then passed to `NewWorkerConfig` via `WithRetryConfig`.

**`WithMaxRetries(int)`** — caps the number of dial attempts after the first. Zero means retry indefinitely. Must not be negative.

**`WithInitialDelay(duration)`** — sets the delay before the second attempt. The first retry is always immediate. Must be positive.

**`WithMaxDelay(duration)`** — caps the exponential backoff delay. Must be positive and at least as large as `InitialDelay`.

**`WithJitterFactor(float64)`** — adds randomization to each backoff delay. A value of 0.2 varies the delay within ±10% of the computed base. Range: 0.0 to 1.0.
