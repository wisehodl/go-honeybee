# Extending the Pool

The pool owns peer registration, event plumbing, and lifecycle management. The worker owns everything that happens on the wire between registration and the terminal event. Everything between `pool.Connect` and the final disconnect event is the worker's responsibility, and it is fully replaceable.

## The Worker Interface

The pool accepts any type that satisfies:

```go
type Worker interface {
    Start(pool PoolPlugin)
    Stop()
    Send(data []byte) error
    Stats() WorkerStats
}
```

The behavioral contract for each method:

**`Start(pool PoolPlugin)`** Called by the pool in a goroutine it owns. Must block until the worker is finished. The pool monitors this goroutine via a `sync.WaitGroup`; `Start` returning is the signal that the worker is done. All I/O, goroutine management, and event emission happen inside `Start`.

**`Stop()`** Must cause `Start` to return in bounded time. Typically cancels a context. May be called from any goroutine, including concurrently with `Start`.

**`Send(data []byte) error`** Writes data to the remote peer and returns an error if it cannot. Must be safe for concurrent callers. The pool calls `Send` from whatever goroutine the consumer calls `pool.Send` from.

**`Stats() WorkerStats`** Returns a snapshot of the worker's internal counters and channel depths. Must be safe for concurrent callers and must not block. The pool calls this from `pool.Stats()` and `pool.PeerStats()` while holding a read lock.

## The PoolPlugin

The pool constructs a `PoolPlugin` and passes it to `Start`. It gives the worker access to pool-level channels and the logging handler.

```go
type PoolPlugin struct {
    ID               string
    Inbox            chan<- honeybee.InboxMessage
    Events           chan<- honeybee.PoolEvent
    InboxCounter     *atomic.Uint64
    Dialer           honeybee.Dialer
    ConnectionConfig *transport.ConnectionConfig
    Handler          slog.Handler
}
```

**`Inbox`** The shared channel that delivers received messages to the pool's consumer. All peers in the pool deliver to the same inbox channel. Workers must include their peer ID in each `InboxMessage`.

**`Events`** The shared channel for lifecycle events. Workers emit `honeybee.EventConnected` and `honeybee.EventDisconnected` directly. All events include a timestamp in the `At` field.

**`InboxCounter`** An atomic counter owned by the pool. Workers must increment this once for each message forwarded to `Inbox`. The pool reads it in `Stats()`.

**`Handler`** The `slog.Handler` passed to the pool constructor. The pool constructs and injects scoped loggers before calling the factory, so most workers will use the logger they receive from the factory rather than constructing one from `Handler` directly. `Handler` is available for workers that manage sub-components that need their own loggers.

## Extending the Pool

### Factory Signature

```go
type honeybee.WorkerFactory func(
    ctx    context.Context,
    id     string,
    logger *slog.Logger,
) (honeybee.Worker, error)
```

The pool calls the factory under its write lock when `Connect` is called. The factory must return without blocking. The worker is responsible for dialing and managing its own connections. The pool constructs `logger` from the worker logging config before calling the factory.

The factory is set via `honeybee.WithWorkerFactory` on the pool config.

### Building Blocks

**`RunDialer(id, ctx, pool, dial, newConn, logger)`** Listens on `dial` for connection requests. On each signal, calls `connect` to dial a new `*transport.Connection`. While a dial is in progress, drains additional `dial` signals so that at most one dial runs at a time. On failure, logs the error and waits for the next `dial` signal. On success, sends the connection on `newConn`. Exits when `ctx` is cancelled.

**`RunKeepalive(ctx, heartbeat, keepalive, timeout, logger)`** Monitors `heartbeat`. Resets a timer on each signal. When the timer fires, sends a signal on `keepalive` to notify the session that the connection should be replaced. When `timeout` is zero, keepalive is disabled: it drains `heartbeat` without acting and exits when `ctx` is cancelled.

**`RunReader(id, ctx, onStop, conn, inbox, heartbeat, logger)`** Reads from `conn.Incoming()` until the channel closes or `ctx` is cancelled. Builds an `InboxMessage` inline with the peer ID and writes it directly to `inbox`. Sends a signal on `heartbeat` for each message. On exit, calls `conn.Close()` and then `onStop`.

**`RunHeartbeatForwarder(ctx, conn, heartbeat, logger)`** Reads from `conn.Heartbeat()` and forwards each signal to `heartbeat`. Propagates pong replies into the worker's heartbeat channel so pongs reset the keepalive timer alongside data messages and sends.

**`RunStopMonitor(ctx, onStop, conn, keepalive, logger)`** Waits for either `ctx.Done` or a signal on `keepalive`. On either, calls `conn.Close()` and then `onStop`. This is how a keepalive expiry propagates into a session tear-down.

**`Session`** The coordination struct that ties the above blocks together for one connection lifecycle. `Session.Start` runs a loop: request a dial, wait for a connection, run `RunReader`, `RunHeartbeatForwarder`, and `RunStopMonitor` concurrently, wait for them to finish, emit `EventDisconnected`, sleep for `ReconnectDelay`, then repeat. `Session` is exported so it can be embedded or used directly in a custom worker.

### Replacement Patterns

**Swap one block.** The most common case is replacing `RunReader` to intercept or annotate inbound messages, or replacing `RunKeepalive` with a different activity metric. Reuse `Session` for the connection lifecycle and substitute the one goroutine you need to change.

**Replace the session loop.** Construct your own loop using `RunDialer`, `RunKeepalive`, and the session-level blocks. This gives you control over reconnection logic, back-off behavior, or multi-connection strategies while keeping the lower-level I/O blocks intact.

**Implement from scratch.** Satisfy the `Worker` interface directly. You are responsible for dialing, managing connection state, forwarding messages to `pool.Inbox`, emitting `honeybee.EventConnected` and `honeybee.EventDisconnected` to `pool.Events`, and incrementing `pool.InboxCounter`.

## Factory Constraints

The factory is called while the pool holds its write lock. Two constraints follow from this directly.

**Factories must not block.** Any operation that could wait — dialing a connection, acquiring another lock, reading from a channel — will deadlock or stall the pool. All blocking work belongs inside `Start`, not inside the factory.

**Factories must not call pool methods.** `pool.Connect`, `pool.Send`, `pool.Remove`, and similar methods all acquire the same lock the factory is called under. Calling them from the factory will deadlock.

The factory's only job is to construct and return a worker. If construction itself can fail — for example, because a config value is invalid — return the error and the pool will propagate it to the caller of `Connect`.
