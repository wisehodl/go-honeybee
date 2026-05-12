# Extending Pools

The pool owns peer registration, event plumbing, and lifecycle management. The worker owns everything that happens on the wire between registration and the terminal event. Everything between `pool.Add` or `pool.Connect` and the final disconnect event is the worker's responsibility, and it is fully replaceable.

## The Worker Interface

Both pools accept any type that satisfies:

```go
type Worker interface {
    Start(pool PoolPlugin)
    Stop()
    Send(data []byte) error
    Stats() WorkerStats
}
````

The behavioral contract for each method:

**`Start(pool PoolPlugin)`** Called by the pool in a goroutine it owns. Must block until the worker is finished. The pool monitors this goroutine via a `sync.WaitGroup`; `Start` returning is the signal that the worker is done. All I/O, goroutine management, and event emission happen inside `Start`.

**`Stop()`** Must cause `Start` to return in bounded time. Typically cancels a context. May be called from any goroutine, including concurrently with `Start`.

**`Send(data []byte) error`** Writes data to the remote peer and returns an error if it cannot. Must be safe for concurrent callers. The pool calls `Send` from whatever goroutine the consumer calls `pool.Send` from.

**`Stats() WorkerStats`** Returns a snapshot of the worker's internal counters and channel depths. Must be safe for concurrent callers and must not block. The pool calls this from `pool.Stats()` and `pool.PeerStats()` while holding a read lock.

## The PoolPlugin

The pool constructs a `PoolPlugin` and passes it to `Start`. It gives the worker access to pool-level channels and the logging handler.

```go
type PoolPlugin struct {
    Inbox        chan<- InboxMessage
    Events       chan<- PoolEvent
    InboxCounter *atomic.Uint64
    OnExit       OnExitFunction  // inbound only
    Handler      slog.Handler
}
```

**`Inbox`** The shared channel that delivers received messages to the pool's consumer. All peers in the pool deliver to the same inbox channel. Workers must include their peer ID in each `InboxMessage`.

**`Events`** The shared channel for lifecycle events. Outbound workers emit `EventConnected` and `EventDisconnected` directly. Inbound workers do not touch this channel directly; instead they call `OnExit`, and the pool translates the exit kind into the appropriate event. All events include a timestamp in the `At` field.

**`InboxCounter`** An atomic counter owned by the pool. Workers must increment this once for each message forwarded to `Inbox`. The pool reads it in `Stats()`.

**`OnExit` (inbound only)** Must be called exactly once when an inbound worker exits on its own initiative — that is, when the peer closed the connection or the watchdog fired, not when `Stop` was called. The pool wraps the underlying function in a `sync.Once` as a safety net, but a well-behaved worker calls it once. Calling `OnExit` removes the peer from the pool's registry and emits the appropriate event.

**`Handler`** The `slog.Handler` passed to the pool constructor. The pool constructs and injects scoped loggers before calling the factory, so most workers will use the logger they receive from the factory rather than constructing one from `Handler` directly. `Handler` is available for workers that manage sub-components that need their own loggers.

## Extending the Inbound Pool

### Factory Signature

```go
type WorkerFactory func(
    ctx    context.Context,
    id     string,
    conn   *transport.Connection,
    config *WorkerConfig,
    logger *slog.Logger,
) (Worker, error)
```

The pool calls the factory under its write lock when `Add` or `Replace` is called. The factory must return without blocking. The pool constructs `logger` from the worker logging config before calling the factory; pass it to your worker or ignore it.

The factory is set via `WithInboundWorkerFactory` on the pool config.

### Building Blocks

The default inbound worker is assembled from these exported functions. Each can be reused, replaced, or wrapped independently.

**`RunReader(ctx, onExit, conn, messages, heartbeat, logger)`** Reads from `conn.Incoming()` until the channel closes or `ctx` is cancelled. Forwards each message to `messages` and sends a signal on `heartbeat` for each message received. When the channel closes, classifies the exit — clean disconnect, unexpected close, or read error — and calls `onExit` with the appropriate `WorkerExitKind`. Does not call `onExit` on context cancellation.

**`RunHeartbeatForwarder(ctx, conn, heartbeat, logger)`** Reads from `conn.Heartbeat()` and forwards each signal to `heartbeat`. This propagates pong replies from the connection layer into the worker's heartbeat channel, so pongs reset the inactivity watchdog alongside data messages.

**`RunQueue(id, ctx, in, out, maxQueueSize, droppedCount, bufferDepth)`** Buffers messages between the reader and the forwarder. When `maxQueueSize` is positive and the queue is full, the oldest message is dropped and `droppedCount` is incremented. When `maxQueueSize` is zero, the queue grows without bound. `bufferDepth` is an `*atomic.Int64` maintained as the current number of items in the queue.

**`RunForwarder(id, ctx, messages, inbox, workerProcessedCount, poolInboxCount)`** Reads from `messages` and writes to `inbox`. Increments both counters on each successful delivery.

**`RunWatchdog(ctx, onInactive, heartbeat, timeout, logger)`** Monitors `heartbeat`. Resets a timer on each signal. When the timer fires, calls `onInactive(ExitPolicy)`. When `timeout` is zero, the watchdog is disabled: it drains `heartbeat` without acting and exits when `ctx` is cancelled.

### Replacement Patterns

**Swap one block.** Embed `*inbound.DefaultWorker`, override `Start`, reuse the goroutines you want unchanged, and substitute your own implementation for the one you are replacing. For example, to tag every forwarded message with a sequence number, reuse `RunReader`, `RunQueue`, and `RunWatchdog` verbatim, and write a custom forwarder.

**Wrap the plugin.** Intercept `Inbox` by substituting a channel you own, process messages, then forward to the original. The worker signature is unchanged; you compose behavior by controlling what the building blocks see.

**Implement from scratch.** Satisfy the `Worker` interface directly. The only obligations are the behavioral contracts on `Start`, `Stop`, `Send`, `Stats`, and `OnExit` described above.

## Extending the Outbound Pool

### Factory Signature

```go
type WorkerFactory func(
    ctx    context.Context,
    id     string,
    logger *slog.Logger,
) (Worker, error)
```

The pool calls the factory under its write lock when `Connect` is called. The factory must return without blocking. Note that the outbound factory does not receive a `*transport.Connection`; the worker is responsible for dialing and managing its own connections. The pool constructs `logger` from the worker logging config before calling the factory.

The factory is set via `WithOutboundWorkerFactory` on the pool config.

### Building Blocks

**`RunDialer(id, ctx, pool, dial, newConn, logger)`** Listens on `dial` for connection requests. On each signal, calls `connect` to dial a new `*transport.Connection`. While a dial is in progress, drains additional `dial` signals so that at most one dial runs at a time. On failure, logs the error and waits for the next `dial` signal. On success, sends the connection on `newConn`. Exits when `ctx` is cancelled.

**`RunKeepalive(ctx, heartbeat, keepalive, timeout, logger)`** Monitors `heartbeat`. Resets a timer on each signal. When the timer fires, sends a signal on `keepalive` to notify the session that the connection should be replaced. When `timeout` is zero, keepalive is disabled: it drains `heartbeat` without acting and exits when `ctx` is cancelled.

**`RunReader(ctx, onStop, conn, messages, heartbeat, logger)`** Reads from `conn.Incoming()` until the channel closes or `ctx` is cancelled. Forwards each message to `messages` and sends a signal on `heartbeat`. On exit, calls `conn.Close()` and then `onStop`.

**`RunHeartbeatForwarder(ctx, conn, heartbeat, logger)`** Reads from `conn.Heartbeat()` and forwards each signal to `heartbeat`. Propagates pong replies into the worker's heartbeat channel so pongs reset the keepalive timer alongside data messages and sends.

**`RunStopMonitor(ctx, onStop, conn, keepalive, logger)`** Waits for either `ctx.Done` or a signal on `keepalive`. On either, calls `conn.Close()` and then `onStop`. This is how a keepalive expiry propagates into a session tear-down.

**`RunForwarder(id, ctx, messages, inbox, workerProcessedCount, poolInboxCount)`** Reads from `messages` and writes to `inbox`. Increments both counters on each successful delivery. Identical in behavior to the inbound variant.

**`Session`** The coordination struct that ties the above blocks together for one connection lifecycle. `Session.Start` runs a loop: request a dial, wait for a connection, run `RunReader`, `RunHeartbeatForwarder`, and `RunStopMonitor` concurrently, wait for them to finish, emit `EventDisconnected`, sleep for `ReconnectDelay`, then repeat. `Session` is exported so it can be embedded or used directly in a custom worker.

### Replacement Patterns

**Swap one block.** The most common case is replacing `RunReader` to intercept or annotate inbound messages, or replacing `RunKeepalive` with a different activity metric. Reuse `Session` for the connection lifecycle and substitute the one goroutine you need to change.

**Replace the session loop.** Construct your own loop using `RunDialer`, `RunKeepalive`, and the session-level blocks. This gives you control over reconnection logic, back-off behavior, or multi-connection strategies while keeping the lower-level I/O blocks intact.

**Implement from scratch.** Satisfy the `Worker` interface directly. You are responsible for dialing, managing connection state, forwarding messages to `pool.Inbox`, emitting `EventConnected` and `EventDisconnected` to `pool.Events`, and incrementing `pool.InboxCounter`.

## Factory Constraints

Factories for both pool types are called while the pool holds its write lock. Two constraints follow from this directly.

**Factories must not block.** Any operation that could wait — dialing a connection, acquiring another lock, reading from a channel — will deadlock or stall the pool. All blocking work belongs inside `Start`, not inside the factory.

**Factories must not call pool methods.** `pool.Add`, `pool.Send`, `pool.Remove`, and similar methods all acquire the same lock the factory is called under. Calling them from the factory will deadlock.

The factory's only job is to construct and return a worker. If construction itself can fail — for example, because a config value is invalid — return the error and the pool will propagate it to the caller of `Add` or `Connect`.
