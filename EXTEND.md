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
    ConnectionConfig transport.ConnectionConfig
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

### Replacing the Worker

Satisfy the `Worker` interface and register your implementation via `honeybee.WithWorkerFactory`. Your worker is responsible for the full connection lifecycle: dialing and redialing, managing connection state, forwarding received messages to `pool.Inbox`, emitting `EventConnected` and `EventDisconnected` to `pool.Events`, and incrementing `pool.InboxCounter` for each message forwarded.

`DefaultWorker`'s source is the authoritative reference for how those responsibilities are met.

## Factory Constraints

The factory is called while the pool holds its write lock. Two constraints follow from this directly.

**Factories must not block.** Any operation that could wait — dialing a connection, acquiring another lock, reading from a channel — will deadlock or stall the pool. All blocking work belongs inside `Start`, not inside the factory.

**Factories must not call pool methods.** `pool.Connect`, `pool.Send`, `pool.Remove`, and similar methods all acquire the same lock the factory is called under. Calling them from the factory will deadlock.

The factory's only job is to construct and return a worker. If construction itself can fail — for example, because a config value is invalid — return the error and the pool will propagate it to the caller of `Connect`.
