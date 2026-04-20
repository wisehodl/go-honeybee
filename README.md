# go-honeybee

WebSocket connection and pool primitives for Go. Zero protocol awareness.

## Library Map

```txt
honeybee.go            top-level re-exports and constructors

transport/             single-connection primitives
  connection.go          *Connection, state machine, reader goroutine
  config.go              ConnectionConfig, RetryConfig, options
  retry.go               exponential backoff with jitter
  socket.go              Dialer interface, AcquireSocket
  url.go                 parsing and normalization

inbound/               pool for peer-initiated connections
  pool.go                Pool, Peer, event plumbing
  worker.go              Worker interface, DefaultWorker, Run* functions
  config.go              WorkerConfig, PoolConfig, options

outbound/              pool for self-initiated connections
  pool.go                Pool, Peer, event plumbing
  worker.go              Worker interface, DefaultWorker, Session, Run* functions
  config.go              WorkerConfig, PoolConfig, options

types/                 shared interfaces (Dialer, Socket)
honeybeetest/          test helpers and mocks for consumers
```

## What This Library Does

Honeybee is a reliable and simple library for managing websocket connections and pools.

- Handles websocket connections and pools cleanly and safely.
- When connecting, robustly retries failed attempts until a connection is achieved.
- Provides two pools: one to manage outbound peers and another to manage inbound peers.
- Exposes a means to replace the internal pool worker to inject custom extensions.

## What This Library Does Not Do

Honeybee is a pure transport layer, but it is also a deliberately simple one. Honeybee does not provide advanced features, relying on its extensibility features to allow you to customize it.

Honeybee does not provide:

- Interpret or modify message content. It moves exactly the data it receives.
- message queuing, prioritization, batching, or coalescing.
- rate limiting, circuit breakers, token buckets, or adaptive throttling.
- broadcast, fanout, or any many-to-many message routing.
- compression strategies, prepared message caching, or encoding optimization.
- authentication, authorization, or session management above the transport.

These are specialized features that deserve robust implementations, but not within Honeybee itself.

## Installation

```bash
go get git.wisehodl.dev/jay/go-honeybee
````

If the primary repository is unavailable, use the `replace` directive in your go.mod:

```
replace git.wisehodl.dev/jay/go-honeybee => github.com/wisehodl/go-honeybee latest
```

## Usage

### Bare Connection

A `Connection` wraps a single WebSocket. Use it directly when you need one socket and do not want pool semantics.

```go
conn, err := honeybee.NewConnection("wss://example.com", nil, nil)
if err != nil { /* handle error */ }

if err := conn.Connect(ctx); err != nil { /* handle error */ }
defer conn.Close()

go func() {
    for data := range conn.Incoming() { // data: []byte
        // process incoming messages
    }
}()

go func() {
    for err := range conn.Errors() {
        // log or handle
    }
}()

// send a message
conn.Send([]byte("hello"))
```

The connection goes through four states: `StateDisconnected`, `StateConnecting`, `StateConnected`, `StateClosed`. Transitions are atomic and observable via `conn.State()`. Once closed, the connection should not be reused. Instead, construct a new one with the same `url` and reconnect.

`Send` is safe for concurrent callers. `Close` is idempotent and safe to call from any goroutine.

When the reader exits, exactly one classified error reaches `Errors()` before the channel closes.

- `ErrPeerClosedClean` for normal closure
- `ErrPeerClosedUnexpected` for abnormal close codes
- `ErrReadError` for anything else.

Consumers use this to decide whether the disconnect was expected. No other errors are sent by the connection.

Pass an `*slog.Logger` as the third argument to get structured logs at INFO, WARN, and ERROR levels. Pass nil to disable logging entirely.

### Inbound Pool

The inbound pool manages connections initiated by peers. The consumer accepts a WebSocket somewhere else and hands the resulting socket to the pool along with an ID.

```go
pool, err := honeybee.NewInboundPool(ctx, nil, logger)
if err != nil { /* handle error */ }
defer pool.Close()

// When a peer connects at the HTTP layer:
if err := pool.Add(peerID, socket); err != nil { /* handle error */ }

// Consume inbound data from all peers on one channel:
go func() {
    for msg := range pool.Inbox() {
        // msg.ID            identifies the peer
		// msg.Data          is the payload
        // msg.ReceivedAt    is the timestamp
    }
}()

// React to peer lifecycle:
go func() {
    for ev := range pool.Events() {
        switch ev.Kind {
        case honeybee.InboundEventDisconnected: // clean close
        case honeybee.InboundEventDropped:      // unexpected drop or read error
        case honeybee.InboundEventEvicted:      // inactivity timeout, if enabled
        }
    }
}()

// send a message to a specific peer
pool.Send(peerID, []byte("response"))
```

`Add`, `Replace`, and `Remove` do not emit events. Events are emitted only when a worker exits on its own, either when the peer closed the socket or it was determined to be inactive (inactivity monitoring is disabled by default).

Use `Replace` if you need to replace a socket for a peer and maintain its ID. No events are emitted during this process.

The watchdog is configured via `WithInboundInactivityTimeout`. When set to zero, it is disabled. When set, the watchdog will observe message traffic on the wire and disconnect if no messages are seen for the configured duration. The watchdog is disabled by default, meaning that connections will persist until manually removed or remotely terminated.

### Outbound Pool

The outbound pool connects to peers by their URLs and keeps them connected. It reconnects automatically when a connection drops and proactively refreshes inactive connections.

```go
pool, err := honeybee.NewOutboundPool(ctx, nil, logger)
if err != nil { /* handle error */ }
defer pool.Close()

if err := pool.Connect("wss://peer.example.com"); err != nil { /* handle error */ }

go func() {
    for msg := range pool.Inbox() {
        // msg.ID            is the normalized URL
		// msg.Data          is the payload
        // msg.ReceivedAt    is the timestamp
    }
}()

go func() {
    for ev := range pool.Events() {
	    // used to determine when a connection is live
        switch ev.Kind {
        case honeybee.OutboundEventConnected:
        case honeybee.OutboundEventDisconnected:
        }
    }
}()

// send a message to a specific peer
pool.Send("wss://peer.example.com", []byte("hello"))
```

URLs are normalized by the pool. For example: `wss://peer.example.com`, `wss://peer.example.com/`, and `WSS://Peer.Example.Com:443` all identify the same peer.

Every time a connection is established, `OutboundEventConnected` is emitted. Every time a connection drops for any reason, `OutboundEventDisconnected` is emitted. A peer that reconnects three times produces three Connected/Disconnected pairs.

Keepalive is configured via `WithOutboundKeepaliveTimeout`. The worker records a heartbeat on every inbound message and every successful send. If no heartbeats come in before the keepalive timer runs out, the connection is proactively disconnected and reconnected. When set to zero, the keepalive mechanism is disabled.

`Send` returns `ErrConnectionUnavailable` during the gap between a disconnect and the next successful reconnect. Callers should try again after observing an `OutboundEventConnected` event and maintain their own write buffers.

Dial failures surface on `pool.Errors()`. These do not stop the pool though. It will continue retrying according to the connection's retry config and the keepalive mechanism.

## Extensibility

The pool owns peer registry, event plumbing, and lifecycle. The worker owns what happens on the wire. Everything between `pool.Add` or `pool.Connect` and the `InboundEventDisconnected`/`OutboundEventDisconnected` event is the worker's responsibility, and it is fully replaceable.

### The Worker Interface

Both pools accept any type implementing:

```go
type Worker interface {
    Start(pool PoolPlugin)
    Stop()
    Send(data []byte) error
}
```

`PoolPlugin` differs slightly between inbound and outbound, giving the worker access to the pool's inbox channel, events channel, logger, and (for inbound) an `OnExit` callback. The pool calls `Start` in a goroutine it owns and expects `Start` to return when the worker is done.

### The Factory Pattern

Workers are constructed by factories injected into the pool config:

```go
config, _ := honeybee.NewInboundPoolConfig(
    honeybee.WithInboundWorkerFactory(myFactory),
)
```

Factories run under the pool's write lock during `Add` or `Connect`, so they must be non-blocking. Anything requiring I/O belongs inside `Start`, not inside the factory.

### Three Levels of Customization

Most will want level one. A few may want level two. Level three is there when you need it.

**Level 1: Use the default worker.** It handles everything described in the usage sections. No factory needed.

**Level 2: Compose the exported `Run*` functions.** Honeybee exports the building blocks the default workers are built from:

- Inbound: `RunReader`, `RunForwarder`, `RunWatchdog`.
- Outbound: `RunDialer`, `RunKeepalive`, `RunForwarder`, `Session`, `RunReader`, `RunStopMonitor`.

A custom worker can reuse most of these and replace one. For example, an inbound worker that wants to tag every message with a receive sequence number before forwarding can reuse `RunReader` and `RunWatchdog` verbatim and write a custom forwarder that wraps the default behavior:

```go
type SequencedWorker struct {
    *inbound.DefaultWorker
    seq atomic.Uint64
}

func (w *SequencedWorker) Start(pool inbound.PoolPlugin) {
    // wrap pool.Inbox with a channel that tags messages,
    // call w.DefaultWorker.Start with the wrapped plugin
}
```

**Level 3: Implement `Worker` from scratch.** The contract is minimal:

1. `Start` runs until the worker is done, then returns. The pool handles its
   own waitgroup to monitor each worker.
2. `Stop` causes `Start` to return in bounded time. Typically this cancels a context.
3. `Send` writes data and returns an error if it cannot. It is called from arbitrary goroutines and must be safe for concurrent use.
4. For inbound workers, call `pool.OnExit(kind)` exactly once when the worker exits on its own (not in response to `Stop`). The pool wraps this in `sync.Once` defensively, but a well-behaved worker calls it once.
5. Forward received bytes to `pool.Inbox` as `InboxMessage` values. Emit events by letting the pool do it through `OnExit`; the worker does not touch `pool.Events` directly.

The pool will not retry a failed factory call, will not rescue a worker whose `Start` blocks forever, and will not interpret the errors `Send` returns.

## Configuration

Three config types cover three scopes.

`ConnectionConfig` governs a single connection's behavior: write timeout, close handler, and retry policy. `RetryConfig` is embedded inside it and governs the `Connect()` retry loop.

`WorkerConfig` governs a single worker's behavior. Inbound and outbound each have their own, with fields specific to their direction.

`PoolConfig` bundles a connection config, a worker config, and an optional worker factory. It is a thin container.

### Option Functions

Connection and retry:

- `WithoutRetry()` disables retry entirely.
- `WithRetryMaxRetries(int)` caps retry attempts; zero means infinite.
- `WithRetryInitialDelay(duration)` sets the first backoff interval.
- `WithRetryMaxDelay(duration)` caps the backoff interval.
- `WithRetryJitterFactor(float64)` adds randomization to backoff, range 0.0 to 1.0.
- `WithWriteTimeout(duration)` sets per-message write deadline.
- `WithCloseHandler(func)` installs a close handler on the socket.

Inbound worker:

- `WithInboundInactivityTimeout(duration)` enables the watchdog.
- `WithInboundMaxQueueSize(int)` bounds the forwarder's internal queue.

Outbound worker:

- `WithOutboundKeepaliveTimeout(duration)` enables keepalive.
- `WithOutboundMaxQueueSize(int)` bounds the forwarder's internal queue.

Pool wiring (both directions have inbound and outbound variants):

- `With{Inbound,Outbound}ConnectionConfig(*ConnectionConfig)`
- `With{Inbound,Outbound}WorkerConfig(*WorkerConfig)`
- `With{Inbound,Outbound}WorkerFactory(WorkerFactory)`

All option functions validate their inputs. Invalid values return errors at application time. Configs constructed via `NewXConfig` are validated at construction and cannot be saved in an invalid state.

### Defaults

| Setting                      | Default | Disabled Value   | Notes                           |
| ---------------------------- | ------- | ---------------- | ------------------------------- |
| `WriteTimeout`               | 30s     | `0`              | Per-message write deadline      |
| `Retry` enabled              | yes     | `WithoutRetry()` | Applies to `Connect()` only     |
| `Retry.MaxRetries`           | `0`     | —                | `0` means infinite              |
| `Retry.InitialDelay`         | 1s      | —                | Must be positive                |
| `Retry.MaxDelay`             | 5s      | —                | Must be at least `InitialDelay` |
| `Retry.JitterFactor`         | 0.5     | `0.0`            | Range [0.0, 1.0]                |
| Inbound `MaxQueueSize`       | `0`     | `0`              | `0` means unbounded             |
| Inbound `InactivityTimeout`  | `0`     | `0`              | `0` disables watchdog           |
| Outbound `KeepaliveTimeout`  | 20s     | `0`              | `0` disables keepalive          |
| Outbound `MaxQueueSize`      | `0`     | `0`              | `0` means unbounded             |
| Connection inbox buffer      | 100     | —                | Not configurable                |
| Connection errors buffer     | 10      | —                | Not configurable                |

## Testing

Run the full suite:

```bash
go test ./...
```

Run with the race detector; the suite is race-clean:

```bash
go test -race ./...
```

### Test Helpers for Consumers

The `honeybeetest` package provides mocks and assertions for code that builds on honeybee:

- `MockSocket` implements `types.Socket` with pluggable function fields for every method.
- `MockDialer` implements `types.Dialer` with a pluggable `DialContextFunc`.
- `MockSlogHandler` captures `slog` records for assertions against log output.
- `Eventually(t, condition, msg)` polls a condition until it holds or the test timeout expires.
- `Never(t, condition, msg)` asserts a condition never holds over a short window.
- `ExpectWrite(t, ch, msgType, data)` asserts the next write matches.
- `ExpectIncoming(t, ch, data)` asserts the next received message matches.

Timer-driven paths use short real sleeps (tens of milliseconds). State-transition paths use `Eventually` and complete as soon as the state is observed.
