# go-honeybee

WebSocket connection and pool primitives in Go. Built for Nostr.

## Library Map

```txt
honeybee.go            top-level re-exports and constructors

transport/             single-connection primitives
  connection.go          *Connection, state machine, reader goroutine, pinger
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

logging/               structured log construction
  logging.go             logger constructors, ForcedLevelHandler

types/                 shared interfaces (Dialer, Socket)
honeybeetest/          test helpers and mocks for consumers
````

## What This Library Does

Honeybee is a reliable and simple library for managing websocket connections and pools.

- Handles websocket connections and pools cleanly and safely.
- Provides two pools: one to manage outbound peers and another to manage inbound peers.
- Exposes statistics at the connection, worker, and pool levels.
- Exposes a means to replace the internal pool worker to inject custom extensions.

## What This Library Does Not Do

Honeybee is a pure transport layer, but it is also a deliberately simple one. Honeybee does not provide advanced features, relying on its extensibility features to allow you to customize it.

Honeybee does not provide:

- interpretation of message content. All messages are treated equally.
- message queuing, prioritization, batching, or coalescing.
- rate limiting, circuit breakers, token buckets, or adaptive throttling.
- broadcast, fanout, or any many-to-many message routing.
- compression strategies, prepared message caching, or encoding optimization.
- authentication, authorization, or session management above the transport.

These are specialized features that deserve robust implementations elsewhere as on-demand extensions rather than core features.

## Installation

```bash
go get git.wisehodl.dev/jay/go-honeybee
```

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

conn.Send([]byte("hello"))
```

The connection goes through four states: `StateDisconnected`, `StateConnecting`, `StateConnected`, `StateClosed`. Transitions are atomic and observable via `conn.State()`. Once closed, the connection should not be reused. Instead, construct a new one with the same URL and reconnect.

`Send` is safe for concurrent callers. `Close` is idempotent and safe to call from any goroutine.

When the reader exits, exactly one classified error reaches `Errors()` before the channel closes.

- `ErrPeerClosedClean` for normal closure
- `ErrPeerClosedUnexpected` for abnormal close codes
- `ErrReadError` for anything else

Consumers use this to decide whether the disconnect was expected. No other errors are sent by the connection.

Pass an `*slog.Logger` as the third argument to get structured logs. Pass nil to disable logging entirely.

### Inbound Pool

The inbound pool manages connections initiated by peers. The consumer accepts a WebSocket somewhere else and hands the resulting socket to the pool along with an ID.

Each pool requires a non-empty string ID. This ID is attached to all structured log records emitted by the pool, its workers, and their connections.

```go
pool, err := honeybee.NewInboundPool(ctx, "my-pool", nil, handler)
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
        // ev.At             is the event timestamp
        switch ev.Kind {
        case honeybee.InboundEventDisconnected:  // clean close
        case honeybee.InboundEventDroppedClose:  // peer closed with abnormal code
        case honeybee.InboundEventDroppedError:  // read error
        case honeybee.InboundEventEvictedPolicy: // inactivity timeout
        }
    }
}()

pool.Send(peerID, []byte("response"))
```

`Add`, `Replace`, and `Remove` do not emit events. Events are emitted only when a worker exits on its own, either when the peer closed the socket or it was determined to be inactive.

Use `Replace` if you need to swap the socket for a peer while keeping its ID. No events are emitted during this process.

The watchdog is configured via `WithInboundInactivityTimeout`. When set to zero, it is disabled. When set, the watchdog observes message traffic and disconnects the peer if no messages arrive within the configured duration. The watchdog is disabled by default, meaning connections persist until manually removed or remotely terminated.

### Outbound Pool

The outbound pool connects to peers by their URLs and keeps them connected. It reconnects automatically when a connection drops and proactively refreshes inactive connections.

Each pool requires a non-empty string ID. This ID is attached to all structured log records emitted by the pool, its workers, and their connections.

```go
pool, err := honeybee.NewOutboundPool(ctx, "my-pool", nil, handler)
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
        // ev.At             is the event timestamp
        switch ev.Kind {
        case honeybee.OutboundEventConnected:
        case honeybee.OutboundEventDisconnected:
        }
    }
}()

pool.Send("wss://peer.example.com", []byte("hello"))
```

URLs are normalized by the pool. `wss://peer.example.com`, `wss://peer.example.com/`, and `WSS://Peer.Example.Com:443` all identify the same peer. `honeybee.NormalizeURL` is also available directly if you need to use the same URLs as keys elsewhere.

Every time a connection is established, `OutboundEventConnected` is emitted. Every time a connection drops for any reason, `OutboundEventDisconnected` is emitted. A peer that reconnects three times produces three Connected/Disconnected pairs.

Keepalive is configured via `WithOutboundKeepaliveTimeout`. The worker records a heartbeat on every inbound message, every successful send, and every received pong. If no heartbeats arrive before the keepalive timer fires, the connection is proactively disconnected and reconnected. When set to zero, keepalive is disabled.

After a disconnect, the worker waits for `ReconnectDelay` before attempting the next connection. The default is 2 seconds. Set to zero in tests or when you need immediate reconnection.

`Send` returns `ErrConnectionUnavailable` during the gap between a disconnect and the next successful reconnect. Callers should wait for `OutboundEventConnected` before retrying and maintain their own write buffers if needed.

Dial failures are handled internally by the worker's retry logic and documented in structured logs. These do not stop the pool; it continues retrying according to the connection's retry config.

## Ping-Pong Heartbeats

Connections send periodic WebSocket ping frames and listen for the corresponding pong replies. A received pong registers as a heartbeat signal within the worker.

For inbound workers, pong-derived heartbeats reset the inactivity watchdog timer alongside data messages. A peer that sends no data but responds to pings will not be evicted.

For outbound workers, pong-derived heartbeats reset the keepalive timer. A peer that sends no data but responds to pings will not be disconnected and reconnected by the keepalive mechanism.

The ping interval is configured via `WithPingInterval` on the `ConnectionConfig`. The default is 20 seconds. Set to zero to disable pings entirely, in which case only data messages and outbound sends generate heartbeats.

## Statistics

Pools, workers, and connections expose counters and channel depths that can be sampled at any time. All values are snapshots; counters are monotonically increasing and are not reset between reconnects on an outbound worker.

```go
// Pool-level snapshot
stats := pool.Stats()
// stats.PeerCount       — number of currently registered peers
// stats.TotalReceived   — messages delivered to pool.Inbox() since construction
// stats.TotalSent       — messages sent via pool.Send() since construction
// stats.ChanInbox       — current depth of the inbox channel
// stats.PeerStats       — one entry per connected peer

// Single peer
peerStats, err := pool.PeerStats(peerID)
// peerStats.Worker      — queue depths, buffer depth, processed/dropped/sent counts
// peerStats.Connection  — channel depths, receive/send/heartbeat counts (inbound)

// Bare connection
connStats := conn.Stats()
// connStats.TotalReceived, connStats.TotalSent, connStats.TotalHeartbeats
```

## Extending Pools

The pool owns peer registration, event plumbing, and lifecycle. The worker owns what happens on the wire. The default worker can be replaced entirely or composed from the exported `Run*` building blocks that Honeybee provides.

See EXTEND.md for the worker interface contract, the `PoolPlugin` fields, and the available building blocks for both inbound and outbound pools.

## Configuration

All configuration is done through option functions applied at construction time. There are three config scopes: `ConnectionConfig`, `WorkerConfig`, and `PoolConfig`. Logging can be enabled and its minimum level overridden independently at the pool, worker, and connection levels.

See CONFIG.md for the full option reference and defaults table.

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

The `honeybeetest` package provides mocks and assertions for code that builds on Honeybee:

- `MockSocket` implements `types.Socket` with pluggable function fields for every method, including `WriteControl` and `SetPongHandler`.
- `MockDialer` implements `types.Dialer` with a pluggable `DialContextFunc`.
- `MockSlogHandler` captures `slog` records for assertions against log output. Child handlers produced via `WithAttrs` share the same record slice as the parent, so attributes added by the logging package appear on the correct records.
- `Eventually(t, condition, msg)` polls a condition until it holds or the test timeout expires.
- `Never(t, condition, msg)` asserts a condition never holds over a short window.
- `ExpectWrite(t, ch, msgType, data)` asserts the next write on the channel matches the expected type and payload.
- `ExpectIncoming(t, ch, data)` asserts the next received message matches.
- `AssertLogSequence(t, records, expected)` asserts that a slice of `ExpectedLog` values appears in order within a set of records, using forward-only matching and allowing gaps.
- `FindLogRecord(records, level, msgSnippet)` returns the first record matching the given level and message substring, or nil.
- `AssertAttributePresent(t, record, key, value)` checks that a specific structured attribute is present on a record and equal to the expected value.

Timer-driven paths use short real sleeps (tens of milliseconds). State-transition paths use `Eventually` and complete as soon as the condition is observed.
