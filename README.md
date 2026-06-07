# go-honeybee

WebSocket connection and pool primitives in Go. Built for Nostr.

## Library Map

```txt
pool.go                Pool, public types
session.go             SessionConfig

transport/             single-connection primitives and helpers
  connection.go          Connection, reader goroutine, pinger
  retry.go               exponential backoff with jitter
  socket.go              Dialer interface, DialWithRetry
  url.go                 parsing and normalization
  watchdog.go            IdleWatchdog helper

types/                 shared interfaces (Dialer, Socket, ReceivedMessage)
honeybeetest/          test helpers and mocks for consumers
```

## What This Library Does

Honeybee is a minimal, general-purpose WebSocket transport library.

- Client-Side: Manage a pool of outbound peer connections that reconnect automatically and surface lifecycle events.
- Server-Side: Wrap already-upgraded sockets in the connection primitive, which provides a ping-based heartbeat, automated read-loop, concurrent-safe writes, and classified disconnect errors.
- The same primitives may also be used directly on the client side when pool semantics are not needed: `transport.DialWithRetry` acquires a socket with configurable exponential backoff and jitter, and the resulting socket can be wrapped in `transport.NewConnection` for the same read-loop, heartbeat, and write primitives without pool semantics.

## What This Library Does Not Do

Honeybee is a pure transport layer, but it is also a deliberately simple one. Honeybee does not provide:


- interpretation of message content. All messages are treated equally.
- message queuing, buffering, prioritization, batching, or coalescing.
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

## Outbound Pool

The pool connects to peers by their URLs and keeps them connected. It reconnects automatically when a connection drops and proactively refreshes inactive connections.

```go
import "git.wisehodl.dev/jay/go-honeybee"

pool, err := honeybee.NewPool(ctx, nil, handler)
if err != nil { /* handle error or panic */ }
defer pool.Close()

if err := pool.Connect("wss://peer.example.com"); err != nil { /* handle error */ }

go func() {
    for msg := range pool.Inbox() {
        // msg.URL          is the normalized URL
        // msg.Data         is the payload
        // msg.ReceivedAt   is the timestamp
    }
}()

go func() {
    for ev := range pool.Events() {
        // ev.At             is the event timestamp
        // ev.Err            is non-nil for EventDialFailed
        switch ev.Kind {
        case honeybee.EventConnected:
        case honeybee.EventDisconnected:
        case honeybee.EventDialFailed:
        case honeybee.EventRetired:
        }
    }
}()

pool.Send("wss://peer.example.com", []byte("hello"))
```

**Usage Notes:**

 URLs are normalized by the pool. `wss://peer.example.com`, `wss://peer.example.com/`, and `WSS://Peer.Example.Com:443` all identify the same peer. `honeybee.NormalizeURL` is also available directly if you need to use the same URLs as keys elsewhere.

Every time a connection is established, `honeybee.EventConnected` is emitted. Every time a connection drops for any reason, `honeybee.EventDisconnected` is emitted. A peer that reconnects three times produces three Connected/Disconnected pairs.

Keepalive is configured via `honeybee.WithKeepaliveTimeout`. The session records a heartbeat on every inbound message, every successful send, and every received pong. If no heartbeats arrive before the keepalive timer fires, the connection is proactively disconnected and reconnected. When set to zero, keepalive is disabled.

After a disconnect, the session waits for `ReconnectDelay` before attempting the next connection. The default is 2 seconds. Set to zero in tests or when you need immediate reconnection.

`Send` returns `ErrConnectionUnavailable` during the gap between a disconnect and the next successful reconnect. Callers should wait for `EventConnected` before retrying and maintain their own write buffers if needed.

Each failed dial attempt emits `EventDialFailed` on `pool.Events()` with the dialer error in `ev.Err`. These do not stop the pool; it continues retrying according to the session's retry config. `EventDialFailed` is only sent when the peer fails to connect, not when dialing is stopped internally.

When a peer's retry policy is exhausted, the session deregisters itself from the pool and emits `EventRetired` on `pool.Events()`. The default `RetryConfig` sets `MaxRetries` to zero, which means infinite retries — so in default operation `EventRetired` is structurally unreachable. It will only appear if you explicitly configure a positive `MaxRetries` via `WithRetryConfig`.

## Server-Side Usage

### Connection

Use `transport.NewConnection` when your HTTP upgrade handler gives you an open socket.

```go
import "git.wisehodl.dev/jay/go-honeybee/transport"

// wsConn is a *websocket.Conn from your upgrade handler
conn, err := transport.NewConnection(ctx, wsConn, nil, handler)
if err != nil { /* handle error */ }
defer conn.Close()

go func() {
    for data := range conn.Incoming() { // data: []byte
        // process incoming messages
    }
}()

go func() {
    for err := range conn.Errors() {
        // log or handle disconnects / read errors
    }
}()

// Send can be called concurrently
conn.Send([]byte("hello"))
```

`Send` is safe for concurrent callers. `Close` is idempotent and safe to call from any goroutine.

When the reader exits, exactly one classified error reaches `Errors()` before the channel closes.

- `ErrPeerClosedClean` for normal closure
- `ErrPeerClosedUnexpected` for abnormal close codes
- `ErrReadError` for anything else

Pass an `slog.Handler` as the fourth argument to enable structured logging. Pass `nil` to disable it.

### IdleWatchdog

The watchdog helper detects clients that have gone silent. Wire activity signals from your `Incoming()` consumer, on each Send() call, and from the connection's `Heartbeat()` channel into it, and provide a callback to invoke when the timeout fires. Feeding all three sources means a client that neither sends or receives data but still responds to pings will not be considered idle. Since there is no reconnect loop on the server side, the typical callback is `conn.Close()`.

```go
import "time"

activity := make(chan struct{}, 1)

signal := func() {
    select {
    case activity <- struct{}{}:
    default:
    }
}

// Feed data messages:
go func() {
    for data := range conn.Incoming() {
        signal()
        // process data
    }
}()

// Feed sends:
func SendWithHeartbeat(conn *Connection, data []byte) error {
    err := conn.Send(data)
    if err != nil {
	    return err
    }
    signal()
    return nil
}

// Feed pong heartbeats:
go func() {
    for range conn.Heartbeat() {
        signal()
    }
}()

// Start the watchdog:
go transport.IdleWatchdog(ctx, activity, 30*time.Second, func() {
    conn.Close()
})
```

When no activity signal arrives within the timeout, `onTimeout` is called once and the watchdog exits. When `ctx` is cancelled, the watchdog exits without calling `onTimeout`. When the timeout is zero or negative, the watchdog drains activity signals and waits for `ctx` to be cancelled without ever firing.

## Bare Connection

Use `transport.DialWithRetry` and `transport.NewConnection` together when you need a single outbound connection without pool semantics — for example, a one-shot query or a standalone client.

First, acquire a socket. Then wrap it in a connection:

```go
import "git.wisehodl.dev/jay/go-honeybee/transport"

// Step 1: dial with retry
retryCfg, _ := transport.NewRetryConfig() // or configure via WithMaxRetries, etc.
mgr := transport.NewRetryManager(retryCfg)

onError := func(err error) {
    // called for each failed attempt; use to log or surface per-attempt errors
}

dialFn := func(ctx context.Context) (types.Socket, error) {
    socket, _, err := myDialer.DialContext(ctx, "wss://example.com", nil)
    return socket, err
}

socket, err := transport.DialWithRetry(ctx, mgr, dialFn, onError, nil)
if err != nil { /* context cancelled or retry policy exhausted */ }

// Step 2: wrap the socket
conn, err := transport.NewConnection(ctx, socket, nil, nil)
if err != nil { /* handle error */ }
defer conn.Close()

go func() {
    for data := range conn.Incoming() {
        // process incoming messages
    }
}()

go func() {
    for err := range conn.Errors() {
        // log or handle disconnects / read errors
    }
}()

// Send can be called concurrently
conn.Send([]byte("hello"))
```

Pass `nil` for `mgr` to disable retry — the first dial failure is then terminal. Pass `nil` for `onError` to ignore per-attempt errors. Context cancellation terminates the retry loop at any point.

For `RetryConfig` and `RetryManager` construction options, see CONFIG.md.

`Send` is safe for concurrent callers. `Close` is idempotent and safe to call from any goroutine.

When the reader exits, exactly one classified error reaches `Errors()` before the channel closes.

- `ErrPeerClosedClean` for normal closure
- `ErrPeerClosedUnexpected` for abnormal close codes
- `ErrReadError` for anything else

## Ping-Pong Heartbeats

Connections send periodic WebSocket ping frames and listen for the corresponding pong replies. A received pong registers as a heartbeat signal within the session.

Pong-derived heartbeats reset the keepalive timer alongside data messages and sends. A peer that sends no data but responds to pings will not be disconnected and reconnected by the keepalive mechanism.

The ping interval is configured via `transport.WithPingInterval` on the `transport.ConnectionConfig`. Import `git.wisehodl.dev/jay/go-honeybee/transport` to construct a `ConnectionConfig`, then wrap it in a `SessionConfig` via `honeybee.WithConnectionConfig`, and pass the `SessionConfig` to `NewPoolConfig` via `honeybee.WithSessionConfig`. You can also supply the `ConnectionConfig` directly to `transport.NewConnection`. The default is 20 seconds. Set to zero to disable pings entirely, in which case only data messages and outbound sends generate heartbeats.

## Statistics

Pools and connections expose counters and channel depths that can be sampled at any time. All values are snapshots; counters are monotonically increasing and are not reset between reconnects.

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
// peerStats.Session     — channel depths, processed/sent counts

// Bare connection (transport package)
connStats := conn.Stats() // conn is a *transport.Connection
// connStats.TotalReceived, connStats.TotalSent, connStats.TotalHeartbeats
```

## Configuration

All configuration is done through option functions applied at construction time. `PoolConfig` is the top-level scope and embeds a `SessionConfig`. `SessionConfig` embeds both a `ConnectionConfig` and a `RetryConfig`. Consumers configure connection and retry behavior through this chain. Logging is not config-controlled; pass an `slog.Handler` to pool, session, and connection constructors directly.

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

- `MockSocket` implements `honeybeetest.Socket` with pluggable function fields for every method, including `WriteControl` and `SetPongHandler`.
- `MockDialer` implements `honeybeetest.Dialer` with a pluggable `DialContextFunc`.
- `MockSlogHandler` captures `slog` records for assertions against log output. Child handlers produced via `WithAttrs` share the same record slice as the parent, so attributes added by the logging package appear on the correct records.
- `Eventually(t, condition, msg)` polls a condition until it holds or the test timeout expires.
- `Never(t, condition, msg)` asserts a condition never holds over a short window.
- `ExpectWrite(t, ch, msgType, data)` asserts the next write on the channel matches the expected type and payload.
- `ExpectIncoming(t, ch, data)` asserts the next received message matches.
- `AssertLogSequence(t, records, expected)` asserts that a slice of `ExpectedLog` values appears in order within a set of records, using forward-only matching and allowing gaps.
- `FindLogRecord(records, level, msgSnippet)` returns the first record matching the given level and message substring, or nil.
- `AssertAttributePresent(t, record, key, value)` checks that a specific structured attribute is present on a record and equal to the expected value.

Timer-driven paths use short real sleeps (tens of milliseconds). State-transition paths use `Eventually` and complete as soon as the condition is observed.
