# go-honeybee - WebSocket Connection Management for Go

Source: https://git.wisehodl.dev/jay/go-honeybee

Mirror: https://github.com/wisehodl/go-honeybee

## What this library does

`go-honeybee` provides reliable WebSocket connection primitives with automatic reconnection. It manages connection lifecycle, exponential backoff retry, and thread-safe message handling.

## What this library does not do

`go-honeybee` does not implement protocol-specific message parsing, subscription management, or connection pooling (yet). It provides the foundation for building protocol implementations (like Nostr clients) on top of stable WebSocket connections.

## Installation

Add `go-honeybee` to your project:

```bash
go get git.wisehodl.dev/jay/go-honeybee
```

If the primary repository is unavailable, use the `replace` directive in your go.mod:

```
replace git.wisehodl.dev/jay/go-honeybee => github.com/wisehodl/go-honeybee latest
```

## Usage

### Basic Connection

```go
import "git.wisehodl.dev/jay/go-honeybee"

// Create connection
conn, err := honeybee.NewConnection("wss://relay.example.com", nil)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// Connect with default retry disabled
if err := conn.Connect(); err != nil {
    log.Fatal(err)
}

// Handle messages
go func() {
    for msg := range conn.Incoming() {
        // Process incoming message
    }
}()

// Handle errors
go func() {
    for err := range conn.Errors() {
        log.Printf("Connection error: %v", err)
    }
}()

// Send message
conn.Send([]byte(`{"type":"subscribe"}`))
```

### Retry When Connecting

```go
config := &honeybee.Config{
    Retry: &honeybee.RetryConfig{
        MaxRetries:   0,                  // 0 = infinite
        InitialDelay: 1 * time.Second,
        MaxDelay:     30 * time.Second,
        JitterFactor: 0.5,
    },
}

conn, err := honeybee.NewConnection("wss://relay.example.com", config)
```

### Configuration Options

```go
config, err := honeybee.NewConfig(
    honeybee.WithRetry(),
    honeybee.WithReadTimeout(30 * time.Second),
    honeybee.WithWriteTimeout(10 * time.Second),
)

conn, err := honeybee.NewConnection("wss://relay.example.com", config)
```

## Connection States

Connections transition through these states:

- `StateDisconnected` - Initial state, no socket
- `StateConnecting` - Connection attempt in progress
- `StateConnected` - Active connection, messages flowing
- `StateClosed` - Connection terminated

Check current state with `conn.State()`.

## Testing

Run tests with:

```bash
go test ./...
```

Run with race detector:

```bash
go test -race ./...
```
