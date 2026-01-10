# durable-streams-with-ralph

A Go (Golang) port of [durable-streams](https://github.com/durable-streams/durable-streams).

## About

Durable Streams is an open protocol for reliable, real-time data synchronization to client applications. It provides HTTP-based streaming with offset-based resumability, solving data loss issues that occur with traditional WebSocket and SSE connections when clients disconnect.

### Key Features

- **Offset-based resumption**: Clients can reconnect and resume from their last known position
- **CDN-friendly**: Offset-based URLs enable aggressive caching
- **Exactly-once semantics**: No data loss or duplication
- **Content-type agnostic**: Works with text, JSON, binary, and custom formats

## Installation

```bash
go get github.com/jilio/durable-streams-with-ralph
```

## Quick Start

### Server

```go
package main

import (
    "log"
    "net/http"
    "time"

    "github.com/jilio/durable-streams-with-ralph/server"
    "github.com/jilio/durable-streams-with-ralph/stream"
)

func main() {
    storage := stream.NewMemoryStorage()
    srv := server.NewWithOptions(storage, "", 30*time.Second)

    log.Println("Starting server on :8080")
    http.ListenAndServe(":8080", srv)
}
```

### Client - Producing Messages

```go
package main

import (
    "context"
    "log"

    "github.com/jilio/durable-streams-with-ralph/client"
)

func main() {
    ctx := context.Background()
    c := client.New("http://localhost:8080/my/stream",
        client.WithContentType("application/json"))

    // Create stream if needed
    c.Create(ctx, client.CreateOptions{
        ContentType: "application/json",
    })

    // Append a message
    offset, err := c.AppendJSON(ctx, map[string]string{
        "event": "user_signup",
        "email": "user@example.com",
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Message appended, next offset: %s", offset)
}
```

### Client - Consuming Messages

```go
package main

import (
    "context"
    "log"

    "github.com/jilio/durable-streams-with-ralph/client"
    "github.com/jilio/durable-streams-with-ralph/stream"
)

func main() {
    ctx := context.Background()
    c := client.New("http://localhost:8080/my/stream")

    // Subscribe from the beginning
    sub := c.Subscribe(ctx, client.SubscribeOptions{
        Offset: stream.StartOffset, // "-1" for start, "now" for tail
    })

    // Process messages
    for result := range sub.Messages {
        for _, msg := range result.Messages {
            log.Printf("Received: %s", string(msg.Data))
        }
        log.Printf("Current offset: %s", sub.Offset())
    }

    if err := sub.Err(); err != nil {
        log.Printf("Subscription error: %v", err)
    }
}
```

### Idempotent Producer with Batching

For high-throughput scenarios, use the IdempotentProducer:

```go
package main

import (
    "log"

    "github.com/jilio/durable-streams-with-ralph/client"
)

func main() {
    c := client.New("http://localhost:8080/my/stream",
        client.WithContentType("application/json"))

    producer := client.NewIdempotentProducer(c, "my-producer-id",
        client.WithLingerMs(10),       // Batch window
        client.WithMaxBatchBytes(1000), // Max batch size
    )
    defer producer.Close()

    // Fire-and-forget appends - batched automatically
    for i := 0; i < 100; i++ {
        producer.AppendJSON(map[string]int{"index": i})
    }

    // Ensure all messages are sent
    if err := producer.Flush(); err != nil {
        log.Fatal(err)
    }
}
```

## Running the Examples

### Start the Server

```bash
go run ./cmd/server/main.go -port 8080 -timeout 30s
```

### Run the Example Client

```bash
# Consume messages (waits for new messages)
go run ./examples/client/main.go -mode consume -offset "-1"

# Produce 10 messages
go run ./examples/client/main.go -mode produce -count 10

# Both produce and consume concurrently
go run ./examples/client/main.go -mode both
```

## API Reference

### Stream Operations

| Method | Path | Description |
|--------|------|-------------|
| PUT | `/{path}` | Create a new stream |
| POST | `/{path}` | Append data to stream |
| GET | `/{path}?offset={offset}` | Read messages from offset |
| GET | `/{path}?offset={offset}&live=long-poll` | Long-poll for new messages |
| HEAD | `/{path}` | Get stream metadata |
| DELETE | `/{path}` | Delete stream |

### Offset Format

Offsets use the format `{readSeq}_{byteOffset}` with 16-digit zero-padding:
- `-1` - Start from the beginning
- `now` - Start from the current tail (new messages only)
- `0000000000000001_0000000000000000` - Example offset

### Response Headers

- `Stream-Next-Offset`: The offset to use for the next read
- `Stream-Up-To-Date`: "true" if the client has caught up to the tail
- `Cache-Control`: Caching directives for CDN compatibility

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Run conformance tests against Go server
bun run conformance:go
```

## License

MIT
