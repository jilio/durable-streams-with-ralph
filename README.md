# durable-streams-with-ralph

A Go (Golang) port of [durable-streams](https://github.com/durable-streams/durable-streams).

## About

Durable Streams is an open protocol for reliable, real-time data synchronization to client applications. It provides HTTP-based streaming with offset-based resumability, solving data loss issues that occur with traditional WebSocket and SSE connections when clients disconnect.

### Key Features

- **Offset-based resumption**: Clients can reconnect and resume from their last known position
- **CDN-friendly**: Offset-based URLs enable aggressive caching
- **Exactly-once semantics**: No data loss or duplication
- **Content-type agnostic**: Works with text, JSON, binary, and custom formats

## Why Go?

This port aims to provide a performant, statically-typed implementation suitable for high-throughput server environments.

## Status

Work in progress.
