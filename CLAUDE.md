# Claude Development Guidelines

## Project Overview

This is a Go port of [durable-streams](https://github.com/durable-streams/durable-streams) - an open protocol for reliable, real-time data synchronization.

## Important Notes

### Reference Implementation
- The reference implementation in `reference/` directory is the source of truth
- **There are NO bugs in the reference server** - if tests fail, the problem is on our side
- If conformance tests fail against reference server, check the test environment setup

### Source Code Reference
- Study `reference/packages/server/` for server implementation patterns
- Study `reference/packages/server-conformance-tests/` for test cases
- Read `reference/PROTOCOL.md` for protocol specification

## Project Structure

```
durable-streams-with-ralph/
├── reference/          # Cloned durable-streams repo (DO NOT MODIFY)
├── stream/             # Core Go types
├── server/             # Go HTTP server
├── client/             # Go client
├── examples/           # Example applications
├── scripts/            # Helper scripts
│   └── run-conformance.ts  # Run conformance tests
├── ralph.md            # Ralph loop prompt
├── package.json        # npm scripts for testing
└── CLAUDE.md           # This file
```

## Commands

```bash
# Run Go tests
go test ./... -v

# Run Go tests with coverage (must be >= 80%)
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Start Go server (default port 8080)
go run ./cmd/server

# Start reference server (port 4437)
bun run reference:start

# Run conformance tests against Go server
bun run conformance:go

# Run conformance tests against reference server
bun run conformance:reference

# Run benchmarks
bun run benchmark:go
bun run benchmark:reference
```

## Testing Requirements

1. **Unit tests**: All Go code must have >= 80% test coverage
2. **Conformance tests**: Go server must pass all 195 official conformance tests
3. **Benchmarks**: Go server should match or exceed reference server performance
4. **Redis tests**: Use testcontainers or local Redis for integration tests

## Redis Storage Implementation

Redis storage uses Redis Streams for optimal performance:
- **XADD**: Append messages to stream
- **XREAD**: Read messages from offset (stream ID)
- **XREAD BLOCK**: Long-polling without busy waiting
- **Pipeline**: Batch multiple commands for lower latency

Performance requirements:
- Use connection pooling for high concurrency
- Pipeline commands when possible (MULTI/EXEC or Pipeline())
- Profile with pprof to identify bottlenecks
- Benchmark against MemoryStorage for comparison

## TDD Workflow

1. Write failing test first
2. Confirm test fails
3. Write minimal code to pass
4. Run all tests
5. Refactor if needed

## Commit Rules

Only commit when:
- `go test ./...` exits with code 0
- No skipped tests
- Coverage >= 80%
