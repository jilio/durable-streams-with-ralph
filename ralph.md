# Durable Streams Go Port

Port the durable-streams project to Go.

## Reference
- Original: https://github.com/durable-streams/durable-streams
- This is a TypeScript project implementing HTTP-based streaming with offset-based resumability

## Goals
1. Implement core protocol types and interfaces
2. Implement server-side streaming with offset tracking
3. Implement client with reconnection and resume from offset
4. Add tests for conformance with the protocol
5. Create examples and documentation

## Phases

### Phase 1: Core Types
- Define Stream, Offset, Message types
- Implement serialization/deserialization

### Phase 2: Server
- HTTP handler for streaming responses
- Offset tracking and persistence
- SSE and long-polling support

### Phase 3: Client
- Connect to stream endpoint
- Track offset locally
- Resume from last offset on reconnect

### Phase 4: Tests
- Unit tests for core types
- Integration tests for server/client
- Conformance tests matching original project

## Completion Criteria
- All phases complete
- Tests passing
- Example server and client working
- Output: <promise>COMPLETE</promise>
