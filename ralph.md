# Durable Streams Go Port

Port the durable-streams protocol to Go.

## Reference
- Original: https://github.com/durable-streams/durable-streams
- Protocol: HTTP-based streaming with offset-based resumability
- **Reference implementation is in `reference/` directory** - study it!
- Read `reference/PROTOCOL.md` for protocol specification
- Study `reference/packages/server/src/` for server implementation patterns
- Study `reference/packages/server-conformance-tests/src/` for test cases

---

## Workflow Rules

### Autonomous Work (CRITICAL)
You MUST work completely autonomously:
- NEVER ask questions
- NEVER wait for user input
- If stuck, add notes to the task with `bd update <id> --notes "issue description"`
- If blocked, move to next available task
- Always end the iteration cleanly

### One Task Per Iteration
Each iteration you work on exactly ONE task:
1. Run `bd ready` to see available tasks
2. Pick ONE task (highest priority first)
3. Complete it using TDD workflow
4. If ALL tests pass → commit, push, `bd done <id>`
5. If epic is complete → close the epic with `bd close <epic-id>`
6. Exit iteration

### Beads Commands
- `bd ready` - See tasks without blockers
- `bd show <id>` - View task details before starting
- `bd done <id>` - Mark complete ONLY after ALL tests pass
- `bd close <id>` - Close an epic when all its tasks are done

### TDD Workflow (MANDATORY)
1. Write failing test first
2. Confirm test fails: `go test ./...`
3. Write minimal code to pass
4. Run all tests: `go test ./...`
5. Refactor if needed (keep tests green)

### Test Coverage Requirement
Go code must have >= 80% test coverage:
```bash
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

### Commit Rules (STRICT)
Commit and push ONLY when:
- `go test ./...` exits with code 0
- Test coverage >= 80%

```bash
go test ./... -coverprofile=coverage.out
# If exit code 0 and coverage >= 80%:
git add -A && git commit -m "message" && git push
bd done <task-id>
```

If tests fail: DO NOT commit. Fix first.

---

## Epic Structure

All tasks are already created in beads with proper dependencies.

### Epic 1: Project Setup (4rc)
- zdn: Initialize Go module
- q2x: Create directory structure

### Epic 2: Core Types (b23)
Depends on Epic 1.
- okl: Implement Offset type
- 46r: Implement Message type
- agt: Implement Batch type
- eve: Implement Stream interface

### Epic 3: Storage Layer (1l0)
Depends on Epic 2.
- u0i: Define StreamStorage interface
- 3h3: Implement MemoryStorage
- sly: Implement offset generation

### Epic 4: HTTP Server (2la)
Depends on Epic 3.
- e1j: PUT stream handler
- fvp: POST stream handler
- dr8: GET stream handler
- w7u: GET long-poll handler
- 2n0: GET SSE handler
- 1e5: DELETE stream handler
- 9ts: HEAD stream handler
- tqr: Cache-Control headers

### Epic 5: Client (5ed)
Depends on Epic 4.
- swl: Basic Client struct
- 136: Subscribe method
- o06: Offset tracking
- a5p: Reconnection logic
- qmk: Producer implementation
- ux1: IdempotentProducer

### Epic 6: Integration & Examples (wy3)
Depends on Epic 5.
- nx7: Integration tests
- ata: Example server
- sgl: Example client
- 7ga: Update README

### Epic 7: Conformance & Benchmarks (t4l)
Depends on Epic 6.
- ifi: Pass all 195 conformance tests
- oiy: Match reference server benchmarks

### Epic 8: Redis Storage (3my)
Depends on Epic 7.
- 3my.1: Design Redis storage schema
- 3my.2: Implement RedisStorage interface
- 3my.3: Implement Redis Streams XADD/XREAD
- 3my.4: Implement command pipelining
- 3my.5: Implement Redis long-poll with XREAD BLOCK
- 3my.6: Add Redis connection pooling
- 3my.7: Profile Redis implementation (pprof)
- 3my.8: Benchmark Redis vs Memory storage
- 3my.9: Add Redis integration tests

---

## Conformance Testing

Run conformance tests against your Go server:
```bash
# Start your Go server on port 8080
go run ./cmd/server

# In another terminal, run conformance tests
bun run conformance:go
```

Run against reference server to verify test setup:
```bash
# Start reference server on port 4437
bun run reference:start

# Run conformance tests
bun run conformance:reference
```

All 195 tests must pass. If tests fail against reference server, the problem is in your test setup, not the reference server.

---

## Benchmarks

Compare performance:
```bash
bun run benchmark:go        # Test Go server
bun run benchmark:reference # Test reference server
```

Go server should match or exceed reference server performance.

---

## Completion Criteria

The loop stops when ALL of the following are true:
- All 8 epics closed
- `bd ready` shows "No open issues"
- `go test ./...` passes with exit code 0
- Test coverage >= 80%
- All 195 conformance tests pass
- Benchmarks match or exceed reference server
- Redis storage implemented with proper profiling
- Example server and client run successfully

When ALL criteria are met, output exactly:
<promise>ALL DONE</promise>

Do NOT output the promise until everything is truly complete.
