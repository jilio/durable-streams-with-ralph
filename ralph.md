# Durable Streams Go Port

Port the durable-streams protocol to Go.

## Reference
- Original: https://github.com/durable-streams/durable-streams
- Protocol: HTTP-based streaming with offset-based resumability

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

### Commit Rules (STRICT)
Commit and push ONLY when `go test ./...` exits with code 0.

```bash
go test ./...
# If exit code 0:
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

---

## Completion Criteria

The loop stops when ALL of the following are true:
- All 6 epics closed
- `bd ready` shows "No open issues"
- `go test ./...` passes with exit code 0
- Example server and client run successfully

When ALL criteria are met, output exactly:
<promise>ALL DONE</promise>

Do NOT output the promise until everything is truly complete.
