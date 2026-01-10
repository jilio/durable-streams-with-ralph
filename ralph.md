# Durable Streams Go Port

Port the durable-streams project to Go.

## Reference
- Original: https://github.com/durable-streams/durable-streams
- TypeScript project implementing HTTP-based streaming with offset-based resumability

## Workflow Rules

### Task Tracking with Beads
You MUST use `bd` (beads) CLI for all task management:
- `bd ready` - Check available tasks at the start of each iteration
- `bd create "Task title"` - Create new tasks as you discover them
- `bd show <id>` - View task details before starting work
- `bd done <id>` - Mark task complete ONLY after tests pass

### TDD Workflow (MANDATORY)
For every feature or fix, follow this strict order:
1. **Write failing test first** - Test must fail initially
2. **Run test to confirm it fails** - `go test ./...`
3. **Write minimal code to pass** - Only enough to make test green
4. **Run all tests** - `go test ./...`
5. **Refactor if needed** - Keep tests green

### Commit Rules (STRICT)
You may ONLY commit and push when ALL of the following are true:
- All tests pass: `go test ./...` exits with code 0
- No test is skipped or pending
- The change is complete and working

Commit workflow:
```bash
go test ./...
# If exit code is 0:
git add -A
git commit -m "descriptive message"
git push
bd done <task-id>
```

If tests fail: DO NOT commit. Fix the issue first.

### Iteration Cycle
Each iteration you must:
1. Run `bd ready` to see available tasks
2. Pick one task and run `bd show <id>`
3. Follow TDD workflow for that task
4. Run `go test ./...`
5. If ALL tests pass → commit, push, `bd done <id>`
6. If tests fail → fix and repeat step 4

## Project Structure

```
durable-streams-with-ralph/
├── stream/           # Core types and interfaces
│   ├── types.go
│   ├── types_test.go
│   ├── offset.go
│   └── offset_test.go
├── server/           # Server implementation
│   ├── handler.go
│   ├── handler_test.go
│   └── storage.go
├── client/           # Client implementation
│   ├── client.go
│   └── client_test.go
├── examples/         # Example applications
├── go.mod
└── go.sum
```

## Initial Tasks

When starting, create these tasks in beads:
```bash
bd create "Initialize Go module" -p 0
bd create "Implement Offset type with tests" -p 1
bd create "Implement Message type with tests" -p 1
bd create "Implement Stream interface" -p 1
bd create "Implement in-memory StreamStorage" -p 2
bd create "Implement HTTP streaming handler" -p 2
bd create "Implement SSE response format" -p 2
bd create "Implement Client with reconnection" -p 3
bd create "Add integration tests" -p 3
bd create "Create example server" -p 4
bd create "Create example client" -p 4
```

## Completion Criteria
- All beads tasks marked done (`bd ready` shows no open issues)
- All tests passing (`go test ./...` exits 0)
- Example server and client working
- Code pushed to remote

When ALL criteria met, output: <promise>COMPLETE</promise>
