# AGENTS.md

Guidelines for AI agents working on this codebase.

## Project Overview

Simple Durable Jobs is a Go library for durable job queues with checkpointed workflows. It provides background job processing, fan-out/fan-in parallelism, pause/resume at multiple levels, and an embedded web UI dashboard.

## Architecture

The project uses a layered architecture with a root facade pattern:

- **`jobs.go` / `pause.go`** - Root facade re-exporting all public types from `pkg/`
- **`pkg/core/`** - Domain models: `Job`, `FanOut`, `Storage` interface, `Event` types, errors
- **`pkg/queue/`** - Queue orchestration, event pub/sub, pause operations, running job registry
- **`pkg/worker/`** - Job processing, per-queue concurrency, pause/resume, heartbeats
- **`pkg/storage/`** - GORM-based `Storage` implementation (SQLite, PostgreSQL, MySQL)
- **`pkg/call/`** - Durable `Call[T]` for checkpointed nested job execution
- **`pkg/fanout/`** - Fan-out/fan-in: `Sub`, `FanOut[T]`, result helpers
- **`pkg/schedule/`** - Schedule types: `Every`, `Daily`, `Weekly`, `Cron`
- **`pkg/jobctx/`** - Context helpers: `JobFromContext`, phase checkpoints
- **`pkg/security/`** - Input validation and sanitization
- **`ui/`** - Embedded web dashboard (Svelte frontend + Connect-RPC API)

## Key Conventions

### Go Code

- **Facade pattern**: All public types are re-exported through the root `jobs` package. Users should never import `pkg/` sub-packages directly.
- **Constructor pattern**: Use `NewXxx()` factory functions, not direct struct initialization.
- **Error handling**: Return sentinel errors from storage layer (e.g., `ErrJobAlreadyPaused`). Convert to user-facing errors at the API boundary.
- **Concurrency**: Queue's event system uses `sync.RWMutex` for subscriber management. Worker tracks running jobs with per-job cancel functions. Use `sync.Once` for one-time initialization (e.g., `StatsCollector.ready`).
- **Event system**: Channels are buffered (100 elements). `Unsubscribe()` removes the channel but does NOT close it. Callers must stop reading before calling `Unsubscribe`. `Emit` copies the subscriber slice under RLock to avoid holding the lock during sends.
- **Context lifecycle**: Background goroutines (like stats collector) should accept a `context.Context` for shutdown. Use bounded timeout contexts for flush operations during shutdown (e.g., 5s timeout).

### Testing

- Use `testify/assert` and `testify/require` for assertions.
- Use `WaitReady()` synchronization instead of `time.Sleep` for event-driven tests.
- Run tests with `-race` flag to catch data races.
- Mock storage with in-memory implementations in `*_test.go` files.

### UI Package

- Frontend: Svelte 5 with runes, Chart.js for visualization, Connect-RPC for API.
- Backend: Connect-RPC service in `ui/service.go`, stats collection in `ui/stats_collector.go`.
- The UI handler is mounted via `ui.Handler(storage, opts...)` and serves both API and static assets.
- Stats collector subscribes to queue events and periodically snapshots queue depth.
- Stream limits: max 50 concurrent `WatchEvents` streams (atomic counter).
- Fallback queries are capped to prevent OOM (1000 jobs per status).

## Adding New Features

1. Define domain types in `pkg/core/` if needed.
2. Implement in the appropriate `pkg/` sub-package.
3. Re-export through the root `jobs.go` facade.
4. Add tests with `-race` safety.
5. Update this file and `README.md` if the feature is user-facing.

## Common Patterns

### Adding a new event type

1. Define the event struct in `pkg/core/events.go` implementing the `Event` interface.
2. Emit it from the appropriate queue/worker method via `q.Emit(&YourEvent{...})`.
3. Add a case in `ui/service.go:coreEventToProto()` to convert to protobuf for the UI.
4. Add the proto message in `ui/proto/` and regenerate.

### Adding a new queue operation

1. Add the method to `pkg/core/storage.go` `Storage` interface.
2. Implement in `pkg/storage/gorm.go`.
3. Add the queue-level method in `pkg/queue/queue.go`.
4. Re-export through `jobs.go` if it should be a standalone function.
5. Update mock storage in test files.

### Adding a UI endpoint

1. Define the RPC method in `ui/proto/jobs/v1/jobs.proto`.
2. Regenerate with `buf generate`.
3. Implement the handler in `ui/service.go`.
4. Add frontend support in `ui/frontend/src/`.

## Running Tests

```bash
# All tests with race detector
go test ./... -race

# Specific package
go test ./pkg/queue/ -v -race

# UI package
go test ./ui/ -v

# Build frontend (required for embedded assets)
cd ui/frontend && npm install && npm run build
```

## File Layout

```
jobs.go              - Root facade (re-exports)
pause.go             - Standalone pause/resume functions
pkg/core/            - Domain types, interfaces, errors, events
pkg/queue/           - Queue + event system + pause ops + running job registry
pkg/worker/          - Worker loop + pause/resume + heartbeat
pkg/storage/         - GormStorage (GORM-based persistence)
pkg/call/            - Durable Call[T]
pkg/fanout/          - FanOut[T], Sub, result helpers
pkg/schedule/        - Every, Daily, Weekly, Cron
pkg/jobctx/          - Context helpers
pkg/security/        - Validation
ui/                  - Embedded dashboard
ui/frontend/         - Svelte 5 SPA
ui/gen/              - Generated protobuf/Connect-RPC
docs/                - GitHub Pages documentation
examples/            - Usage examples
```
