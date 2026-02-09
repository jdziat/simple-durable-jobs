# Embeddable UI Design

**Date:** 2026-02-09
**Status:** Draft

## Overview

An embeddable web dashboard for monitoring and managing jobs in simple-durable-jobs. Provides both operational monitoring (queue depths, failures, retries) and debugging capabilities (inspect args, view checkpoints, trace workflows).

## Goals

- **Embeddable**: Mount as `http.Handler` in any Go application
- **Standalone**: Run as separate binary against existing database
- **Zero dependencies**: Single binary with embedded frontend assets
- **Type-safe**: Connect-RPC for end-to-end type safety
- **Real-time**: Live updates via streaming

## Architecture

### Location

The UI lives at `ui/` in the main repository:

```
simple-durable-jobs/
├── pkg/                    # Core library
├── ui/                     # Dashboard (this design)
│   ├── proto/
│   ├── gen/
│   ├── frontend/
│   └── cmd/jobsui/
└── examples/
```

Import paths:
- `github.com/jdziat/simple-durable-jobs` - core library
- `github.com/jdziat/simple-durable-jobs/ui` - dashboard

### Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Frontend | Svelte | Small bundles, fast, great DX for dashboards |
| API | Connect-RPC | Type-safe, streaming, browser-compatible |
| Embedding | `embed.FS` | Single binary distribution |
| Auth | Middleware hook | Users integrate their existing auth |

## Project Structure

```
ui/
├── proto/
│   └── jobs/v1/
│       └── jobs.proto              # Service definition
├── gen/
│   ├── jobs/v1/                    # Generated Go
│   │   ├── jobs.pb.go
│   │   └── jobsv1connect/
│   │       └── jobs.connect.go
│   └── ts/                         # Generated TypeScript
│       └── jobs/v1/
│           ├── jobs_pb.ts
│           └── jobs_connect.ts
├── handler.go                      # Main http.Handler
├── service.go                      # JobsServiceHandler implementation
├── embed.go                        # embed.FS for frontend assets
├── options.go                      # Handler options (WithMiddleware, etc.)
├── cmd/jobsui/
│   └── main.go                     # Standalone binary
├── frontend/
│   ├── src/
│   │   ├── App.svelte
│   │   ├── main.ts
│   │   ├── lib/
│   │   │   ├── client.ts           # Connect client setup
│   │   │   └── components/
│   │   │       ├── JobsTable.svelte
│   │   │       ├── StatsCard.svelte
│   │   │       ├── Chart.svelte
│   │   │       └── ...
│   │   └── routes/
│   │       ├── Dashboard.svelte
│   │       ├── Jobs.svelte
│   │       ├── JobDetail.svelte
│   │       ├── Queues.svelte
│   │       └── Scheduled.svelte
│   ├── static/
│   ├── package.json
│   ├── vite.config.ts
│   ├── tsconfig.json
│   └── buf.gen.yaml                # TS code generation
├── buf.yaml                        # Buf module config
└── buf.gen.yaml                    # Go code generation
```

## Connect-RPC API

### Proto Definition

```protobuf
syntax = "proto3";
package jobs.v1;

option go_package = "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1;jobsv1";

import "google/protobuf/timestamp.proto";

service JobsService {
  // Dashboard
  rpc GetStats(GetStatsRequest) returns (GetStatsResponse);
  rpc GetStatsHistory(GetStatsHistoryRequest) returns (GetStatsHistoryResponse);

  // Jobs
  rpc ListJobs(ListJobsRequest) returns (ListJobsResponse);
  rpc GetJob(GetJobRequest) returns (GetJobResponse);
  rpc RetryJob(RetryJobRequest) returns (RetryJobResponse);
  rpc DeleteJob(DeleteJobRequest) returns (DeleteJobResponse);
  rpc BulkRetryJobs(BulkRetryJobsRequest) returns (BulkRetryJobsResponse);
  rpc BulkDeleteJobs(BulkDeleteJobsRequest) returns (BulkDeleteJobsResponse);

  // Queues
  rpc ListQueues(ListQueuesRequest) returns (ListQueuesResponse);
  rpc PurgeQueue(PurgeQueueRequest) returns (PurgeQueueResponse);

  // Scheduled
  rpc ListScheduledJobs(ListScheduledJobsRequest) returns (ListScheduledJobsResponse);

  // Real-time
  rpc WatchEvents(WatchEventsRequest) returns (stream Event);
}

// --- Messages ---

message Job {
  string id = 1;
  string type = 2;
  string queue = 3;
  string status = 4;
  int32 priority = 5;
  int32 attempt = 6;
  int32 max_retries = 7;
  bytes args = 8;
  string last_error = 9;
  google.protobuf.Timestamp created_at = 10;
  google.protobuf.Timestamp started_at = 11;
  google.protobuf.Timestamp completed_at = 12;
  google.protobuf.Timestamp run_at = 13;
}

message Checkpoint {
  string id = 1;
  string job_id = 2;
  int32 call_index = 3;
  string call_type = 4;
  bytes result = 5;
  string error = 6;
  google.protobuf.Timestamp created_at = 7;
}

message QueueStats {
  string name = 1;
  int64 pending = 2;
  int64 running = 3;
  int64 completed = 4;
  int64 failed = 5;
}

message Event {
  string type = 1;  // job.started, job.completed, job.failed, job.retrying
  Job job = 2;
  google.protobuf.Timestamp timestamp = 3;
}

// --- Requests/Responses ---

message GetStatsRequest {}
message GetStatsResponse {
  repeated QueueStats queues = 1;
  int64 total_pending = 2;
  int64 total_running = 3;
  int64 total_completed = 4;
  int64 total_failed = 5;
  int32 active_workers = 6;
}

message GetStatsHistoryRequest {
  string period = 1;  // hour, day, week
}
message GetStatsHistoryResponse {
  repeated DataPoint completed = 1;
  repeated DataPoint failed = 2;
}
message DataPoint {
  google.protobuf.Timestamp timestamp = 1;
  int64 value = 2;
}

message ListJobsRequest {
  string status = 1;
  string queue = 2;
  string type = 3;
  string search = 4;
  google.protobuf.Timestamp since = 5;
  google.protobuf.Timestamp until = 6;
  int32 page = 7;
  int32 limit = 8;
}
message ListJobsResponse {
  repeated Job jobs = 1;
  int64 total = 2;
  int32 page = 3;
}

message GetJobRequest {
  string id = 1;
}
message GetJobResponse {
  Job job = 1;
  repeated Checkpoint checkpoints = 2;
}

message RetryJobRequest {
  string id = 1;
}
message RetryJobResponse {
  Job job = 1;
}

message DeleteJobRequest {
  string id = 1;
}
message DeleteJobResponse {}

message BulkRetryJobsRequest {
  repeated string ids = 1;
}
message BulkRetryJobsResponse {
  int32 count = 1;
}

message BulkDeleteJobsRequest {
  repeated string ids = 1;
}
message BulkDeleteJobsResponse {
  int32 count = 1;
}

message ListQueuesRequest {}
message ListQueuesResponse {
  repeated QueueStats queues = 1;
}

message PurgeQueueRequest {
  string name = 1;
  string status = 2;  // failed, completed
}
message PurgeQueueResponse {
  int64 deleted = 1;
}

message ListScheduledJobsRequest {}
message ListScheduledJobsResponse {
  repeated ScheduledJob jobs = 1;
}
message ScheduledJob {
  string name = 1;
  string schedule = 2;
  string queue = 3;
  google.protobuf.Timestamp next_run = 4;
  google.protobuf.Timestamp last_run = 5;
}

message WatchEventsRequest {
  repeated string queues = 1;  // empty = all queues
}
```

## Go Handler API

### Basic Usage

```go
package main

import (
    "net/http"

    jobs "github.com/jdziat/simple-durable-jobs"
    "github.com/jdziat/simple-durable-jobs/ui"
)

func main() {
    storage := jobs.NewGormStorage(db)
    queue := jobs.New(storage)

    // Mount dashboard at /jobs/
    mux := http.NewServeMux()
    mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage)))

    http.ListenAndServe(":8080", mux)
}
```

### With Authentication

```go
authMiddleware := func(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        user := getUser(r)
        if user == nil || !user.IsAdmin {
            http.Error(w, "Forbidden", http.StatusForbidden)
            return
        }
        next.ServeHTTP(w, r)
    })
}

mux.Handle("/admin/jobs/", http.StripPrefix("/admin/jobs",
    ui.Handler(storage, ui.WithMiddleware(authMiddleware)),
))
```

### Handler Options

```go
// WithMiddleware wraps the handler with middleware (auth, logging, etc.)
func WithMiddleware(mw func(http.Handler) http.Handler) Option

// WithQueue provides access to registered handlers for scheduled jobs view
func WithQueue(q *queue.Queue) Option
```

## Standalone Binary

```go
// ui/cmd/jobsui/main.go
package main

import (
    "flag"
    "log"
    "net/http"

    "github.com/jdziat/simple-durable-jobs/pkg/storage"
    "github.com/jdziat/simple-durable-jobs/ui"
    "gorm.io/driver/postgres"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    dsn := flag.String("dsn", "", "Database connection string (required)")
    driver := flag.String("driver", "postgres", "Database driver: postgres, sqlite")
    addr := flag.String("addr", ":8080", "Listen address")
    flag.Parse()

    if *dsn == "" {
        log.Fatal("--dsn is required")
    }

    var dialector gorm.Dialector
    switch *driver {
    case "postgres":
        dialector = postgres.Open(*dsn)
    case "sqlite":
        dialector = sqlite.Open(*dsn)
    default:
        log.Fatalf("Unknown driver: %s", *driver)
    }

    db, err := gorm.Open(dialector, &gorm.Config{})
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }

    store := storage.NewGormStorage(db)

    log.Printf("Jobs UI running at http://localhost%s", *addr)
    log.Fatal(http.ListenAndServe(*addr, ui.Handler(store)))
}
```

**Usage:**
```bash
# Install
go install github.com/jdziat/simple-durable-jobs/ui/cmd/jobsui@latest

# Run
jobsui --driver postgres --dsn "postgres://user:pass@localhost/myapp"
jobsui --driver sqlite --dsn "./jobs.db"
```

## Frontend Screens

### 1. Dashboard
- Queue depth cards (pending/running/failed per queue)
- Throughput chart (jobs completed/failed over time)
- Active workers count
- Recent failures list (last 10)

### 2. Jobs List
- Filterable table with columns: ID, Type, Queue, Status, Created, Actions
- Filters: status dropdown, queue dropdown, type dropdown, date range
- Search box for job ID or args content
- Bulk selection with retry/delete actions
- Pagination

### 3. Job Detail
- Header: ID, Type, Status badge, Queue
- Timestamps: Created, Started, Completed
- Attempt count and max retries
- Args viewer (formatted JSON with syntax highlighting)
- Error message (if failed)
- Checkpoints table (for workflows): Index, Type, Result/Error
- Actions: Retry, Delete

### 4. Queues
- Table: Queue name, Pending, Running, Completed, Failed
- Actions per queue: Purge failed, Purge completed

### 5. Scheduled Jobs
- Table: Name, Schedule (human-readable), Queue, Next Run, Last Run
- Shows registered scheduled jobs from the queue

## Build Process

### Buf Configuration

```yaml
# ui/buf.yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - DEFAULT
breaking:
  use:
    - FILE
```

```yaml
# ui/buf.gen.yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gen
    opt: paths=source_relative
  - remote: buf.build/connectrpc/go
    out: gen
    opt: paths=source_relative
```

```yaml
# ui/frontend/buf.gen.yaml
version: v2
plugins:
  - remote: buf.build/connectrpc/es
    out: src/lib/gen
    opt: target=ts
```

### Makefile

```makefile
.PHONY: ui-generate ui-build ui

# Generate protobuf code
ui-generate:
	cd ui && buf generate
	cd ui/frontend && npx buf generate --template buf.gen.yaml ../proto

# Build frontend
ui-build: ui-generate
	cd ui/frontend && npm install && npm run build

# Full UI build (run before go build)
ui: ui-build
	@echo "UI built successfully"
```

### CI Integration

Add to `.github/workflows/ci.yml`:

```yaml
- name: Setup Node
  uses: actions/setup-node@v4
  with:
    node-version: '20'
    cache: 'npm'
    cache-dependency-path: ui/frontend/package-lock.json

- name: Install Buf
  uses: bufbuild/buf-setup-action@v1

- name: Generate and build UI
  run: make ui

- name: Build
  run: go build ./...
```

## Storage Interface Extensions

The UI needs some additional query capabilities. Add to `core.Storage`:

```go
type Storage interface {
    // ... existing methods ...

    // UI queries (optional - UI checks for interface)
    GetQueueStats(ctx context.Context) ([]QueueStats, error)
    SearchJobs(ctx context.Context, filter JobFilter) ([]Job, int64, error)
    RetryJob(ctx context.Context, jobID string) error
    DeleteJob(ctx context.Context, jobID string) error
    PurgeJobs(ctx context.Context, queue string, status JobStatus) (int64, error)
}

// UIStorage is optionally implemented by storage backends
type UIStorage interface {
    Storage
    GetQueueStats(ctx context.Context) ([]QueueStats, error)
    SearchJobs(ctx context.Context, filter JobFilter) ([]Job, int64, error)
    // ...
}
```

The UI checks if storage implements `UIStorage`, otherwise falls back to basic queries.

## Testing

### Backend
- Unit tests for service methods
- Integration tests with test database

### Frontend
- Component tests with Vitest
- E2E tests with Playwright (optional)

### Manual Testing
```bash
# Run with test data
go run ./ui/cmd/jobsui --driver sqlite --dsn ":memory:"
```

## Future Considerations

Not in initial scope, but potential additions:
- **Pause/resume queues** - requires core library changes
- **Job editing** - modify args before retry
- **Metrics export** - Prometheus endpoint
- **Dark mode** - theme toggle
- **Webhook notifications** - on failure patterns

## Implementation Plan

1. **Proto & codegen setup** - buf config, generate Go and TS
2. **Go service skeleton** - implement JobsServiceHandler
3. **Frontend scaffold** - Svelte project with routing
4. **Dashboard screen** - stats, charts
5. **Jobs list & detail** - main functionality
6. **Queues & scheduled** - secondary screens
7. **Streaming** - WatchEvents implementation
8. **Standalone binary** - cmd/jobsui
9. **Documentation** - README, examples
10. **CI integration** - build frontend in workflow
