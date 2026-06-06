# Simple Durable Jobs

[![Go Reference](https://pkg.go.dev/badge/github.com/jdziat/simple-durable-jobs.svg)](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs)
[![CI](https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/jdziat/simple-durable-jobs)](https://goreportcard.com/report/github.com/jdziat/simple-durable-jobs)
[![codecov](https://codecov.io/gh/jdziat/simple-durable-jobs/branch/main/graph/badge.svg)](https://codecov.io/gh/jdziat/simple-durable-jobs)

A Go library for durable, checkpointed background jobs and workflows that runs
on the database you already have — PostgreSQL or MySQL in production, SQLite for
local development. Conceptually inspired by [River](https://riverqueue.com/) and
[Temporal](https://temporal.io/).

### What it is

An **in-process, embeddable** job system: import a package, point it at a GORM
database, register handlers, run workers. No separate server, broker, or control
plane to operate. You get durable background jobs, checkpointed multi-step
workflows, fan-out/fan-in, durable signals, scheduling, and an optional embedded
dashboard — in one dependency.

### When to choose it

- You're a **Go team already on Postgres/MySQL** and want durable jobs and light
  workflows without standing up Temporal or another service.
- You want crash-safe, checkpointed multi-step jobs with a small operational
  footprint.

Reach for **Temporal** instead if you need full deterministic workflow replay,
workflow queries, and versioning at scale; for **River** if you want a
Postgres-only, single-purpose job queue with a larger team behind it. This
library deliberately trades some of that surface area for "one import, your
existing database."

### What it guarantees (and asks of you)

Execution is **at-least-once**: handlers and `Call()` steps re-run from their
last checkpoint after a crash, so **handlers must be idempotent**. Jobs are
durable and never silently lost, and a job has a single active owner — lock
timing is anchored to the database clock, so worker clock skew can't reclaim a
live lock. See **[Guarantees & Production Readiness](https://jdziat.github.io/simple-durable-jobs/docs/advanced/guarantees/)**
for the full contract, backend support tiers, and crash-recovery tuning.

## Documentation

- [Getting Started](https://jdziat.github.io/simple-durable-jobs/docs/getting-started/)
- [API Reference](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/)
- [Examples](https://jdziat.github.io/simple-durable-jobs/docs/examples/)
- [Embedded Web UI](https://jdziat.github.io/simple-durable-jobs/docs/embedded-ui/)
- [Live Demo](https://jdziat.github.io/simple-durable-jobs/docs/live-demo/)
- [Guarantees & Production Readiness](https://jdziat.github.io/simple-durable-jobs/docs/advanced/guarantees/) - Execution semantics, backend tiers, crash-recovery tuning
- [Advanced Topics](https://jdziat.github.io/simple-durable-jobs/docs/advanced/) - Transactional enqueue, dead-letter queue, retention/GC, rate limiting, concurrency caps, metrics, workflow versioning, payload codec, execution middleware, dashboard authorization, and testing utilities

## Features

- **Background Jobs** - Fire-and-forget task processing
- **Durable Workflows** - Multi-step workflows with automatic checkpointing
- **Durable Signals** - send/wait/check/drain for cross-job and external coordination
- **Fan-Out/Fan-In** - Spawn parallel sub-jobs, wait for results, aggregate
- **Crash Recovery** - Jobs resume from the last successful checkpoint
- **Pause / Resume / Cancel** - Pause, resume, or cancel jobs, queues, or workers (graceful or aggressive); first-class cancel verb across facade, RPC, and dashboard
- **Scheduled Jobs** - Cron, daily, weekly, and interval-based scheduling
- **Priority Queues** - Higher priority jobs run first
- **Transactional Enqueue** - Enqueue inside your own DB transaction (outbox pattern) so business rows and jobs commit together
- **Batch Enqueue & Dequeue** - Bulk insert plus buffered worker-side batch dequeue for throughput
- **Concurrency Caps** - Fleet-wide and per-key concurrency limits, not just per-process
- **Rate Limiting** - Per-queue token buckets and an optional fleet-wide limiter (throttle waits don't burn retry attempts)
- **Retries with Backoff** - Configurable retry logic with exponential backoff and jitter
- **Dead-Letter Queue** - Explicit dead-letter metadata with list/count/requeue triage
- **Retention / GC** - Automatic pruning of terminal jobs by per-status age window
- **Execution Middleware** - Interceptors that wrap handler execution
- **Payload Codec** - Pluggable encryption-at-rest for job arguments, results, and checkpoints
- **Workflow Versioning** - GetVersion markers to evolve in-flight workflows safely across deploys
- **Metrics** - Optional Prometheus / OpenTelemetry metrics exporter (queue depth, latency, throughput, failures)
- **Observability** - Hooks, event streams, OpenTelemetry tracing, and an embedded web dashboard
- **Embedded Web UI** - Real-time monitoring dashboard with stats, event streaming, and job actions (incl. cancel)
- **Dashboard Authorization** - Optional per-action authorization for mutating dashboard RPCs
- **Stale Lock Reaper** - Automatically reclaims stuck running jobs after worker crashes
- **Connection Pool Presets** - Pre-built pool configurations for different workloads
- **Testing Utilities** - The jobstest package: SQLite fixtures and enqueue assertions
- **Simple API** - Minimal boilerplate, type-safe handlers

## Installation

```bash
go get github.com/jdziat/simple-durable-jobs
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    jobs "github.com/jdziat/simple-durable-jobs"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    // Setup database. The SQLite DSN parameters (WAL + busy_timeout + immediate
    // transactions) are required for safe concurrent workers — without them,
    // concurrent writes transiently fail with SQLITE_BUSY/SQLITE_READONLY.
    db, _ := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
    storage := jobs.NewGormStorage(db)
    storage.Migrate(context.Background())

    // Create queue
    queue := jobs.New(storage)

    // Register a job handler
    queue.Register("send-email", func(ctx context.Context, args EmailArgs) error {
        fmt.Printf("Sending email to %s\n", args.To)
        return nil
    })

    // Enqueue a job
    queue.Enqueue(context.Background(), "send-email", EmailArgs{
        To:      "user@example.com",
        Subject: "Hello!",
    })

    // Start worker
    worker := queue.NewWorker()
    worker.Start(context.Background())
}

type EmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}
```

## Durable Workflows

Build multi-step workflows where each step is checkpointed. If a workflow fails, it resumes from the last successful checkpoint:

```go
// Register step handlers
queue.Register("charge-payment", func(ctx context.Context, amount int) (string, error) {
    // Process payment...
    return "receipt-123", nil
})

queue.Register("ship-order", func(ctx context.Context, items []string) error {
    // Ship items...
    return nil
})

// Register workflow that uses checkpointed calls
queue.Register("process-order", func(ctx context.Context, order Order) error {
    // Step 1: Charge payment (checkpointed)
    receipt, err := jobs.Call[string](ctx, "charge-payment", order.Amount)
    if err != nil {
        return err
    }

    // Step 2: Ship order (checkpointed)
    // If this fails, step 1 won't re-execute on retry
    _, err = jobs.Call[any](ctx, "ship-order", order.Items)
    return err
})
```

## Fan-Out/Fan-In

Process items in parallel using sub-jobs, then aggregate results:

```go
// Register sub-job handler
queue.Register("process-item", func(ctx context.Context, item Item) (Result, error) {
    // Process individual item...
    return Result{ID: item.ID, Status: "done"}, nil
})

// Register parent job that fans out
queue.Register("batch-process", func(ctx context.Context, items []Item) error {
    // Create sub-jobs for each item
    subJobs := make([]jobs.SubJob, len(items))
    for i, item := range items {
        subJobs[i] = jobs.Sub("process-item", item)
    }

    // Fan-out: spawn all sub-jobs in parallel, wait for results
    results, err := jobs.FanOut[Result](ctx, subJobs, jobs.FailFast())
    if err != nil {
        return err
    }

    // Aggregate successful results
    processed := jobs.Values(results)
    fmt.Printf("Processed %d items\n", len(processed))
    return nil
})
```

### Fan-Out Strategies

```go
// Fail immediately on first sub-job failure
jobs.FanOut[T](ctx, subJobs, jobs.FailFast())

// Wait for all sub-jobs, return partial results
jobs.FanOut[T](ctx, subJobs, jobs.CollectAll())

// Succeed if at least 80% of sub-jobs complete
jobs.FanOut[T](ctx, subJobs, jobs.Threshold(0.8))
```

### Fan-Out Options

```go
jobs.FanOut[T](ctx, subJobs,
    jobs.FailFast(),                      // Strategy
    jobs.WithFanOutQueue("batch"),        // Run sub-jobs on specific queue
    jobs.WithFanOutRetries(5),            // Sub-job retry count
    jobs.WithFanOutTimeout(1*time.Hour),  // Total fan-out timeout
    jobs.CancelOnParentFailure(),         // Cancel sub-jobs if parent fails
)
```

### Result Helpers

```go
results, _ := jobs.FanOut[T](ctx, subJobs, jobs.CollectAll())

// Extract successful values
values := jobs.Values(results)

// Split into successes and failures
successes, failures := jobs.Partition(results)

// Check if all succeeded
if jobs.AllSucceeded(results) {
    // ...
}
```

## Pause/Resume

Pause and resume at three levels: individual jobs, entire queues, or workers.

```go
// Pause/resume a pending or waiting job
queue.PauseJob(ctx, jobID)
queue.ResumeJob(ctx, jobID)

// Cancel a running job
queue.PauseJob(ctx, jobID, jobs.WithPauseMode(jobs.PauseModeAggressive))

// Pause/resume an entire queue (stops dequeuing)
queue.PauseQueue(ctx, "emails")
queue.ResumeQueue(ctx, "emails")

// Pause/resume a worker
worker.Pause(jobs.PauseModeGraceful)    // Let running jobs finish
worker.Pause(jobs.PauseModeAggressive)  // Cancel running jobs immediately
worker.Resume()

// Check pause status
paused, _ := queue.IsJobPaused(ctx, jobID)
paused, _ = queue.IsQueuePaused(ctx, "emails")
```

## Embedded Web UI

Mount a real-time monitoring dashboard into your existing HTTP server:

```go
import "github.com/jdziat/simple-durable-jobs/ui"

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage,
    ui.WithQueue(queue),             // Enable event streaming and scheduled jobs view
    ui.WithContext(ctx),             // Lifecycle context for background workers
    ui.WithStatsRetention(7*24*time.Hour), // How long to keep stats (default: 7 days)
    ui.WithMiddleware(authMiddleware),      // Wrap with auth, logging, etc.
)))
```

The dashboard provides:
- Real-time queue statistics (pending, running, completed, failed)
- Historical stats charts with configurable time periods
- Live event streaming via Connect-RPC
- Job listing with search, filtering, and pagination
- Job actions (retry, delete, bulk operations)
- Queue management (purge by status)
- Scheduled jobs overview

## Scheduled Jobs

```go
// Run every 5 minutes
queue.Schedule("cleanup", jobs.Every(5 * time.Minute))

// Run daily at 9:00 AM
queue.Schedule("daily-report", jobs.Daily(9, 0))

// Run weekly on Sunday at 2:00 AM
queue.Schedule("backup", jobs.Weekly(time.Sunday, 2, 0))

// Use cron expressions
queue.Schedule("hourly-check", jobs.Cron("0 * * * *"))

// Start worker with scheduler enabled
worker := queue.NewWorker(jobs.WithScheduler(true))
```

## Job Options

```go
queue.Enqueue(ctx, "task", args,
    jobs.Priority(100),                   // Higher priority runs first
    jobs.Retries(5),                      // Max retry attempts
    jobs.Delay(10 * time.Second),         // Delay execution
    jobs.At(scheduledAt),                 // Run at a specific time
    jobs.Timeout(30 * time.Minute),       // Recorded on the job; enforce via ctx
    jobs.QueueOpt("critical"),            // Assign to named queue
    jobs.Unique("order-123"),             // Deduplicate by key; ErrDuplicateJob if taken
    jobs.Determinism(jobs.Strict),        // Policy for Call() replay on restart
)
```

## Observability

```go
// Hooks
queue.OnJobStart(func(ctx context.Context, job *jobs.Job) {
    log.Printf("Job %s started", job.ID)
})

queue.OnJobComplete(func(ctx context.Context, job *jobs.Job) {
    log.Printf("Job %s completed", job.ID)
})

queue.OnJobFail(func(ctx context.Context, job *jobs.Job, err error) {
    log.Printf("Job %s failed: %v", job.ID, err)
})

// Event stream
events := queue.Events()
defer queue.Unsubscribe(events) // Clean up when done
go func() {
    for event := range events {
        switch e := event.(type) {
        case *jobs.JobStarted:
            // Handle start
        case *jobs.JobCompleted:
            // Handle completion
        case *jobs.JobFailed:
            // Handle failure
        case *jobs.JobPaused:
            // Handle pause
        }
    }
}()

// Custom ephemeral events
queue.EmitCustomEvent(jobID, "progress", map[string]any{"pct": 75})
```

### OpenTelemetry Tracing

Optional distributed tracing via the `pkg/otel` sub-package. Trace context propagates
from enqueue to worker execution automatically.

```go
import jobsotel "github.com/jdziat/simple-durable-jobs/pkg/otel"

// Instrument the queue (uses global TracerProvider by default)
jobsotel.Instrument(queue)

// Or provide a custom TracerProvider
jobsotel.Instrument(queue, jobsotel.WithTracerProvider(tp))
```

This creates spans for:
- `job.enqueue` — when a job is enqueued (producer span)
- `job.process {type}` — when a job is executed by a worker (consumer span, child of enqueue)
- `job.retry` — span event when a job is retried

Span attributes include `job.id`, `job.type`, `job.queue`, `job.attempt`, and `job.priority`.

## Worker Configuration

```go
worker := queue.NewWorker(
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
    jobs.WorkerQueue("critical", jobs.Concurrency(5)),
    jobs.WithScheduler(true),
    jobs.WithPollInterval(500 * time.Millisecond),
)
```

## Error Handling

```go
// Don't retry this job
return jobs.NoRetry(errors.New("invalid input"))

// Retry after specific duration
return jobs.RetryAfter(5 * time.Minute, errors.New("rate limited"))
```

## Database Support

The library uses GORM, supporting:

- **SQLite** - Great for development and single-instance deployments
- **PostgreSQL** - Recommended for production and distributed workers
- **MySQL** - Supported via GORM drivers

```go
// PostgreSQL
import "gorm.io/driver/postgres"

db, _ := gorm.Open(postgres.Open("host=localhost user=app dbname=jobs"), &gorm.Config{})
storage := jobs.NewGormStorage(db)

// With connection pool tuning
storage := jobs.NewGormStorageWithPool(db, jobs.HighConcurrencyPoolConfig())
```

## Package Structure

The library is organized into a layered architecture with a clean facade:

```
simple-durable-jobs/
├── jobs.go                    # Root facade - import this package
├── pause.go                   # Standalone pause/resume functions
├── hugo.toml                  # Documentation site config (Hextra theme)
├── content/                   # Hugo documentation pages
├── layouts/                   # Hugo layout overrides
├── pkg/
│   ├── core/                  # Domain models (Job, FanOut, Storage, Event, errors)
│   ├── storage/               # GormStorage implementation
│   ├── queue/                 # Queue orchestration, event system, pause operations
│   ├── worker/                # Worker processing, pause/resume, configuration
│   ├── schedule/              # Schedule implementations (Every, Daily, Cron)
│   ├── call/                  # Durable Call[T] function
│   ├── fanout/                # Fan-out/fan-in patterns (Sub, FanOut, helpers)
│   ├── otel/                  # Optional OpenTelemetry tracing integration
│   ├── jobctx/                # Job context helpers (JobFromContext, phase checkpoints)
│   ├── security/              # Validation and sanitization
│   └── internal/              # Private implementation details
├── ui/                        # Embeddable web UI dashboard
│   ├── frontend/              # Svelte SPA (built with Vite)
│   ├── handler.go             # HTTP handler factory
│   ├── options.go             # UI configuration options
│   ├── service.go             # Connect-RPC service implementation
│   ├── stats_collector.go     # Event-driven stats aggregation
│   └── gen/                   # Generated protobuf/Connect-RPC code
└── examples/                  # Usage examples
```

Users should import the root package:

```go
import jobs "github.com/jdziat/simple-durable-jobs"
```

All public types and functions are re-exported through the facade for a clean API.

## Examples

- [Basic Usage](./examples/basic/) - Simple job processing
- [Workflows](./examples/workflow/) - Multi-step durable workflows
- [Scheduled Jobs](./examples/scheduled/) - Recurring job scheduling
- [Distributed](./examples/distributed/) - Multiple workers

## License

MIT License - see [LICENSE](./LICENSE) for details.
