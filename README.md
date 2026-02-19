# Simple Durable Jobs

[![Go Reference](https://pkg.go.dev/badge/github.com/jdziat/simple-durable-jobs.svg)](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs)
[![CI](https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/jdziat/simple-durable-jobs)](https://goreportcard.com/report/github.com/jdziat/simple-durable-jobs)
[![codecov](https://codecov.io/gh/jdziat/simple-durable-jobs/branch/main/graph/badge.svg)](https://codecov.io/gh/jdziat/simple-durable-jobs)

A Go library for durable job queues with checkpointed workflows, inspired by [River](https://riverqueue.com/), [Temporal](https://temporal.io/), and async patterns.

## Documentation

- [Getting Started](https://jdziat.github.io/simple-durable-jobs/docs/getting-started/)
- [API Reference](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/)
- [Examples](https://jdziat.github.io/simple-durable-jobs/docs/examples/)
- [Embedded Web UI](https://jdziat.github.io/simple-durable-jobs/docs/embedded-ui/)
- [Live Demo](https://jdziat.github.io/simple-durable-jobs/docs/live-demo/)
- [Advanced Topics](https://jdziat.github.io/simple-durable-jobs/docs/advanced/) - Stale lock reaper, pool configuration, storage retry

## Features

- **Background Jobs** - Fire-and-forget task processing
- **Durable Workflows** - Multi-step workflows with automatic checkpointing
- **Fan-Out/Fan-In** - Spawn parallel sub-jobs, wait for results, aggregate
- **Crash Recovery** - Jobs resume from the last successful checkpoint
- **Pause/Resume** - Pause and resume individual jobs, queues, or workers (graceful or aggressive)
- **Scheduled Jobs** - Cron, daily, weekly, and interval-based scheduling
- **Priority Queues** - Higher priority jobs run first
- **Retries with Backoff** - Configurable retry logic with exponential backoff
- **Observability** - Hooks, event streams, and an embedded web dashboard
- **Embedded Web UI** - Real-time monitoring dashboard with stats and event streaming
- **Stale Lock Reaper** - Automatically reclaims stuck running jobs after worker crashes
- **Connection Pool Presets** - Pre-built pool configurations for different workloads
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
    // Setup database
    db, _ := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
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
// Pause/resume a specific job
queue.PauseJob(ctx, jobID)
queue.ResumeJob(ctx, jobID)

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
    jobs.Priority(100),              // Higher priority runs first
    jobs.Retries(5),                 // Max retry attempts
    jobs.Delay(10 * time.Second),    // Delay execution
    jobs.QueueOpt("critical"),       // Assign to named queue
    jobs.Unique("order-123"),        // Deduplicate by key
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
├── pkg/
│   ├── core/                  # Domain models (Job, FanOut, Storage, Event, errors)
│   ├── storage/               # GormStorage implementation
│   ├── queue/                 # Queue orchestration, event system, pause operations
│   ├── worker/                # Worker processing, pause/resume, configuration
│   ├── schedule/              # Schedule implementations (Every, Daily, Cron)
│   ├── call/                  # Durable Call[T] function
│   ├── fanout/                # Fan-out/fan-in patterns (Sub, FanOut, helpers)
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
