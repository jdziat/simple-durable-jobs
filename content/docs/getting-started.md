---
title: "Getting Started"
weight: 1
---

This guide walks you through setting up Simple Durable Jobs in your Go application.

## Installation

```bash
go get github.com/jdziat/simple-durable-jobs
```

You'll also need a database driver. For development, SQLite works great:

```bash
go get gorm.io/driver/sqlite
```

For production, PostgreSQL is recommended:

```bash
go get gorm.io/driver/postgres
```

## Basic Setup

### 1. Create Storage and Queue

```go
package main

import (
    "context"

    jobs "github.com/jdziat/simple-durable-jobs"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    // Open database connection
    db, err := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
    if err != nil {
        panic(err)
    }

    // Create storage layer
    storage := jobs.NewGormStorage(db)

    // Run migrations to create tables
    if err := storage.Migrate(context.Background()); err != nil {
        panic(err)
    }

    // Create the queue
    queue := jobs.New(storage)
}
```

### 2. Register Job Handlers

Job handlers are functions that process jobs. They receive a context and typed arguments:

```go
// Handler with struct arguments
type SendEmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

queue.Register("send-email", func(ctx context.Context, args SendEmailArgs) error {
    // Send the email...
    fmt.Printf("Sending email to %s: %s\n", args.To, args.Subject)
    return nil
})

// Handler with primitive arguments
queue.Register("process-number", func(ctx context.Context, n int) error {
    fmt.Printf("Processing: %d\n", n)
    return nil
})

// Handler that returns a value (for use with Call)
queue.Register("calculate", func(ctx context.Context, x int) (int, error) {
    return x * 2, nil
})
```

### 3. Enqueue Jobs

```go
ctx := context.Background()

// Basic enqueue
jobID, err := queue.Enqueue(ctx, "send-email", SendEmailArgs{
    To:      "user@example.com",
    Subject: "Welcome!",
    Body:    "Thanks for signing up.",
})

// With options
jobID, err = queue.Enqueue(ctx, "send-email", args,
    jobs.Priority(100),           // Higher runs first
    jobs.Retries(5),              // Retry up to 5 times
    jobs.Delay(time.Minute),      // Wait 1 minute before running
    jobs.QueueOpt("emails"),      // Use specific queue
)
```

### 4. Start a Worker

```go
// Create and start worker
worker := queue.NewWorker()
worker.Start(ctx) // Blocks until context is cancelled
```

For graceful shutdown:

```go
ctx, cancel := context.WithCancel(context.Background())

// Start worker in goroutine
go worker.Start(ctx)

// On shutdown signal
cancel()
```

## Worker Configuration

Configure worker concurrency and queues:

```go
worker := queue.NewWorker(
    // Process "default" queue with 10 concurrent workers
    jobs.WorkerQueue("default", jobs.Concurrency(10)),

    // Process "emails" queue with 5 concurrent workers
    jobs.WorkerQueue("emails", jobs.Concurrency(5)),

    // Enable the scheduler for recurring jobs
    jobs.WithScheduler(true),
)
```

{{< callout type="info" >}}
When `Concurrency()` is used inside `WorkerQueue()`, it applies only to that queue. Each queue independently tracks how many jobs it has running and only dequeues more when below its limit.
{{< /callout >}}

## Durable Workflows

For multi-step workflows, use `jobs.Call` to create checkpoints:

```go
queue.Register("process-order", func(ctx context.Context, order Order) error {
    // Step 1: Validate (checkpointed)
    validated, err := jobs.Call[Order](ctx, "validate", order)
    if err != nil {
        return err
    }

    // Step 2: Charge payment (checkpointed)
    // If this fails, step 1 won't re-run on retry
    receipt, err := jobs.Call[string](ctx, "charge", validated.Total)
    if err != nil {
        return err
    }

    // Step 3: Ship (checkpointed)
    _, err = jobs.Call[any](ctx, "ship", validated.Items)
    return err
})
```

## Scheduled Jobs

Set up recurring jobs:

```go
// Every 5 minutes
queue.Schedule("cleanup", jobs.Every(5 * time.Minute))

// Daily at 9:00 AM
queue.Schedule("report", jobs.Daily(9, 0))

// Weekly on Sunday at 2:00 AM
queue.Schedule("backup", jobs.Weekly(time.Sunday, 2, 0))

// Cron expression
queue.Schedule("hourly", jobs.Cron("0 * * * *"))

// Remember to enable scheduler in worker
worker := queue.NewWorker(jobs.WithScheduler(true))
```

## Observability

Add hooks to monitor job execution:

```go
queue.OnJobStart(func(ctx context.Context, job *jobs.Job) {
    log.Printf("Job %s started", job.ID)
})

queue.OnJobComplete(func(ctx context.Context, job *jobs.Job) {
    log.Printf("Job %s completed in %v", job.ID, job.CompletedAt.Sub(*job.StartedAt))
})

queue.OnJobFail(func(ctx context.Context, job *jobs.Job, err error) {
    log.Printf("Job %s failed: %v", job.ID, err)
})

queue.OnRetry(func(ctx context.Context, job *jobs.Job, attempt int, err error) {
    log.Printf("Job %s retrying (attempt %d): %v", job.ID, attempt, err)
})

// Event stream (remember to unsubscribe to prevent leaks)
events := queue.Events()
defer queue.Unsubscribe(events)
```

## Pause/Resume

Pause and resume at the job, queue, or worker level:

```go
// Pause a specific job
queue.PauseJob(ctx, jobID)
queue.ResumeJob(ctx, jobID)

// Pause an entire queue
queue.PauseQueue(ctx, "emails")
queue.ResumeQueue(ctx, "emails")

// Pause a worker (graceful: finish running jobs, stop picking new ones)
worker.Pause(jobs.PauseModeGraceful)
worker.Resume()

// Aggressive: cancel running jobs immediately
worker.Pause(jobs.PauseModeAggressive)
```

## Embedded Web UI

Mount a monitoring dashboard into your HTTP server:

```go
import "github.com/jdziat/simple-durable-jobs/ui"

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage,
    ui.WithQueue(queue),      // Enable event streaming
    ui.WithContext(ctx),      // Lifecycle context for graceful shutdown
)))
```

The dashboard shows real-time queue stats, historical charts, live event streaming, and job management controls.

## Error Handling

Control retry behavior with special error types:

```go
// Don't retry this job
return jobs.NoRetry(errors.New("invalid input"))

// Retry after specific duration
return jobs.RetryAfter(5 * time.Minute, errors.New("rate limited"))
```

## Production Setup

For production, use PostgreSQL:

```go
import "gorm.io/driver/postgres"

dsn := "host=localhost user=app password=secret dbname=jobs"
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
```

Run multiple workers for horizontal scaling:

```bash
# Terminal 1
./myapp -worker

# Terminal 2
./myapp -worker

# Terminal 3
./myapp -worker
```

Each worker will process jobs from the queue without duplicates.

### Stale Lock Reaper

Workers automatically reclaim jobs stuck in "running" status when their lock expires (e.g., after a worker crash). This runs in the background with configurable intervals:

```go
worker := queue.NewWorker(
    jobs.WithStaleLockInterval(5 * time.Minute), // How often to check (default: 5min)
    jobs.WithStaleLockAge(45 * time.Minute),      // How old a lock must be (default: 45min)
)
```

Set `WithStaleLockInterval(0)` to disable the reaper.

### Connection Pool Configuration

For production deployments, tune the database connection pool:

```go
// Use a preset
storage, err := jobs.NewGormStorageWithPool(db, jobs.HighConcurrencyPoolConfig())

// Or customize individual settings
storage, err := jobs.NewGormStorageWithPool(db,
    jobs.MaxOpenConns(50),
    jobs.MaxIdleConns(20),
    jobs.ConnMaxLifetime(10 * time.Minute),
    jobs.ConnMaxIdleTime(2 * time.Minute),
)
```

Available presets:

| Preset | MaxOpen | MaxIdle | MaxLifetime | MaxIdleTime | Use Case |
|--------|---------|---------|-------------|-------------|----------|
| `DefaultPoolConfig()` | 25 | 10 | 5min | 1min | General purpose |
| `HighConcurrencyPoolConfig()` | 100 | 25 | 10min | 2min | 50+ workers, high volume |
| `LowLatencyPoolConfig()` | 50 | 40 | 15min | 5min | Latency-sensitive |
| `ResourceConstrainedPoolConfig()` | 10 | 5 | 3min | 30s | Limited DB resources |

## Next Steps

- [API Reference](../api-reference/) - Complete API documentation
- [Examples](../examples/) - More code examples
- [Embedded Web UI](../embedded-ui/) - Dashboard setup and configuration
- [Advanced Topics](../advanced/) - Stale lock reaper, pool configuration, storage retry
- [GitHub](https://github.com/jdziat/simple-durable-jobs) - Source code and issues
