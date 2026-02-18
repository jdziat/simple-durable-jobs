---
title: "Embedded Web UI"
weight: 4
---

# Embedded Web UI

The `ui` package provides a full-featured monitoring dashboard that embeds directly into your Go application. It serves a Svelte SPA frontend alongside a Connect-RPC API, giving you real-time visibility into job queues, execution history, and worker status without deploying a separate service.

## Setup

Import the `ui` package and mount the handler on any `http.ServeMux` or router.

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    jobs "github.com/jdziat/simple-durable-jobs"
    "github.com/jdziat/simple-durable-jobs/ui"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    db, _ := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
    storage := jobs.NewGormStorage(db)
    storage.Migrate(context.Background())
    queue := jobs.New(storage)

    // Register your job handlers
    queue.Register("example", func(ctx context.Context, args string) error {
        return nil
    })

    // Start worker
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    worker := queue.NewWorker()
    go worker.Start(ctx)

    // Mount the UI dashboard
    mux := http.NewServeMux()
    mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage,
        ui.WithQueue(queue),
        ui.WithContext(ctx),
        ui.WithStatsRetention(7 * 24 * time.Hour),
        ui.WithMiddleware(func(next http.Handler) http.Handler {
            return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                // Add authentication, logging, etc.
                next.ServeHTTP(w, r)
            })
        }),
    )))

    log.Println("Dashboard available at http://localhost:8080/jobs/")
    log.Fatal(http.ListenAndServe(":8080", mux))
}
```

The `ui.Handler` function returns an `http.Handler` that serves both the Connect-RPC API and the embedded Svelte SPA frontend. It uses H2C (HTTP/2 over cleartext) internally to support Connect streaming without requiring TLS. If the frontend has not been built, a placeholder page is shown with instructions.

## Options

All options are passed to `ui.Handler` as variadic arguments.

### WithQueue

```go
ui.WithQueue(queue)
```

Provides access to the queue instance. This enables two features:

- **Event streaming**: The `WatchEvents` RPC method subscribes to real-time job events through the queue's event bus.
- **Scheduled jobs view**: The `ListScheduledJobs` RPC method reads registered schedules from the queue.

Without this option, event streaming returns no events and the scheduled jobs list is empty.

### WithContext

```go
ui.WithContext(ctx)
```

Provides a lifecycle context for background goroutines, primarily the stats collector. When the context is cancelled, the stats collector flushes any pending counters (with a 5-second timeout) and exits gracefully.

If not provided, `context.Background()` is used and background goroutines run until the process exits.

### WithStatsRetention

```go
ui.WithStatsRetention(7 * 24 * time.Hour)
```

Controls how long historical stats rows are kept in the database. Older rows are pruned automatically during each flush cycle. The default is 7 days.

### WithMiddleware

```go
ui.WithMiddleware(func(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Check auth, set CORS headers, log requests, etc.
        next.ServeHTTP(w, r)
    })
})
```

Wraps the entire handler (both API and frontend) with middleware. This is the recommended place to add authentication, CORS, request logging, or any other cross-cutting HTTP concerns. The middleware is applied as the outermost layer, after H2C setup.

## Dashboard Features

The embedded Svelte SPA provides a browser-based interface for monitoring and managing jobs.

### Queue Overview

The dashboard home screen shows aggregate statistics across all queues:

- **Pending** -- jobs waiting to be picked up by a worker.
- **Running** -- jobs currently being executed.
- **Completed** -- jobs that finished successfully.
- **Failed** -- jobs that exhausted all retry attempts.

Each queue is listed individually with its own breakdown of these counts.

### Job Browser

A paginated, filterable view of all jobs. You can filter by:

- **Status** -- pending, running, completed, or failed.
- **Queue** -- show jobs from a specific queue.
- **Type** -- filter by job type name.
- **Search** -- free-text search across job ID and arguments.

### Job Detail

Selecting a job shows its full details including arguments, timing, error messages, retry count, and all checkpoints recorded during workflow execution.

### Job Actions

From the UI you can perform management actions on jobs:

- **Retry** a failed job (resets it to pending).
- **Delete** a job.
- **Bulk retry** or **bulk delete** multiple selected jobs.
- **Purge** an entire queue by status.

### Scheduled Jobs

When `WithQueue` is provided, the dashboard lists all registered scheduled jobs with their schedule expressions and target queues.

### Historical Charts

When the stats collector is active (requires `WithQueue` and a GORM-backed storage), the dashboard shows time-series charts of completed and failed jobs. The history can be viewed over three periods: 1 hour, 24 hours, and 7 days.

### Real-Time Events

The dashboard uses server-streaming to display job events as they happen. Events include job started, completed, failed, retrying, paused, resumed, and queue/worker pause/resume notifications. Events can be filtered by queue.

## Stats Collector

The stats collector is a background goroutine that captures throughput and queue-depth metrics for the dashboard's historical charts. It starts automatically when all of the following are true:

1. `WithQueue` is provided.
2. The underlying storage implements a GORM-backed interface (exposes a `DB() *gorm.DB` method).

### How It Works

The collector subscribes to the queue's event bus and listens for three event types:

- `JobCompleted` -- increments the completed counter for the job's queue.
- `JobFailed` -- increments the failed counter.
- `JobRetrying` -- increments the retried counter.

Counters are accumulated in memory per queue.

Every 1 minute, the collector performs three operations:

1. **Flush** -- writes the accumulated counters to the database as a `JobStat` row bucketed by the current minute. Counters are then reset to zero.
2. **Snapshot** -- queries pending and running job counts from storage and writes them as queue-depth data points.
3. **Prune** -- deletes stats rows older than the configured retention period.

On context cancellation (graceful shutdown), the collector flushes any remaining counters with a 5-second timeout before exiting.

### Stats Model

Stats are stored in `JobStat` rows with the following fields:

| Field       | Type      | Description                          |
|-------------|-----------|--------------------------------------|
| Queue       | string    | Queue name                           |
| Timestamp   | time.Time | Minute-bucketed timestamp            |
| Pending     | int64     | Snapshot of pending jobs at this time |
| Running     | int64     | Snapshot of running jobs at this time |
| Completed   | int64     | Jobs completed during this minute    |
| Failed      | int64     | Jobs failed during this minute       |
| Retried     | int64     | Jobs retried during this minute      |

The `StatsStorage` interface defines the persistence layer:

```go
type StatsStorage interface {
    MigrateStats(ctx context.Context) error
    UpsertStatCounters(ctx context.Context, queue string, ts time.Time,
        completed, failed, retried int64) error
    SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time,
        pending, running int64) error
    GetStatsHistory(ctx context.Context, queue string,
        since time.Time, until time.Time) ([]JobStat, error)
    PruneStats(ctx context.Context, before time.Time) (int64, error)
}
```

A GORM-backed implementation (`GormStatsStorage`) is provided out of the box and is automatically configured when the storage layer supports it.

## Connect-RPC API

The UI exposes a Connect-RPC service (`jobs.v1.JobsService`) with 12 methods. These can also be called programmatically from any Connect, gRPC, or gRPC-Web client.

| Method              | Type             | Description                                                         |
|---------------------|------------------|---------------------------------------------------------------------|
| `GetStats`          | Unary            | Returns aggregate statistics (pending, running, completed, failed) per queue and totals. |
| `GetStatsHistory`   | Unary            | Returns historical stats data points filtered by period (`1h`, `24h`, `7d`). Returns completed and failed time series. |
| `ListJobs`          | Unary            | Paginated job listing with filters for status, queue, type, and free-text search. Default page size is 50, maximum is 100. |
| `GetJob`            | Unary            | Returns a single job with its full details and all associated checkpoints. |
| `RetryJob`          | Unary            | Resets a failed job back to pending status so it will be picked up again. |
| `DeleteJob`         | Unary            | Permanently removes a job from storage.                              |
| `BulkRetryJobs`     | Unary            | Retries multiple jobs by ID. Returns the count of successfully retried jobs. |
| `BulkDeleteJobs`    | Unary            | Deletes multiple jobs by ID. Returns the count of successfully deleted jobs. |
| `ListQueues`        | Unary            | Returns all queues with per-queue statistics.                        |
| `PurgeQueue`        | Unary            | Deletes jobs from a named queue filtered by status.                  |
| `ListScheduledJobs` | Unary            | Returns all registered scheduled jobs with their schedule expressions and target queues. Requires `WithQueue`. |
| `WatchEvents`       | Server streaming | Streams real-time job events. Supports filtering by queue names. Maximum 50 concurrent streams; additional connections receive a `ResourceExhausted` error. |

## Advanced Topics

### UIStorage Interface

The dashboard works with any `core.Storage` implementation, but provides enhanced functionality when the storage also implements the `UIStorage` interface:

```go
type UIStorage interface {
    core.Storage
    GetQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error)
    SearchJobs(ctx context.Context, filter JobFilter) ([]*core.Job, int64, error)
    RetryJob(ctx context.Context, jobID string) (*core.Job, error)
    DeleteJob(ctx context.Context, jobID string) error
    PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error)
}
```

When `UIStorage` is available:

- **GetQueueStats** runs an optimized aggregation query instead of fetching all jobs.
- **SearchJobs** supports server-side pagination, filtering, and search with no cap on total job count.
- **RetryJob** and **DeleteJob** perform direct database mutations.
- **PurgeJobs** deletes jobs from a queue by status in a single query.

Without `UIStorage`, the service falls back to basic queries using the `core.Storage` interface. These fallback queries are capped at 1000 jobs per status to prevent excessive memory usage, and retry/delete/purge operations return an `Unimplemented` error.

### Stream Limits

The `WatchEvents` endpoint enforces a maximum of 50 concurrent streaming connections. This limit is tracked with an atomic counter. When the limit is reached, new connections receive a `ResourceExhausted` error and should retry with backoff.

Streams can be filtered by queue name by passing a list of queue names in the request. When no filter is specified, all events are delivered. Each stream subscribes to the queue's event bus and forwards events until the client disconnects or the server context is cancelled.
