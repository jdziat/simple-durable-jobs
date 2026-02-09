# Simple Durable Jobs

[![Go Reference](https://pkg.go.dev/badge/github.com/jdziat/simple-durable-jobs.svg)](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs)
[![CI](https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/jdziat/simple-durable-jobs)](https://goreportcard.com/report/github.com/jdziat/simple-durable-jobs)
[![codecov](https://codecov.io/gh/jdziat/simple-durable-jobs/branch/main/graph/badge.svg)](https://codecov.io/gh/jdziat/simple-durable-jobs)

A Go library for durable job queues with checkpointed workflows, inspired by [River](https://riverqueue.com/), [Temporal](https://temporal.io/), and async patterns.

## Features

- **Background Jobs** - Fire-and-forget task processing
- **Durable Workflows** - Multi-step workflows with automatic checkpointing
- **Crash Recovery** - Jobs resume from the last successful checkpoint
- **Scheduled Jobs** - Cron, daily, weekly, and interval-based scheduling
- **Priority Queues** - Higher priority jobs run first
- **Retries with Backoff** - Configurable retry logic with exponential backoff
- **Observability** - Hooks and event streams for monitoring
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
go func() {
    for event := range events {
        switch e := event.(type) {
        case *jobs.JobStarted:
            // Handle start
        case *jobs.JobCompleted:
            // Handle completion
        case *jobs.JobFailed:
            // Handle failure
        }
    }
}()
```

## Worker Configuration

```go
worker := queue.NewWorker(
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
    jobs.WorkerQueue("critical", jobs.Concurrency(5)),
    jobs.WithScheduler(true),
)
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
```

## Package Structure

The library is organized into a layered architecture with a clean facade:

```
simple-durable-jobs/
├── jobs.go                    # Root facade - import this package
├── pkg/
│   ├── core/                  # Domain models (Job, Storage, Event, errors)
│   ├── storage/               # GormStorage implementation
│   ├── queue/                 # Queue orchestration and options
│   ├── worker/                # Worker processing and configuration
│   ├── schedule/              # Schedule implementations (Every, Daily, Cron)
│   ├── call/                  # Durable Call[T] function
│   ├── security/              # Validation and sanitization
│   └── internal/              # Private implementation details
└── examples/                  # Usage examples
```

Users should import the root package:

```go
import jobs "github.com/jdziat/simple-durable-jobs"
```

All public types and functions are re-exported through the facade for a clean API.

## Documentation

- [Getting Started](https://jdziat.github.io/simple-durable-jobs/getting-started/)
- [API Reference](https://jdziat.github.io/simple-durable-jobs/api-reference/)
- [Examples](./examples/)

## Examples

- [Basic Usage](./examples/basic/) - Simple job processing
- [Workflows](./examples/workflow/) - Multi-step durable workflows
- [Scheduled Jobs](./examples/scheduled/) - Recurring job scheduling
- [Distributed](./examples/distributed/) - Multiple workers

## License

MIT License - see [LICENSE](./LICENSE) for details.
