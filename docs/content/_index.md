---
title: "Simple Durable Jobs"
type: docs
---

# Simple Durable Jobs

A Go library for durable job queues with checkpointed workflows.

## Why Simple Durable Jobs?

Building reliable background job processing is hard. Jobs fail, servers crash, and you need to ensure work gets done exactly once. Simple Durable Jobs provides:

- **Durability** - Jobs persist to a database and survive crashes
- **Checkpointing** - Multi-step workflows resume from where they left off
- **Simplicity** - Clean API that gets out of your way
- **Type Safety** - Compile-time checks for job arguments

## Features

| Feature | Description |
|---------|-------------|
| Background Jobs | Fire-and-forget task processing |
| Durable Workflows | Multi-step workflows with automatic checkpointing |
| Fan-Out/Fan-In | Spawn parallel sub-jobs, wait for results, aggregate |
| Crash Recovery | Jobs resume from the last successful checkpoint |
| Pause/Resume | Pause and resume jobs, queues, or workers (graceful or aggressive) |
| Scheduled Jobs | Cron, daily, weekly, and interval scheduling |
| Priority Queues | Higher priority jobs run first |
| Retries | Configurable retry logic with exponential backoff |
| Observability | Hooks, event streams, and custom events |
| Web UI | Embeddable real-time monitoring dashboard |
| Unique Jobs | Deduplicate jobs by key |
| Stale Lock Reaper | Automatically reclaims stuck running jobs whose locks have expired |
| Connection Pool Presets | Pre-built pool configurations for different workloads |
| Per-Queue Concurrency | Independent concurrency limits per queue |

## Installation

```bash
go get github.com/jdziat/simple-durable-jobs
```

## Quick Example

```go
package main

import (
    "context"
    "fmt"

    jobs "github.com/jdziat/simple-durable-jobs"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    // Setup
    db, _ := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
    storage := jobs.NewGormStorage(db)
    storage.Migrate(context.Background())
    queue := jobs.New(storage)

    // Register handler
    queue.Register("greet", func(ctx context.Context, name string) error {
        fmt.Printf("Hello, %s!\n", name)
        return nil
    })

    // Enqueue job
    queue.Enqueue(context.Background(), "greet", "World")

    // Process jobs
    worker := queue.NewWorker()
    worker.Start(context.Background())
}
```

## Documentation

- [Getting Started](./docs/getting-started/) - Installation and basic usage
- [API Reference](./docs/api-reference/) - Complete API documentation
- [Examples](./docs/examples/) - Code examples for common use cases
- [Embedded Web UI](./docs/embedded-ui/) - Dashboard setup and configuration
- [Advanced Topics](./docs/advanced/) - Stale lock reaper, pool configuration, storage retry

## Links

- [GitHub Repository](https://github.com/jdziat/simple-durable-jobs)
- [Go Package Documentation](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs)
- [Report Issues](https://github.com/jdziat/simple-durable-jobs/issues)
