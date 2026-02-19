# Distributed Workers

This example demonstrates running multiple workers that process jobs from a shared queue.

## What It Does

- Supports two modes via command-line flags: **enqueue** mode and **worker** mode
- In enqueue mode, adds N jobs to the queue
- In worker mode, starts a worker with a unique ID that processes jobs concurrently
- Demonstrates job locking -- each job is processed by exactly one worker

## Prerequisites

- Go 1.21+
- No external services needed for the demo (uses SQLite with WAL mode)
- For production: PostgreSQL recommended for proper concurrent access

## Running

### Step 1: Enqueue Jobs

```bash
go run main.go -enqueue -jobs 100
```

### Step 2: Start Workers (in separate terminals)

```bash
# Terminal 1
go run main.go -worker worker-1

# Terminal 2
go run main.go -worker worker-2

# Terminal 3
go run main.go -worker worker-3
```

Press Ctrl+C in each terminal to shut down workers gracefully.

## Expected Output

**Enqueue mode:**
```
Enqueueing 100 jobs...
Enqueued 100 jobs
```

**Worker mode:**
```
[worker-1] Starting worker...
[worker-1] Started job abc12345
[worker-1] Processing item 0: Item-0
[worker-1] Completed item 0
[worker-1] Completed job abc12345
[worker-1] Started job def67890
[worker-1] Processing item 3: Item-3
...
```

Jobs are distributed across workers. You'll see different workers picking up different jobs.

## Key Concepts

- **Job locking** ensures each job is processed by exactly one worker. When a worker dequeues a job, it acquires an exclusive lock in the database.
- **Horizontal scaling** is achieved by simply starting more worker processes. No configuration changes needed.
- **Graceful shutdown** via Ctrl+C (SIGINT/SIGTERM) lets running jobs finish before the worker exits.
- **Worker concurrency** is set per-worker with `jobs.Concurrency(n)`. Each worker independently processes up to N jobs at a time.

## Tips

- **Use PostgreSQL for production.** SQLite supports WAL mode for basic concurrency but has limitations with multiple writer processes. PostgreSQL handles concurrent workers much better.
  ```go
  db, err := gorm.Open(postgres.Open("host=localhost user=app dbname=jobs"), &gorm.Config{})
  ```
- **Tune concurrency per worker.** If your jobs are I/O-bound (HTTP calls, file operations), higher concurrency (10-50) is fine. For CPU-bound jobs, match concurrency to available cores.
- **Monitor with hooks.** Add `OnJobStart`/`OnJobComplete`/`OnJobFail` hooks to track which worker processes each job and how long jobs take.
- **Stale lock reaper** runs automatically on each worker. If a worker crashes, its locked jobs are reclaimed by surviving workers after the lock expires (default: 45 minutes).

## Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-worker` | `worker-1` | Worker ID (used in log output) |
| `-enqueue` | `false` | Run in enqueue mode instead of worker mode |
| `-jobs` | `10` | Number of jobs to enqueue (only in enqueue mode) |

## Related Documentation

- [Getting Started - Production Setup](https://jdziat.github.io/simple-durable-jobs/docs/getting-started/#production-setup)
- [Advanced - Stale Lock Reaper](https://jdziat.github.io/simple-durable-jobs/docs/advanced/stale-lock-reaper/)
- [Advanced - Connection Pool Configuration](https://jdziat.github.io/simple-durable-jobs/docs/advanced/pool-configuration/)
- [Examples](https://jdziat.github.io/simple-durable-jobs/docs/examples/)
