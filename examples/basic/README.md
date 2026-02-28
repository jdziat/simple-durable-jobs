# Basic Job Processing

This example demonstrates fire-and-forget job processing with Simple Durable Jobs.

## What It Does

- Sets up a SQLite-backed job queue
- Registers a `send-email` handler that simulates sending emails
- Enqueues multiple jobs with different options (delay, priority)
- Hooks into job lifecycle events (start, complete, fail) for observability
- Starts a worker with concurrency of 2

## Prerequisites

- Go 1.21+
- No external services needed (uses SQLite)

## Running

```bash
go run main.go
```

The example runs for 10 seconds and then shuts down gracefully.

## Expected Output

```
Enqueued job abc12345
Enqueued job def67890
Enqueued job ghi24680
Enqueued delayed job jkl13579 (runs in 5s)
Enqueued high-priority job mno97531
Starting worker...
[START] Job mno97531 (send-email)
Sending email to urgent@example.com: URGENT: Action required
Email sent to urgent@example.com
[DONE] Job mno97531 completed
...
Shutting down...
Done!
```

High-priority jobs run before normal ones. The delayed job runs after 5 seconds.

## Key Concepts

- **Job handlers** receive a context and typed arguments. Register them with `queue.Register()`.
- **Enqueue options** like `jobs.Priority()`, `jobs.Delay()` control scheduling behavior.
- **Lifecycle hooks** (`OnJobStart`, `OnJobComplete`, `OnJobFail`) let you add observability without modifying handlers.
- **Graceful shutdown** is handled by cancelling the worker's context.

## Tips

- Use `jobs.Priority(n)` with higher values to prioritize urgent work. The default priority is 0.
- `jobs.Delay(d)` is useful for scheduling future work (e.g., send a reminder in 24 hours).
- Always handle `context.Context` in your handlers to support graceful cancellation.
- For production, switch from SQLite to PostgreSQL for better concurrency support.

## Related Documentation

- [Getting Started](https://jdziat.github.io/simple-durable-jobs/docs/getting-started/)
- [API Reference](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/)
- [Examples](https://jdziat.github.io/simple-durable-jobs/docs/examples/)
