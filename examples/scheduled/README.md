# Scheduled Jobs

This example demonstrates recurring job scheduling with various schedule types.

## What It Does

- Registers 4 job handlers: health check, cleanup, daily report, and database backup
- Schedules them at different intervals using `jobs.Every()` for the demo
- Shows commented-out examples of `jobs.Daily()`, `jobs.Weekly()`, and `jobs.Cron()` for production use
- Starts a worker with the **scheduler enabled** (`jobs.WithScheduler(true)`)

## Prerequisites

- Go 1.21+
- No external services needed (uses SQLite)

## Running

```bash
go run main.go
```

The example runs for 60 seconds (or until Ctrl+C) to show the recurring schedules in action.

## Expected Output

```
Scheduled: health-check (every 5 seconds)
Scheduled: cleanup-temp-files (every 10 seconds)
Scheduled: send-daily-report (every 15 seconds for demo)
Scheduled: backup-database (every 20 seconds for demo)

Starting worker with scheduler...
Running for 60 seconds (Ctrl+C to stop early)...
[14:30:05] Health check: OK
[14:30:10] Health check: OK
[14:30:10] Cleaning up temporary files...
[14:30:10] Cleanup complete
[14:30:15] Health check: OK
[14:30:15] Generating daily report...
[14:30:15] Daily report sent
...
```

## Key Concepts

- **`jobs.Every(d)`** runs a job at fixed intervals. Good for health checks, polling, and maintenance.
- **`jobs.Daily(hour, minute)`** runs once per day at a specific time (UTC).
- **`jobs.Weekly(day, hour, minute)`** runs once per week on a specific day and time (UTC).
- **`jobs.Cron(expr)`** accepts standard 5-field cron expressions for complex schedules.
- **`jobs.WithScheduler(true)`** must be passed to the worker to enable the scheduler. Without it, scheduled jobs won't fire.

## Tips

- The scheduler is **per-worker**. If you run multiple workers with the scheduler enabled, duplicate scheduled jobs may be enqueued. Use `jobs.Unique()` on your scheduled jobs or enable the scheduler on only one worker.
- Schedule intervals are measured from when the scheduler ticks, not from when the previous job completed.
- For production, use `jobs.Daily()` or `jobs.Cron()` instead of short `jobs.Every()` intervals to avoid overwhelming your queue.
- Scheduled job handlers typically take `struct{}` or no meaningful arguments since they're triggered by time, not input data.

## Related Documentation

- [Getting Started - Scheduled Jobs](https://jdziat.github.io/simple-durable-jobs/docs/getting-started/#scheduled-jobs)
- [API Reference - Schedules](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/#schedules)
- [Examples](https://jdziat.github.io/simple-durable-jobs/docs/examples/)
