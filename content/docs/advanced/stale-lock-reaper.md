---
title: "Stale Lock Reaper"
weight: 1
---

When a worker crashes mid-job, the job stays in "running" status with no one to
complete or fail it. The stale lock reaper is a background mechanism that detects
these abandoned jobs and resets them so another worker can pick them up.

## How Job Locking Works

Every time a worker dequeues a job, it acquires an exclusive lock on the job
record in the database:

1. The storage layer sets `LockedBy` to the worker's unique ID and
   `LockedUntil` to 45 minutes from now.
2. While the job is executing, the worker sends a **heartbeat** every 2 minutes
   that extends `LockedUntil` by another 45 minutes. This keeps the lock fresh
   for long-running jobs without requiring an excessively long initial lock
   window.
3. When the job completes or fails, the worker clears the lock fields.

If a worker crashes before it can clear the lock, the `LockedUntil` timestamp
eventually lapses and the job becomes reclaimable.

## The Stale Lock Reaper

Each worker starts a background goroutine called the stale lock reaper. It
performs the following cycle:

1. **Tick** -- wake up on a configurable interval (default: every 5 minutes).
2. **Scan** -- query the database for jobs whose `status` is `running` and whose
   `LockedUntil` timestamp is older than the current time minus `StaleLockAge`
   (default: 45 minutes).
3. **Reset** -- set those jobs back to `pending`, clear `LockedBy` and
   `LockedUntil`, so another worker can dequeue them.
4. **Log** -- if any jobs were reclaimed, emit a structured log line:

```
INFO released stale running jobs count=N
```

Because every worker runs its own reaper, the cluster self-heals even if only
one worker remains online.

## Configuration

Both tuning knobs are set through worker options:

```go
worker := queue.NewWorker(
    jobs.WithStaleLockInterval(5 * time.Minute), // How often to check (default: 5min)
    jobs.WithStaleLockAge(45 * time.Minute),      // Lock expiry threshold (default: 45min)
)
```

### Disabling the Reaper

If you run a single worker and prefer to handle stale jobs through external
monitoring, you can disable the reaper entirely:

```go
worker := queue.NewWorker(
    jobs.WithStaleLockInterval(0), // Disable the stale lock reaper
)
```

### Choosing Values

| Parameter | Default | Guidance |
|-----------|---------|----------|
| `WithStaleLockInterval` | 5 min | Lower values detect stale jobs faster but add more database queries. For high-throughput clusters, 2-3 minutes is reasonable. |
| `WithStaleLockAge` | 45 min | Must be at least as large as the lock duration (45 min) so that a lock that just expired is not immediately reaped before the heartbeat has a chance to extend it. Increase this if your jobs routinely run for hours and you want extra safety margin. |

The default 45-minute age matches the lock duration set by the storage layer.
During normal operation heartbeats extend locks every 2 minutes, so an active
job's lock is always well within the 45-minute window and will never be
reclaimed by the reaper.

## When the Reaper Helps

The reaper is your safety net against several failure modes:

- **Worker process crash or SIGKILL** -- the process is gone, no cleanup runs.
  The lock expires naturally and the reaper resets the job.
- **Network partition between worker and database** -- the worker cannot send
  heartbeats, so the lock expires. Once the partition heals, the reaper (running
  on any healthy worker) reclaims the job.
- **Long GC pause or resource starvation** -- if a worker is paused by the
  operating system long enough for the lock to expire, the reaper on a different
  worker can reclaim the job.

In all of these cases the job returns to `pending` status and will be retried by
the next available worker, preserving any checkpoints that were saved before the
failure.

## Interaction with Heartbeats and Retries

The heartbeat, lock, and reaper work together as a layered reliability
mechanism:

```
Heartbeat interval:  2 min   (keeps lock fresh)
Lock duration:       45 min  (initial window before heartbeat required)
Reaper age:          45 min  (how stale before reclaim)
Reaper interval:     5 min   (how often we check)
```

A job can only be reclaimed if **all** of the following are true:

1. Its status is `running`.
2. Its `LockedUntil` is at least `StaleLockAge` in the past.
3. No heartbeat has extended the lock in that window.

When a reclaimed job is dequeued again, its `Attempt` counter increments
normally. If it has already exhausted `MaxRetries`, the next failure will mark
it as permanently failed.
