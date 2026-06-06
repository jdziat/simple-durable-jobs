---
title: "Storage Operation Retry"
weight: 3
---

Transient database errors -- connection drops, query timeouts, lock-wait
timeouts, deadlocks -- are a fact of life in production systems. Without retry
logic a single blip can cause a job to be lost or stuck in an incorrect state.
The `simple-durable-jobs` library retries storage operations automatically using
exponential backoff with jitter so that your workers ride through brief database
disruptions without intervention.

## RetryConfig

All retry behavior is controlled by a single configuration struct:

```go
type RetryConfig struct {
    MaxAttempts       int           // Maximum attempts including initial (default: 5)
    InitialBackoff    time.Duration // First backoff duration (default: 100ms)
    MaxBackoff        time.Duration // Maximum backoff cap (default: 5s)
    BackoffMultiplier float64       // Multiplier per attempt (default: 2.0)
    JitterFraction    float64       // Randomization fraction 0-1 (default: 0.1)
}
```

### Default Configuration

`DefaultRetryConfig()` returns:

| Field | Value |
|-------|-------|
| MaxAttempts | 5 |
| InitialBackoff | 100 ms |
| MaxBackoff | 5 s |
| BackoffMultiplier | 2.0 |
| JitterFraction | 0.1 (10%) |

These defaults handle the vast majority of transient failures (brief connection
resets, short deadlocks) without delaying job processing noticeably.

## Worker Options

### Full Control

Use `WithStorageRetry` to provide a complete `RetryConfig` for storage
operations (Complete, Fail, SaveCheckpoint, heartbeat, fan-out counter
increments):

```go
worker := queue.NewWorker(
    jobs.WithStorageRetry(jobs.RetryConfig{
        MaxAttempts:       10,
        InitialBackoff:    200 * time.Millisecond,
        MaxBackoff:        10 * time.Second,
        BackoffMultiplier: 2.0,
        JitterFraction:    0.2,
    }),
)
```

### Separate Dequeue Retry

Dequeue operations poll the database repeatedly. When the database goes down,
you typically want longer backoff intervals to avoid hammering it with tight-loop
queries. Use `WithDequeueRetry` to configure dequeue retry independently:

```go
worker := queue.NewWorker(
    jobs.WithDequeueRetry(jobs.RetryConfig{
        MaxAttempts:    3,
        InitialBackoff: 500 * time.Millisecond,
        MaxBackoff:     5 * time.Second,
    }),
)
```

The default dequeue retry configuration (applied when you do not set one
explicitly) uses:

| Field | Value |
|-------|-------|
| MaxAttempts | 3 |
| InitialBackoff | 500 ms |
| MaxBackoff | 10 s |
| BackoffMultiplier | 2.0 |
| JitterFraction | 0.2 (20%) |

### Convenience: Set Max Attempts Only

If the default backoff timing is fine and you only want to change how many times
operations are retried, use the shorthand:

```go
worker := queue.NewWorker(jobs.WithRetryAttempts(10))
```

This updates the storage retry config while keeping all other fields at their
defaults.

### Disable Retry Entirely

For testing or when your infrastructure provides its own retry layer (for
example, a connection-pooling proxy like PgBouncer with retry), you can disable
retry:

```go
worker := queue.NewWorker(jobs.DisableRetry())
```

This sets `MaxAttempts` to 1 for both storage and dequeue operations, meaning
every operation is attempted exactly once.

## What Gets Retried

The library applies retry in two distinct scopes:

### Storage Retry

Used for operations that modify job state after a job has been dequeued:

- **Complete** -- marking a job as successfully finished.
- **Fail** -- recording an error and optionally scheduling a retry.
- **SaveCheckpoint** -- persisting durable call results.
- **Heartbeat** -- extending the lock on a running job.
- **Fan-out counter increments** -- updating completed/failed counts on fan-out
  records.
- **Resume job** -- moving a waiting job back to pending after fan-out
  completion.

A failure in any of these operations is retried with the storage retry config.

### Dequeue Retry

Used for the job polling loop:

- **Dequeue** -- fetching and locking the next available job from the database.

A failure here is retried with the dequeue retry config, which defaults to
longer backoff to avoid tight-loop polling during an outage.

### What Is Never Retried

Context cancellation and deadline errors are **never** retried regardless of
configuration. If the worker's context is cancelled (for example, during
shutdown), the retry loop exits immediately and returns the context error.

## Backoff Behavior

The retry mechanism uses exponential backoff with jitter:

### Exponential Growth

Each successive attempt multiplies the backoff duration by `BackoffMultiplier`:

```
Attempt 1: InitialBackoff                          (100ms)
Attempt 2: InitialBackoff * BackoffMultiplier       (200ms)
Attempt 3: InitialBackoff * BackoffMultiplier^2     (400ms)
Attempt 4: InitialBackoff * BackoffMultiplier^3     (800ms)
Attempt 5: InitialBackoff * BackoffMultiplier^4     (1600ms)
```

### Backoff Cap

The computed backoff is capped at `MaxBackoff`. Once the backoff reaches the
cap, all subsequent attempts wait for `MaxBackoff`:

```
Attempt 1: 100ms
Attempt 2: 200ms
Attempt 3: 400ms
Attempt 4: 800ms
Attempt 5: 1600ms
Attempt 6: 3200ms
Attempt 7: 5000ms  (capped at MaxBackoff)
Attempt 8: 5000ms  (capped at MaxBackoff)
```

### Jitter

Before sleeping, the backoff is adjusted by a random amount in the range
`[-JitterFraction * backoff, +JitterFraction * backoff]`. With the default 10%
jitter, a 1-second backoff becomes a sleep between 900ms and 1100ms.

Jitter prevents the **thundering herd** problem where multiple workers that
failed at the same instant all retry at the exact same time and overload the
database again.

## Combining Storage Retry with Job Retry

Storage retry and job retry are independent mechanisms that operate at different
levels:

| Mechanism | Scope | Purpose |
|-----------|-------|---------|
| **Storage retry** (`RetryConfig`) | Database operations within a single job execution | Handle transient DB errors (connection drops, deadlocks) |
| **Job retry** (`MaxRetries` on the job) | Re-execution of the entire job handler | Handle application-level failures (HTTP 503, temporary service outage) |

A single job execution may trigger several storage retries (for example, three
attempts to call `Complete`), all of which are invisible to the job handler. If
the handler itself returns an error, the job-level retry logic kicks in and the
job is re-enqueued according to its `MaxRetries` setting.
