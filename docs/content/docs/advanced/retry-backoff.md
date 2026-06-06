---
title: "Job Retry Backoff"
weight: 4
---

Job retry backoff controls when a failed job handler is re-executed. This is
different from storage-operation retry: storage retry protects individual
database calls such as `Complete`, `Fail`, `Heartbeat`, and `Dequeue`, while job
retry backoff schedules the whole job to run again after the handler returns an
error.

## BackoffPolicy

`BackoffPolicy` is the job retry extension point:

```go
type BackoffPolicy interface {
    NextRetry(attempt int, err error) time.Duration
}
```

The worker calls `NextRetry` when a handler error should be retried. The
computed delay is stored through the normal `Fail(retryAt)` path; no additional
database state is required.

## ExponentialBackoff

`ExponentialBackoff` is the built-in policy:

```go
type ExponentialBackoff struct {
    InitialInterval time.Duration
    Multiplier      float64
    MaxInterval     time.Duration
    JitterFraction  float64
}
```

With `JitterFraction: 0`, the default curve is the legacy job retry sequence:
1s, 2s, 4s, 8s, and so on, capped at `MaxInterval`. The library default uses
10% jitter so workers that fail at the same time do not all retry in lockstep.

```go
worker := queue.NewWorker(jobs.WithBackoff(jobs.ExponentialBackoff{
    InitialInterval: 5 * time.Second,
    Multiplier:      2,
    MaxInterval:     5 * time.Minute,
    JitterFraction:  0.2,
}))
```

`DefaultBackoffPolicy()` returns the library default policy.

## BackoffFunc

Use `BackoffFunc` for small policies or River/Asynq-style closures:

```go
worker := queue.NewWorker(jobs.WithBackoff(jobs.BackoffFunc(
    func(attempt int, err error) time.Duration {
        if errors.Is(err, ErrRateLimited) {
            return 30 * time.Second
        }
        return time.Second << min(attempt, 5)
    },
)))
```

## Worker Default

`WithBackoff(policy)` sets the worker-default policy for job re-execution. It
does not affect storage-operation retry.

```go
worker := queue.NewWorker(jobs.WithBackoff(jobs.ExponentialBackoff{
    InitialInterval: time.Second,
    Multiplier:      1.5,
    MaxInterval:     time.Minute,
    JitterFraction:  0.1,
}))
```

## Per-Handler Override

`WithHandlerBackoff(policy)` attaches a policy to a registered handler. This is
useful when one job type calls a fragile external service and should back off
more aggressively than the rest of the worker.

```go
queue.Register("sync-account", syncAccount,
    jobs.WithHandlerBackoff(jobs.ExponentialBackoff{
        InitialInterval: 30 * time.Second,
        Multiplier:      2,
        MaxInterval:     10 * time.Minute,
        JitterFraction:  0.15,
    }),
)
```

## Precedence

Retry delay resolution follows this order:

| Source | Behavior |
|---|---|
| `NoRetry(err)` | Terminal failure; no retry is scheduled. |
| `RetryAfter(d, err)` | Uses `d` exactly as the handler's explicit override. |
| `WithHandlerBackoff(policy)` | Uses the handler-specific policy. |
| `WithBackoff(policy)` | Uses the worker-default policy. |
| Library default | Uses `DefaultBackoffPolicy()`. |

Every computed policy delay is still capped by `WithMaxRetryBackoff(d)` for
backward compatibility. `RetryAfter` is an explicit handler override and is
honored as-is.

## Storage Retry Is Separate

Do not use job retry backoff to tune database resilience. For transient storage
failures, configure `WithStorageRetry` and `WithDequeueRetry` as described in
[Storage Operation Retry]({{< relref "storage-retry" >}}). Storage retry happens
inside a single job execution; job retry backoff schedules another execution of
the handler after the handler itself fails.
