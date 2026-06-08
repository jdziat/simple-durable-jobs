---
title: "Error Handling"
weight: 9
---

### `NoRetry(err error) error`

Wraps an error to indicate the job should not be retried.

### `RetryAfter(d time.Duration, err error) error`

Wraps an error to indicate the job should be retried after a specific duration.

## Discriminating job outcomes with `LoadResult`

The two wrappers above shape how a handler reports failure *to the worker*. The
sentinel errors below run in the other direction: they let a caller polling for a
job's outcome decide what happened. `LoadResult[T](ctx, q, jobID)` decodes a
completed job's persisted return value into `T`, and returns one of a small set
of sentinels depending on the job's terminal (or non-terminal) status. Every one
is matchable with `errors.Is`, so a poller never has to inspect error strings.

| Returned error | Job status | What it means |
| -------------- | ---------- | ------------- |
| `nil` | completed (with result) | The decoded value of type `T` is returned. |
| `ErrNoResult` | completed (no result) | The job finished but persisted no return value. |
| `ErrJobFailed` | failed | The job exhausted retries. The wrapped message embeds `job.LastError`, so `errors.Is(err, ErrJobFailed)` is true and `err.Error()` carries the cause. |
| `ErrJobCancelled` | cancelled | The job was cancelled before completing. |
| `ErrJobNotCompleted` | pending, running, retrying, waiting, or paused | The job is in a genuinely non-terminal state — keep polling. |

`ErrJobFailed` and `ErrJobCancelled` are returned wrapped (via `fmt.Errorf("%w: …")`),
so always compare with `errors.Is`, never `==`. `ErrJobNotCompleted` is the only
sentinel that signals a non-terminal state; on `ErrJobFailed` and `ErrJobCancelled`
a poller should stop, and on `ErrJobNotCompleted` it should poll again.

A three-way polling loop typically looks like this:

```go
res, err := jobs.LoadResult[Receipt](ctx, q, jobID)
switch {
case err == nil:
	// res holds the decoded result; we're done.
	return res, nil
case errors.Is(err, jobs.ErrJobNotCompleted):
	// Still pending/running/retrying/waiting/paused — wait and retry.
	time.Sleep(pollInterval)
	continue
case errors.Is(err, jobs.ErrJobFailed):
	// Terminal failure; err.Error() embeds the job's LastError.
	return zero, fmt.Errorf("job failed: %w", err)
case errors.Is(err, jobs.ErrJobCancelled):
	// Terminal cancellation.
	return zero, err
case errors.Is(err, jobs.ErrNoResult):
	// Completed with no return value.
	return zero, nil
default:
	// Storage/decode error (e.g. ErrJobNotFound, JSON decode failure).
	return zero, err
}
```
