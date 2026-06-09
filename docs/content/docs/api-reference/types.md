---
title: "Types"
weight: 6
---

### `RetryConfig`

```go
type RetryConfig struct {
    MaxAttempts       int           // Default: 5
    InitialBackoff    time.Duration // Default: 100ms
    MaxBackoff        time.Duration // Default: 5s
    BackoffMultiplier float64       // Default: 2.0
    JitterFraction    float64       // Default: 0.1 (10%)
}
```

### `DefaultRetryConfig() RetryConfig`

Returns the default retry configuration.

### `Job`

```go
type Job struct {
    ID              string
    Type            string
    Args            []byte      // JSON-encoded arguments
    Queue           string
    Tenant          string      // Optional tenant owner
    Metadata        map[string]string // Queryable string tags
    Priority        int
    Status          JobStatus
    Attempt         int
    MaxRetries      int
    LastError       string
    RunAt           *time.Time
    StartedAt       *time.Time
    CompletedAt     *time.Time
    CreatedAt       time.Time
    UpdatedAt       time.Time
    LastHeartbeatAt *time.Time  // Updated by worker heartbeats
}
```

`Tenant` is an optional owner label for multi-tenant applications. `Metadata`
stores queryable string tags such as region, environment, or customer segment.
Set them with `WithTenant`, `WithMetadata`, or `WithMeta` when enqueueing.

### `JobFilter`

Used by UI-capable storage implementations to list and search jobs.

```go
type JobFilter struct {
    Status       string
    Queue        string
    Type         string
    Tenant       string
    MetaContains *MetadataMap
    Search       string
    Since        time.Time
    Until        time.Time
    Limit        int
    Offset       int
}
```

`Tenant` matches jobs whose tenant is exactly the provided string.
`MetaContains` requires every provided metadata key/value fragment to match, but
`GormStorage` implements this portably as a `LIKE` predicate over serialized
JSON metadata. Treat it as substring matching over JSON text, not exact
structured key/value matching; unusual values that contain the same serialized
fragment can over-match.

### `JobStatus`

```go
const (
    StatusPending   JobStatus = "pending"
    StatusRunning   JobStatus = "running"
    StatusCompleted JobStatus = "completed"
    StatusFailed    JobStatus = "failed"
    StatusRetrying  JobStatus = "retrying"
    StatusWaiting   JobStatus = "waiting"    // Parent waiting for fan-out sub-jobs
    StatusCancelled JobStatus = "cancelled"  // Terminated before completion
    StatusPaused    JobStatus = "paused"     // Paused by user
)
```

### `PauseMode`

```go
const (
    PauseModeGraceful   // Let running jobs finish
    PauseModeAggressive // Cancel running jobs immediately
)
```

### `SubJobFailure`

Populated on `FanOutError.Failures` when a fan-out returns with at least one failed sub-job.

```go
type SubJobFailure struct {
    Index   int    // Position in the original subJobs slice
    JobID   string // Sub-job ID; useful for cross-referencing logs and events
    Error   string // Last error message reported by the sub-job
    Attempt int    // Attempt number that produced Error
}
```

### Error Variables

Sentinel errors exported from the `jobs` package. Compare with `errors.Is`:

```go
var (
    ErrInvalidJobTypeName error // Job type name failed validation
    ErrJobTypeNameTooLong error
    ErrInvalidQueueName   error
    ErrQueueNameTooLong   error
    ErrJobArgsTooLarge    error // Marshalled args exceed MaxJobArgsSize
    ErrJobNotOwned        error // Complete/Fail called by a worker that does not hold the lock
    ErrDuplicateJob       error // Unique()-keyed job already pending/running
    ErrUniqueKeyTooLong   error
    ErrJobAlreadyPaused   error
    ErrJobNotPaused       error
    ErrQueueAlreadyPaused error
    ErrQueueNotPaused     error
    ErrCannotPauseStatus  error // Attempted to pause a job in an incompatible status

    // Returned by LoadResult when retrieving a job's persisted return value:
    ErrJobNotCompleted    error // Job is in a genuinely non-terminal state (pending, running, retrying, waiting, paused) — keep polling
    ErrJobFailed          error // Job failed; the wrapping message embeds job.LastError — stop polling
    ErrJobCancelled       error // Job was cancelled — stop polling
    ErrNoResult           error // Job completed but has no persisted result value
)
```

When polling for a job's result with `LoadResult`, only `ErrJobNotCompleted`
signals a non-terminal state — it is returned for `pending`, `running`,
`retrying`, `waiting`, and `paused`, but **not** for `cancelled` (which has its
own `ErrJobCancelled`). A poller keeps polling on `ErrJobNotCompleted` and stops
on `ErrJobFailed` or `ErrJobCancelled` (both terminal). All four are
`errors.Is`-matchable.
