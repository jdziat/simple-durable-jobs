---
title: "Types"
weight: 5
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
)
```
