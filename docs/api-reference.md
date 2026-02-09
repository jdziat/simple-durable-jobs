---
layout: default
title: API Reference
---

# API Reference

Complete API documentation for Simple Durable Jobs.

## Package `jobs`

```go
import jobs "github.com/jdziat/simple-durable-jobs"
```

---

## Queue

### `New(storage Storage) *Queue`

Creates a new job queue with the given storage backend.

```go
queue := jobs.New(storage)
```

### `(*Queue) Register(name string, fn any, opts ...Option)`

Registers a job handler function. The function must have one of these signatures:

```go
func(ctx context.Context, args T) error
func(ctx context.Context, args T) (R, error)
func(args T) error
func(args T) (R, error)
```

Example:
```go
queue.Register("send-email", func(ctx context.Context, args EmailArgs) error {
    return sendEmail(args.To, args.Subject)
})
```

### `(*Queue) Enqueue(ctx context.Context, name string, args any, opts ...Option) (string, error)`

Adds a job to the queue. Returns the job ID.

```go
jobID, err := queue.Enqueue(ctx, "send-email", EmailArgs{
    To: "user@example.com",
})
```

### `(*Queue) Schedule(name string, schedule Schedule, opts ...Option)`

Registers a recurring job with the given schedule.

```go
queue.Schedule("cleanup", jobs.Every(5 * time.Minute))
```

### `(*Queue) NewWorker(opts ...WorkerOption) *Worker`

Creates a new worker for processing jobs.

```go
worker := queue.NewWorker(
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
)
```

### `(*Queue) Storage() Storage`

Returns the underlying storage implementation.

---

## Worker

### `(*Worker) Start(ctx context.Context)`

Starts the worker. Blocks until the context is cancelled.

```go
ctx, cancel := context.WithCancel(context.Background())
go worker.Start(ctx)
// Later...
cancel() // Graceful shutdown
```

---

## Durable Calls

### `Call[T any](ctx context.Context, name string, args any, opts ...Option) (T, error)`

Executes a nested job call with checkpointing. Must be called from within a job handler.

```go
queue.Register("workflow", func(ctx context.Context, input Input) error {
    // This call is checkpointed
    result, err := jobs.Call[string](ctx, "step1", input)
    if err != nil {
        return err
    }

    // If this fails, step1 won't re-execute on retry
    _, err = jobs.Call[any](ctx, "step2", result)
    return err
})
```

---

## Job Options

### `Priority(p int) Option`

Sets job priority. Higher values run first. Default is 0.

```go
queue.Enqueue(ctx, "task", args, jobs.Priority(100))
```

### `Retries(n int) Option`

Sets maximum retry attempts. Default is 3.

```go
queue.Enqueue(ctx, "task", args, jobs.Retries(5))
```

### `Delay(d time.Duration) Option`

Delays job execution by the specified duration.

```go
queue.Enqueue(ctx, "task", args, jobs.Delay(10 * time.Minute))
```

### `At(t time.Time) Option`

Schedules the job to run at a specific time.

```go
queue.Enqueue(ctx, "task", args, jobs.At(time.Now().Add(time.Hour)))
```

### `QueueOpt(name string) Option`

Assigns the job to a specific queue.

```go
queue.Enqueue(ctx, "task", args, jobs.QueueOpt("critical"))
```

---

## Worker Options

### `WorkerQueue(name string, opts ...WorkerOption) WorkerOption`

Configures the worker to process a specific queue.

```go
worker := queue.NewWorker(
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
    jobs.WorkerQueue("critical", jobs.Concurrency(5)),
)
```

### `Concurrency(n int) WorkerOption`

Sets the number of concurrent job processors. Default is 10.

### `WithScheduler(enabled bool) WorkerOption`

Enables the scheduler for recurring jobs.

```go
worker := queue.NewWorker(jobs.WithScheduler(true))
```

---

## Schedules

### `Every(d time.Duration) Schedule`

Creates a schedule that runs at fixed intervals.

```go
jobs.Every(5 * time.Minute)
```

### `Daily(hour, minute int) Schedule`

Creates a schedule that runs daily at the specified time (UTC).

```go
jobs.Daily(9, 0)  // 9:00 AM UTC
```

### `Weekly(day time.Weekday, hour, minute int) Schedule`

Creates a schedule that runs weekly on the specified day and time (UTC).

```go
jobs.Weekly(time.Sunday, 2, 0)  // Sunday at 2:00 AM UTC
```

### `Cron(expr string) Schedule`

Creates a schedule from a cron expression.

```go
jobs.Cron("0 * * * *")     // Every hour
jobs.Cron("0 9 * * 1-5")   // Weekdays at 9:00 AM
jobs.Cron("0 0 1 * *")     // First day of month
```

---

## Hooks

### `(*Queue) OnJobStart(fn func(context.Context, *Job))`

Registers a callback for when a job starts processing.

### `(*Queue) OnJobComplete(fn func(context.Context, *Job))`

Registers a callback for when a job completes successfully.

### `(*Queue) OnJobFail(fn func(context.Context, *Job, error))`

Registers a callback for when a job fails permanently.

### `(*Queue) OnRetry(fn func(context.Context, *Job, error))`

Registers a callback for when a job is being retried.

---

## Events

### `(*Queue) Events() <-chan Event`

Returns a channel that receives job events.

```go
events := queue.Events()
go func() {
    for event := range events {
        switch e := event.(type) {
        case *jobs.JobStarted:
            log.Printf("Started: %s", e.Job.ID)
        case *jobs.JobCompleted:
            log.Printf("Completed: %s", e.Job.ID)
        case *jobs.JobFailed:
            log.Printf("Failed: %s - %v", e.Job.ID, e.Error)
        case *jobs.JobRetrying:
            log.Printf("Retrying: %s", e.Job.ID)
        }
    }
}()
```

---

## Error Types

### `NoRetry(err error) error`

Wraps an error to indicate the job should not be retried.

```go
if invalidInput {
    return jobs.NoRetry(errors.New("invalid input"))
}
```

### `RetryAfter(d time.Duration, err error) error`

Wraps an error to indicate the job should be retried after a specific duration.

```go
if rateLimited {
    return jobs.RetryAfter(5 * time.Minute, errors.New("rate limited"))
}
```

---

## Types

### `Job`

```go
type Job struct {
    ID          string
    Type        string
    Args        []byte      // JSON-encoded arguments
    Queue       string
    Priority    int
    Status      JobStatus
    Attempt     int
    MaxRetries  int
    LastError   string
    RunAt       *time.Time
    StartedAt   *time.Time
    CompletedAt *time.Time
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### `JobStatus`

```go
type JobStatus string

const (
    StatusPending   JobStatus = "pending"
    StatusRunning   JobStatus = "running"
    StatusCompleted JobStatus = "completed"
    StatusFailed    JobStatus = "failed"
    StatusRetrying  JobStatus = "retrying"
)
```

### `Checkpoint`

```go
type Checkpoint struct {
    ID        string
    JobID     string
    CallIndex int
    CallType  string
    Result    []byte  // JSON-encoded result
    Error     string
    CreatedAt time.Time
}
```

---

## Storage Interface

```go
type Storage interface {
    Migrate(ctx context.Context) error
    Enqueue(ctx context.Context, job *Job) error
    Dequeue(ctx context.Context, queues []string) (*Job, error)
    Complete(ctx context.Context, jobID string) error
    Fail(ctx context.Context, jobID string, errMsg string, runAt *time.Time) error
    SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
    GetCheckpoints(ctx context.Context, jobID string) ([]Checkpoint, error)
    DeleteCheckpoints(ctx context.Context, jobID string) error
    GetDueJobs(ctx context.Context, limit int) ([]Job, error)
    Heartbeat(ctx context.Context, jobID string, workerID string) error
    ReleaseStaleLocks(ctx context.Context, timeout time.Duration) (int64, error)
    GetJob(ctx context.Context, jobID string) (*Job, error)
    GetJobsByStatus(ctx context.Context, status JobStatus, limit int) ([]Job, error)
}
```

### `NewGormStorage(db *gorm.DB) *GormStorage`

Creates a new GORM-based storage implementation.

```go
db, _ := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
storage := jobs.NewGormStorage(db)
```
