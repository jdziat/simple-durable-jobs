---
title: "Queue"
weight: 1
---

## Package `jobs`

```go
import jobs "github.com/jdziat/simple-durable-jobs/v4"
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

### `(*Queue) EnqueueRemote(ctx context.Context, name string, args any, opts ...Option) (string, error)`

Adds a job without requiring a local handler registration. Use this for
producer-only processes that enqueue work for workers running elsewhere.
Malformed job names are rejected.

```go
jobID, err := queue.EnqueueRemote(ctx, "send-email", EmailArgs{
    To: "user@example.com",
})
```

### `(*Queue) Schedule(name string, args any, sched Schedule, opts ...Option) error`

Registers a recurring job with the given schedule. The job name must already be
registered, and duplicate scheduled names return an error.

```go
queue.Register("cleanup", func(ctx context.Context, _ struct{}) error {
    return cleanup(ctx)
})

if err := queue.Schedule("cleanup", nil, jobs.Every(5*time.Minute)); err != nil {
    return err
}
```

### `(*Queue) NewWorker(opts ...any) core.Starter`

Creates a worker for this queue. Note the return type: it is `core.Starter`,
whose only method is `Start`. **Use `jobs.NewWorker(q, ...)` instead** unless
`Start` is genuinely all you need — the facade returns `*Worker`, which is the
type that carries `Pause`, `Resume`, `WaitForPause`, `CancelJob`, `Health`,
`IsPaused`, `RunningJobCount` and `HealthHandler`. The `...any` parameter also
means a mistyped option is not caught by the compiler; the facade takes typed
`WorkerOption` values.

```go
// Preferred: returns *Worker, options are type-checked.
worker := jobs.NewWorker(queue,
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
)

// Start-only; cannot be paused, resumed or health-checked.
starter := queue.NewWorker(
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
)
```

### `(*Queue) Storage() Storage`

Returns the underlying storage implementation.

---

## Context Helpers

### `JobFromContext(ctx context.Context) *Job`

Returns the current Job from context, or nil if not in a job handler. Use this to get the job ID for logging or progress tracking.

```go
queue.Register("my-job", func(ctx context.Context, args MyArgs) error {
    job := jobs.JobFromContext(ctx)
    log.Printf("Processing job %s", job.ID)
    return nil
})
```

### `JobIDFromContext(ctx context.Context) string`

Returns the current job ID from context, or empty string if not in a job handler.
