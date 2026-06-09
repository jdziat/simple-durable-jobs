---
title: "Queue"
weight: 1
---

## Package `jobs`

```go
import jobs "github.com/jdziat/simple-durable-jobs/v2"
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

### `(*Queue) Schedule(name string, args any, sched Schedule, opts ...Option)`

Registers a recurring job with the given schedule.

```go
queue.Schedule("cleanup", nil, jobs.Every(5 * time.Minute))
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
