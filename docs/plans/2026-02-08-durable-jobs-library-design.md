# Durable Jobs Queue Library Design

A Go library for durable job queues combining the best of River Queue, Temporal, and async patterns.

## Goals

- Background job processing (fire-and-forget tasks)
- Multi-step workflows with nested durable calls
- Scheduled/recurring jobs (cron and intervals)
- Full durability and resumability across crashes
- Simple, ergonomic API

## Core API

### Job Registration & Enqueueing

```go
package jobs

// Create a queue with a storage backend
queue := jobs.New(jobs.NewGormStorage(db))

// Register a simple job
queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
    return sendEmail(p.To, p.Subject, p.Body)
})

// Register a workflow with nested durable calls
queue.Register("process-order", func(ctx context.Context, order Order) error {
    receipt, err := jobs.Call(ctx, "charge-card", order.Payment)
    if err != nil {
        return err
    }

    _, err = jobs.Call(ctx, "reserve-inventory", order.Items)
    if err != nil {
        return jobs.Call(ctx, "refund", receipt)
    }

    return jobs.Call(ctx, "send-confirmation", order.Email)
})

// Enqueue a job
queue.Enqueue(ctx, "send-email", EmailParams{To: "user@example.com", ...})
```

### Nested Durable Calls

`jobs.Call(ctx, ...)` is checkpointed. On crash recovery, the job function replays from the start but completed calls return instantly from the checkpoint store.

```go
// First run: executes charge-card, saves checkpoint, crashes before confirm
// Resume: returns saved charge-card result, executes confirm
```

## Storage Backend

GORM-based implementation supporting PostgreSQL, MySQL, SQLite, SQL Server, ClickHouse.

```go
type Storage interface {
    // Job lifecycle
    Enqueue(ctx context.Context, job *Job) error
    Dequeue(ctx context.Context, queues []string) (*Job, error)
    Complete(ctx context.Context, jobID string, result []byte) error
    Fail(ctx context.Context, jobID string, err error) error

    // Checkpointing for nested calls
    SaveCheckpoint(ctx context.Context, jobID string, callID string, result []byte) error
    GetCheckpoints(ctx context.Context, jobID string) ([]Checkpoint, error)

    // Scheduling
    ScheduleAt(ctx context.Context, job *Job, runAt time.Time) error
    GetDueJobs(ctx context.Context, now time.Time) ([]*Job, error)

    // Locking (for distributed workers)
    Lock(ctx context.Context, jobID string, workerID string, ttl time.Duration) (bool, error)
    Heartbeat(ctx context.Context, jobID string, workerID string) error
    Unlock(ctx context.Context, jobID string) error
}

// Default implementation
func NewGormStorage(db *gorm.DB) Storage
```

Interface remains open for custom implementations (Redis, DynamoDB, etc.).

## Retries & Error Handling

Two-layer retry system with sensible defaults.

```go
// Defaults
jobs.DefaultCallRetries = 3
jobs.DefaultCallBackoff = jobs.ExponentialBackoff(time.Second, time.Minute)
jobs.DefaultJobRetries = 2

// Override per nested call
receipt, err := jobs.Call(ctx, "charge-card", payment,
    jobs.Retries(5),
    jobs.Backoff(jobs.ExponentialBackoff(100*time.Millisecond, 30*time.Second)),
)

// Override at job registration
queue.Register("critical-workflow", processFn,
    jobs.Retries(5),
    jobs.OnExhausted(jobs.MoveToDLQ),
)

// Error control
return jobs.NoRetry(errors.New("invalid input"))      // Fail immediately
return jobs.RetryAfter(30*time.Second, err)           // Retry after delay
```

Backoff strategies:
- `jobs.ConstantBackoff(d)`
- `jobs.ExponentialBackoff(base, max)`
- `jobs.ExponentialBackoffWithJitter(base, max)`

## Scheduling & Recurring Jobs

```go
// Interval-based
queue.Schedule("cleanup", cleanupFn, jobs.Every(time.Hour))
queue.Schedule("daily-digest", digestFn, jobs.Daily(9, 0))
queue.Schedule("weekly-report", reportFn, jobs.Weekly(time.Monday, 10, 0))

// Cron expressions
queue.Schedule("weekday-morning", fn, jobs.Cron("0 9 * * MON-FRI"))

// With timezone
queue.Schedule("daily", fn, jobs.Daily(9, 0), jobs.Timezone("America/New_York"))

// One-time delayed execution
queue.Enqueue(ctx, "reminder", data, jobs.Delay(24*time.Hour))
queue.Enqueue(ctx, "reminder", data, jobs.At(specificTime))

// Uniqueness
queue.Schedule("singleton", fn, jobs.Every(time.Hour), jobs.Unique("singleton-key"))
```

## Workers & Scaling

Named queues, priorities, configurable concurrency.

```go
// Basic worker
worker := queue.NewWorker(storage)
worker.Start(ctx)

// Named queues with concurrency
worker := queue.NewWorker(storage,
    jobs.Queue("critical", jobs.Concurrency(20)),
    jobs.Queue("emails", jobs.Concurrency(10)),
    jobs.Queue("reports", jobs.Concurrency(2)),
)

// Priorities
queue.Enqueue(ctx, "urgent", data, jobs.Queue("emails"), jobs.Priority(100))
queue.Enqueue(ctx, "newsletter", data, jobs.Queue("emails"), jobs.Priority(1))

// Graceful shutdown
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
defer cancel()
worker.Start(ctx)  // Blocks until ctx cancelled, drains gracefully
```

## Observability

Three layers: hooks, middleware, event stream.

```go
// Hooks
queue.OnJobStart(func(ctx context.Context, j *jobs.Job) { ... })
queue.OnJobComplete(func(ctx context.Context, j *jobs.Job, duration time.Duration) { ... })
queue.OnJobFail(func(ctx context.Context, j *jobs.Job, err error) { ... })
queue.OnRetry(func(ctx context.Context, j *jobs.Job, attempt int, err error) { ... })

// Middleware
queue.Use(jobs.PrometheusMetrics(prometheus.DefaultRegisterer))
queue.Use(jobs.OTelTracing(otel.GetTracerProvider()))
queue.Use(jobs.SlogLogger(slog.Default()))

// Event stream
events := queue.Events()
for event := range events {
    switch e := event.(type) {
    case *jobs.JobStarted:
    case *jobs.JobCompleted:
    case *jobs.JobFailed:
    case *jobs.CheckpointSaved:
    }
}
```

Prometheus metrics: `jobs_enqueued_total`, `jobs_completed_total`, `jobs_failed_total`, `jobs_duration_seconds`, `jobs_in_progress`, `jobs_queue_depth`.

## Determinism & Replay

Default: explicit checkpoints. Only `jobs.Call()` results are durable.

```go
// Default behavior
queue.Register("order", func(ctx context.Context, o Order) error {
    receipt, _ := jobs.Call(ctx, "charge", o.Payment)  // Durable
    log.Info("charged", receipt)                        // Runs on replay too
    return jobs.Call(ctx, "ship", o.Items)             // Durable
})

// Strict mode
queue.Register("time-sensitive", fn, jobs.Determinism(jobs.Strict))
// In strict mode, use helpers:
now := jobs.Now(ctx)
id := jobs.UUID(ctx)
val := jobs.Random(ctx, 100)

// Best effort mode
queue.Register("flexible", fn, jobs.Determinism(jobs.BestEffort))

// Global default
queue.SetDeterminism(jobs.ExplicitCheckpoints)
```

Call sequence tracked by `jobType:callIndex`.

## Complete API Reference

```go
package jobs

// Core
func New(storage Storage) *Queue
func (q *Queue) Register(name string, fn any, opts ...Option)
func (q *Queue) Enqueue(ctx context.Context, name string, args any, opts ...Option) (string, error)
func (q *Queue) Schedule(name string, fn any, schedule Schedule, opts ...Option)

// Durable calls
func Call[T any](ctx context.Context, name string, args any, opts ...Option) (T, error)

// Worker
func (q *Queue) NewWorker(opts ...WorkerOption) *Worker
func (w *Worker) Start(ctx context.Context) error

// Options
func Retries(n int) Option
func Backoff(b BackoffStrategy) Option
func Queue(name string) Option
func Priority(p int) Option
func Delay(d time.Duration) Option
func At(t time.Time) Option
func Unique(key string) Option
func Determinism(d DeterminismMode) Option

// Scheduling
func Every(d time.Duration) Schedule
func Daily(hour, minute int) Schedule
func Weekly(day time.Weekday, hour, minute int) Schedule
func Cron(expr string) Schedule

// Error handling
func NoRetry(err error) error
func RetryAfter(d time.Duration, err error) error

// Storage
type Storage interface { ... }
func NewGormStorage(db *gorm.DB) Storage

// Observability
func (q *Queue) OnJobStart(fn func(context.Context, *Job))
func (q *Queue) OnJobComplete(fn func(context.Context, *Job, time.Duration))
func (q *Queue) OnJobFail(fn func(context.Context, *Job, error))
func (q *Queue) Events() <-chan Event
func PrometheusMetrics(reg prometheus.Registerer) Middleware
func OTelTracing(tp trace.TracerProvider) Middleware
```

## Implementation Phases

1. **Core**: Job struct, Queue, Register, Enqueue, basic Worker
2. **Storage**: GORM storage, migrations, job lifecycle
3. **Checkpointing**: jobs.Call(), checkpoint storage, replay logic
4. **Retries**: Retry logic, backoff strategies, error types
5. **Scheduling**: Cron parser, scheduler goroutine, due job polling
6. **Workers**: Named queues, priorities, concurrency control, locking
7. **Observability**: Hooks, middleware, event stream, Prometheus/OTel
8. **Polish**: Documentation, examples, tests
