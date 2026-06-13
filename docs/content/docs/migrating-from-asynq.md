---
title: "Migrating from Asynq"
weight: 10
toc: true
---

Asynq is a Redis-backed Go task queue. Simple Durable Jobs is a SQL-backed Go job library with durable checkpoints, signals, timers, transactional enqueue, and an embedded dashboard. Migration usually means moving queued task state from Redis into your application database.

## Concept Mapping

| Asynq concept | Simple Durable Jobs concept |
| --- | --- |
| Task type | Job name string |
| Payload | Typed Go arguments marshaled to JSON |
| `asynq.Client` | `jobs.New(storage)` queue |
| `client.Enqueue` | `queue.Enqueue(ctx, name, args, opts...)` |
| `asynq.Server` | `queue.NewWorker(...).Start(ctx)` |
| `ServeMux.HandleFunc` | `queue.Register("name", handler)` |
| Queue option | `jobs.QueueOpt("queue")` |
| `Unique(...)` | `jobs.IdempotencyKey(key, ttl)` or `jobs.UniqueFor(ttl)` |
| `ProcessIn(...)` | `jobs.Delay(duration)` |
| `ProcessAt(...)` | `jobs.At(time)` |
| Max retry option | `jobs.Retries(n)` |
| Cron scheduler | `queue.Schedule(name, args, jobs.MustCron(expr))` |

## Side-by-Side

Asynq-style task definition, enqueue, and worker:

```go
const TypeEmailDelivery = "email.deliver"

type EmailPayload struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func NewEmailDeliveryTask(userID, email string) (*asynq.Task, error) {
	payload, err := json.Marshal(EmailPayload{UserID: userID, Email: email})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeEmailDelivery, payload), nil
}

func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}
	return sendEmail(p.UserID, p.Email)
}

client := asynq.NewClient(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"})
defer client.Close()

task, err := NewEmailDeliveryTask("user-123", "user@example.com")
if err != nil {
	return err
}
if _, err := client.Enqueue(task, asynq.Queue("mailers"), asynq.ProcessIn(time.Minute)); err != nil {
	return err
}

srv := asynq.NewServer(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}, asynq.Config{
	Concurrency: 10,
	Queues: map[string]int{
		"mailers": 1,
	},
})

mux := asynq.NewServeMux()
mux.HandleFunc(TypeEmailDelivery, HandleEmailDeliveryTask)
return srv.Run(mux)
```

The same shape with Simple Durable Jobs:

```go
type EmailPayload struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
if err != nil {
	return err
}

storage := jobs.NewGormStorage(db)
if err := storage.Migrate(ctx); err != nil {
	return err
}

queue := jobs.New(storage)
queue.Register("email.deliver", func(ctx context.Context, p EmailPayload) error {
	return sendEmail(p.UserID, p.Email)
})

_, err = queue.Enqueue(ctx, "email.deliver", EmailPayload{
	UserID: "user-123",
	Email:  "user@example.com",
}, jobs.QueueOpt("mailers"), jobs.Delay(time.Minute))
if err != nil {
	return err
}

worker := queue.NewWorker(
	jobs.WorkerQueue("mailers", jobs.Concurrency(10)),
)
return worker.Start(ctx)
```

Cron-like recurring enqueue moves to the built-in scheduler:

```go
queue.Schedule("email.digest", EmailPayload{UserID: "user-123"},
	jobs.MustCron("0 9 * * *"),
	jobs.QueueOpt("mailers"),
)

worker := queue.NewWorker(
	jobs.WorkerQueue("mailers", jobs.Concurrency(10)),
	jobs.WithScheduler(true),
)
```

## Gotchas

Both systems are at-least-once. Handlers must remain idempotent, especially around external side effects such as email, payment, or webhook delivery.

Redis-specific behavior does not carry over. If you use Redis key inspection, Redis memory sizing, Asynq task archival behavior, or Asynq-specific queue tooling, replace those with SQL queries, the embedded dashboard, metrics, and dead-letter triage.

The backing database changes. Simple Durable Jobs stores jobs in PostgreSQL, MySQL, or SQLite through GORM; it does not use Redis as a broker.

Retry counts map to `jobs.Retries(n)`. Delays map to `jobs.Delay(d)` or `jobs.At(t)`. Handler-controlled retry timing maps to returning `jobs.RetryAfter(d, err)`.

Asynq's `Unique(ttl)` is a time-window deduplication option. Map a
caller-supplied key to `jobs.IdempotencyKey(key, ttl)`, or use
`jobs.UniqueFor(ttl)` when the job's queue, name, and arguments identify the
work. `jobs.Unique(key)` is narrower: it only prevents another pending or
running job with the same key.

As of 2026-06-13, there is no first-party workflow checkpointing equivalent in Asynq. If your Asynq task currently chains follow-up tasks manually, consider a Simple Durable Jobs workflow handler with `jobs.Call[T]` checkpoints or `jobs.FanOut[T]`.

Asynq's Redis task groups and some queue inspection behaviors do not have direct equivalents. Use queue names, unique keys, typed arguments, dashboard filtering, and application-owned tables for domain-level grouping.

## What You Gain

- SQL-native jobs in the same PostgreSQL or MySQL database as your application.
- Transactional enqueue with `queue.EnqueueTx(...)`.
- Checkpointed workflows through `jobs.Call[T]`.
- Durable signals and timers.
- Per-key concurrency caps and storage-backed rate limits.

## What You Lose

- Redis-backed task speed and operational simplicity if Redis was already your queueing layer.
- Asynq-specific task states, tooling, and Redis inspection workflows.
- Drop-in compatibility with Asynq middleware and scheduler APIs.
- A queue model centered on Redis primitives.
