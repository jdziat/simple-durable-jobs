---
title: "Migrating from River"
weight: 9
toc: true
---

River and Simple Durable Jobs both fit Go applications that want workers close to application code. The main migration question is whether you want to keep a Postgres-only queue optimized around River's model, or move to a smaller SQL/GORM facade that also supports MySQL and SQLite and includes checkpointed `Call[T]` workflows.

## Concept Mapping

| River concept | Simple Durable Jobs concept |
| --- | --- |
| Client | `jobs.New(storage)` queue |
| `Worker[T]` | `queue.Register("name", handler)` |
| Insert / insert params | `queue.Enqueue(ctx, name, args, opts...)` |
| `InsertTx` | `queue.EnqueueTx(ctx, tx, name, args, opts...)` |
| `JobArgs` | Typed Go argument struct marshaled to JSON |
| Queue name | `jobs.QueueOpt("queue")` and `jobs.WorkerQueue("queue", ...)` |
| Periodic jobs | `queue.Schedule(name, args, jobs.Every(...))`, `jobs.Daily`, `jobs.Weekly`, `jobs.Cron` |
| Unique jobs | `jobs.Unique(key)` |
| `UniqueOpts.ByPeriod` | `jobs.UniqueFor(ttl)` |
| Retries | `jobs.Retries(n)` plus worker backoff options |
| Worker process | `queue.NewWorker(...).Start(ctx)` |

## Side-by-Side

River-style enqueue and worker registration:

```go
type SendEmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (SendEmailArgs) Kind() string { return "send-email" }

type SendEmailWorker struct {
	river.WorkerDefaults[SendEmailArgs]
}

func (w *SendEmailWorker) Work(ctx context.Context, job *river.Job[SendEmailArgs]) error {
	return sendEmail(job.Args.To, job.Args.Subject)
}

workers := river.NewWorkers()
river.AddWorker(workers, &SendEmailWorker{})

client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
	Queues: map[string]river.QueueConfig{
		river.QueueDefault: {MaxWorkers: 10},
	},
	Workers: workers,
})
if err != nil {
	return err
}

_, err = client.Insert(ctx, SendEmailArgs{
	To:      "user@example.com",
	Subject: "Welcome",
}, nil)
```

The same shape with Simple Durable Jobs:

```go
type SendEmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
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
queue.Register("send-email", func(ctx context.Context, args SendEmailArgs) error {
	return sendEmail(args.To, args.Subject)
})

_, err = queue.Enqueue(ctx, "send-email", SendEmailArgs{
	To:      "user@example.com",
	Subject: "Welcome",
}, jobs.QueueOpt("default"))
if err != nil {
	return err
}

worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(10)))
return worker.Start(ctx)
```

Transactional enqueue maps directly when your application database is also the jobs database:

```go
tx := db.Begin()
defer tx.Rollback()

if err := tx.WithContext(ctx).Create(&EmailRequest{ID: requestID}).Error; err != nil {
	return err
}

_, err := queue.EnqueueTx(ctx, tx, "send-email", SendEmailArgs{
	To:      "user@example.com",
	Subject: "Welcome",
}, jobs.Unique("email:"+requestID))
if err != nil {
	return err
}

return tx.Commit().Error
```

## Gotchas

Both systems are at-least-once. Keep handlers idempotent, put side effects behind natural idempotency keys, and expect retries after worker crashes.

River-specific PostgreSQL features and APIs may not carry over as of 2026-06-13. If your current design depends on River's exact job states, queue internals, LISTEN/NOTIFY wakeups, or Pro features, map those deliberately against River's current documentation instead of expecting a drop-in replacement.

You do not have to leave PostgreSQL. Simple Durable Jobs supports PostgreSQL through GORM and is happiest in production on PostgreSQL or MySQL. SQLite is useful for local development and single-process tests.

Retry counts map to `jobs.Retries(n)`. Worker retry timing maps to `jobs.WithBackoff(...)`, `jobs.DefaultBackoffPolicy()`, or handler-returned `jobs.RetryAfter(...)`.

River uniqueness by period maps to `jobs.UniqueFor(ttl)` when the job arguments
identify the work. If your River key is caller-supplied, use
`jobs.IdempotencyKey(key, ttl)` instead. `jobs.Unique(key)` is only an
active-job guard and releases when the first job leaves pending/running.

There is no direct equivalent for River's Postgres-specialized low-latency wakeup path. Workers poll; lower `jobs.WithPollInterval(...)` reduces latency at the cost of more database traffic, and `jobs.WithDequeueBatchSize(...)` improves drain throughput.

## What You Gain

- SQLite and MySQL support in addition to PostgreSQL.
- Durable `jobs.Call[T]` checkpoints for multi-step Go workflows.
- Durable signals and timers in the same library.
- An embedded dashboard served from your own Go HTTP server.
- A small root facade that keeps most application code inside one `jobs` import.

## What You Lose

- River's PostgreSQL-only performance focus and Postgres wakeup model.
- River-specific APIs, job metadata, and operational behavior.
- Paid Pro features if you rely on them, such as River Pro workflows or sequences.
- The larger ecosystem and specialization around River's queue model.
