---
title: "Batch Dequeue"
weight: 7
---

`WithDequeueBatchSize` lets a worker claim more than one due job per poll when the storage backend supports batch dequeue. The worker drains available work within each poll interval, so the poll tick is the idle backstop rather than the throughput ceiling. Batch dequeue remains additive and does not change the public `core.Storage` interface.

```go
import (
	jobs "github.com/jdziat/simple-durable-jobs/v2"
)

q := jobs.New(store)
w := q.NewWorker(
	jobs.WorkerQueue("default", jobs.Concurrency(50)),
	jobs.WithDequeueBatchSize(50),
)
```

The default batch size is `10`, so a single worker is no longer capped at the old one-claim-per-poll throughput floor. Set `WithDequeueBatchSize(1)` to force strict single-row claims; pairing that with a slower `WithPollInterval(...)` is the rollback escape hatch for conservative slow-poll behavior.

## How it works

- On each poll tick, the worker keeps dequeuing and dispatching while capacity remains and progress is made.
- The worker computes free capacity as the lower of total worker capacity, per-queue remaining capacity, and `WithDequeueBatchSize(n)`.
- If the backend implements the optional `DequeueBatchPerQueue(ctx, workerID, budgets)` capability and the computed limit is greater than one, the worker passes per-queue budgets derived from remaining capacity.
- If the backend lacks `DequeueBatchPerQueue` but implements `DequeueBatch(ctx, queues, workerID, limit)`, the worker falls back to that older batch capability.
- Each returned job is tracked against its queue concurrency before it is placed on the worker's internal buffered dispatch channel.
- If the worker shuts down while claimed jobs are still buffered or not yet delivered to a handler, those jobs are released back to `pending` immediately instead of waiting for stale-lock recovery.

### Per-queue caps and fairness

When one worker serves several queues with different concurrency caps,
`DequeueBatchPerQueue` prevents a hot queue from consuming the whole batch. The
worker computes `maxConcurrency - running` for each available queue and passes
that budget map to storage. `GormStorage` still uses one global
`priority DESC, created_at ASC` scan, then skip-lists rows whose queue already
hit its budget. This preserves cross-queue global priority while avoiding the
surplus claim/release churn that older batch dequeue could cause.

## Storage capability

Batch dequeue is additive. `core.Storage` does not include `DequeueBatch`, so custom storage implementations continue to compile unchanged. Backends that do not implement the optional capability silently fall back to single-row `Dequeue`; APIs that expose the capability directly can use `core.ErrStorageNoBatchDequeue` when they need a sentinel error.

`GormStorage` implements:

```go
DequeueBatch(ctx context.Context, queues []string, workerID string, limit int) ([]*core.Job, error)
DequeueBatchPerQueue(ctx context.Context, workerID string, budgets map[string]int) ([]*core.Job, error)
```

It preserves the same predicates and ordering as single-row `Dequeue`: active queue filter, `pending` status, due `run_at`, expired or empty lock, and `priority DESC, created_at ASC`.

## Backend locking

PostgreSQL and MySQL use `FOR UPDATE SKIP LOCKED` inside one transaction, then mark the selected rows `running` with `locked_by`, `locked_until`, `started_at`, and `attempt = attempt + 1` using the database clock.

SQLite does not support row-level `SKIP LOCKED`, so it keeps the existing optimistic claim protocol: select the next candidate, atomically update it only while it is still `pending`, and treat `RowsAffected == 0` as a lost race. SQLite uses the worker's local `time.Now`, matching single-row dequeue.

## Tuning

Set the batch size near the worker concurrency when poll round trips are the bottleneck:

```go
q := jobs.New(store)
w := q.NewWorker(
	jobs.WorkerQueue("default", jobs.Concurrency(100)),
	jobs.WithDequeueBatchSize(100),
)
```

The per-poll request is still bounded by actual free capacity, so a worker with only 12 open slots asks for at most 12 jobs even if the configured batch size is higher.

See [Production Operations]({{< relref "/docs/production-ops" >}}) for the
work-conserving drain loop, per-queue claim budgets, and rollback guidance.
