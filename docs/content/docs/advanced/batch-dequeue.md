---
title: "Batch Dequeue"
weight: 7
---

`WithDequeueBatchSize` lets a worker claim more than one due job per poll when the storage backend supports batch dequeue. This removes the one-job-per-poll throughput ceiling for high-concurrency workers without changing the public `core.Storage` interface.

```go
import (
	jobs "github.com/jdziat/simple-durable-jobs"
)

q := jobs.New(store)
w := q.NewWorker(
	jobs.WorkerQueue("default", jobs.Concurrency(50)),
	jobs.WithDequeueBatchSize(50),
)
```

The default batch size is `10`, so a single worker is no longer capped at the one-claim-per-poll throughput floor (~20 jobs/sec at a 50ms poll) regardless of concurrency. Set `WithDequeueBatchSize(1)` to restore strict single-row dequeue.

## How it works

- On each poll, the worker computes free capacity as the lower of total worker capacity, per-queue remaining capacity, and `WithDequeueBatchSize(n)`.
- If the backend implements the optional `DequeueBatch(ctx, queues, workerID, limit)` capability and the computed limit is greater than one, the worker asks for that many jobs in one storage round trip.
- Each returned job is tracked against its queue concurrency before it is placed on the worker's internal buffered dispatch channel.
- If the worker shuts down while claimed jobs are still buffered or not yet delivered to a handler, those jobs are released back to `pending` immediately instead of waiting for stale-lock recovery.

### Per-queue caps and fairness

When one worker serves several queues with different concurrency caps, a batch claim follows the global `priority DESC, created_at ASC` order with no per-queue split. A hot or high-priority queue can therefore be over-claimed within a single poll: the worker may pull more rows for that queue than its concurrency cap allows. Those surplus rows never run over the cap — each one is dispatched only after an atomic per-queue compare-and-swap, and any row that would exceed the cap is immediately released back to `pending`. The release is **attempt-neutral**: it decrements the claim's `attempt + 1` increment via the same `Release` path, so over-claim churn cannot inflate a job's retry budget or violate exactly-once.

This surplus claim/release is bounded (at most `limit - cap` extra rows per poll, each one extra UPDATE pair) and is the same behavior any existing `WithDequeueBatchSize(10)` user already saw; raising the default just makes the common single-queue worker fast. The work-conserving per-queue drain split that avoids the surplus entirely is planned for v1.9.0.

## Storage capability

Batch dequeue is additive. `core.Storage` does not include `DequeueBatch`, so custom storage implementations continue to compile unchanged. Backends that do not implement the optional capability silently fall back to single-row `Dequeue`; APIs that expose the capability directly can use `core.ErrStorageNoBatchDequeue` when they need a sentinel error.

`GormStorage` implements:

```go
DequeueBatch(ctx context.Context, queues []string, workerID string, limit int) ([]*core.Job, error)
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
