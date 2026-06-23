# Transactional Enqueue

This example demonstrates an outbox-style producer that writes application data and enqueues a job in the same transaction.

## What It Does

- Creates an `orders` table next to the jobs tables in SQLite
- Registers a `fulfill-order` handler that idempotently marks an order fulfilled
- Commits one order and job together with `queue.EnqueueTx()`
- Rolls back another transaction that had staged both an order and a job
- Starts a worker and verifies only the committed job runs

## Prerequisites

- Go 1.25+
- No external services needed (uses SQLite)

## Running

```bash
go run ./examples/transactional-enqueue
```

The example runs once, processes the committed job, and then shuts down.

## Expected Output

```
Committed order and job 4272fe22
Rolled back transaction that had staged job c0367f41
After commit and rollback: orders=1 jobs=1
Rolled-back job visible after rollback: false
Starting worker...
[worker] Fulfilling order ORD-COMMIT
Committed order final status: fulfilled
After worker: orders=1 jobs=1
Transactional enqueue example complete: only the committed job ran
```

The committed business row and job become visible together. The rolled-back order and staged job both disappear.

## Key Concepts

- **`queue.EnqueueTx()`** writes the job row through the caller's GORM transaction.
- **Atomic producer writes** keep application data and queued work from diverging.
- **Rollback safety** means staged jobs are not visible to workers when the surrounding transaction rolls back.
- **Idempotent handlers** are still required because jobs are at-least-once after they are committed.

## Tips

- Use transactional enqueue when a request handler must create domain data and schedule follow-up work as one unit.
- Keep the job arguments small and point to committed business rows by ID.
- Combine `queue.EnqueueTx()` with `jobs.Unique()` when each domain object should have at most one active job.
- PostgreSQL or MySQL are better choices than SQLite for production systems with concurrent producers and workers.

## Related Documentation

- [Advanced - Transactional Enqueue](https://jdziat.github.io/simple-durable-jobs/docs/advanced/transactional-enqueue/)
- [API Reference - Job Options](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/job-options/)
- [Advanced - Guarantees](https://jdziat.github.io/simple-durable-jobs/docs/advanced/guarantees/)
- [pkg.go.dev - simple-durable-jobs](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs/v4)
