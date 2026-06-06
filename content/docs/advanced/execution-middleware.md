---
title: "Execution Middleware"
weight: 5
---

Execution middleware wraps each job handler attempt in the worker. It is useful
for cross-cutting behavior that belongs around execution itself: tracing,
metrics, panic-to-error mapping, context injection, result/error rewriting, and
domain-specific error classification.

Middleware runs once per handler attempt. It wraps the whole handler execution,
including checkpoint loading and replay, so it does not sit between individual
`Call()` checkpoints.

```go
package main

import (
	"context"
	"errors"
	"log/slog"

	jobs "github.com/jdziat/simple-durable-jobs"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var ErrIgnored = errors.New("already applied")

func main() {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}
	q := jobs.New(store)

	q.UseExecutionMiddleware(func(
		ctx context.Context,
		job *jobs.Job,
		next func(context.Context, *jobs.Job) ([]byte, error),
	) ([]byte, error) {
		slog.InfoContext(ctx, "job attempt started", "job_id", job.ID, "type", job.Type)

		result, err := next(ctx, job)
		if err != nil {
			slog.WarnContext(ctx, "job attempt failed", "job_id", job.ID, "error", err)
		}
		// Return errors unchanged unless you intentionally map them. Waiting
		// errors are control flow for FanOut and signals, and must reach the
		// worker unchanged so it can suspend the job.
		return result, err
	})

	q.SetErrorHandler(func(ctx context.Context, job *jobs.Job, err error) {
		slog.ErrorContext(ctx, "final execution error", "job_id", job.ID, "error", err)
	})

	q.SetIsFailure(func(job *jobs.Job, err error) bool {
		// Treat this idempotency outcome as a successful attempt.
		return !errors.Is(err, ErrIgnored)
	})
}
```

## Ordering

Middleware is ordered first-registered outermost. Given:

```go
q.UseExecutionMiddleware(first)
q.UseExecutionMiddleware(second)
```

the call order is:

```text
first before
second before
handler
second after
first after
```

## Waiting Errors

`FanOut` and `WaitForSignal` suspend a job by returning a waiting error. Do not
wrap, replace, or swallow waiting errors in middleware. Return them unchanged so
the worker can leave the job in `StatusWaiting` instead of retrying or failing
it.

## Error Policy

`SetErrorHandler` observes the final non-waiting error after middleware and
`SetIsFailure` classification. It does not change retry or terminal failure
behavior.

`SetIsFailure` can classify a non-nil, non-waiting error as success by returning
`false`. In that case the worker takes the normal completion path, including
completion hooks, `JobCompleted`, and result persistence. Nil errors always
complete, and waiting errors always suspend without consulting the policy.
