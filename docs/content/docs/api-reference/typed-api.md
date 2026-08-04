---
title: "Typed API"
weight: 4
---

## Package `typed`

```go
import typed "github.com/jdziat/simple-durable-jobs/v4/pkg/typed"
```

The typed API adds compile-time checked handles over the existing string-keyed
queue. Routing still uses the registered job name, so it works with the same
workers, storage, checkpoints, middleware, and payload codec as `Queue.Register`
and `Queue.Enqueue`.

Job IDs are `core.UUID` (`github.com/jdziat/simple-durable-jobs/v4/pkg/core`),
a defined string type — not `string`. The root facade re-exports it as
`jobs.UUID`.

Keep using `Queue.Register`, `Queue.Enqueue`, and `Queue.EnqueueRemote` directly
when job names are dynamic, configured at runtime, or produced by non-Go
systems. `Queue.EnqueueRemote` still permits producer-only enqueue, but rejects
malformed job names.

---

## Definitions

### `Define[A any, R any](q *queue.Queue, name string, fn any, opts ...queue.Option) *Def[A, R]`

Registers a typed handler and returns a typed definition handle. Like
`Queue.Register`, invalid handler registration panics. The result type `R` must
match the handler's return type.

`fn` is declared as `any` so `Define` accepts every handler shape
`Queue.Register` accepts (`func(ctx, A) (R, error)`, `func(A) (R, error)`, …)
and validates it reflectively. The consequence is that **`A` and `R` cannot be
inferred from the arguments — you must always write the type parameters out**:

```go
sendEmail := typed.Define[SendEmailArgs, SendEmailResult](queue, "send-email",
    func(ctx context.Context, args SendEmailArgs) (SendEmailResult, error) {
        return SendEmailResult{MessageID: "msg_123"}, nil
    })
```

Omitting them is a compile error (`cannot infer A`). Use
[`DefineE`](#definee) instead of `Define` when the handler name or function is
configuration-driven and a returned error is preferable to a panic; it has the
same signature and the same explicit-type-parameter requirement.

### `DefineE[A any, R any](q *queue.Queue, name string, fn any, opts ...queue.Option) (*Def[A, R], error)`

The error-returning form of `Define`. It reports handler/argument/result type
mismatches and registration failures as an error instead of panicking.

```go
sendEmail, err := typed.DefineE[SendEmailArgs, SendEmailResult](queue, "send-email",
    func(ctx context.Context, args SendEmailArgs) (SendEmailResult, error) {
        return SendEmailResult{MessageID: "msg_123"}, nil
    })
if err != nil {
    return err
}
```

### `DeclareUnchecked[A any, R any](q *queue.Queue, name string) *Def[A, R]`

Returns a typed handle without registering a local handler. Use this in
producer-only processes that enqueue work for workers running elsewhere.

```go
sendEmail := typed.DeclareUnchecked[SendEmailArgs, SendEmailResult](queue, "send-email")
jobID, err := sendEmail.EnqueueRemote(ctx, SendEmailArgs{To: "user@example.com"})
```

`DeclareUnchecked` cannot validate that the remote worker's handler uses the
same argument and result types. Keep those types synchronized with the worker.

### `DefineVoid[A any](q *queue.Queue, name string, fn func(context.Context, A) error, opts ...queue.Option) *Def[A, struct{}]`

Registers an error-only handler. The definition uses `struct{}` as the typed
result so it can still be called and loaded consistently.

Unlike `Define`, `DefineVoid` takes a *typed* `fn`, so `A` is inferred from the
handler literal and the type parameter can be omitted.

```go
cleanup := typed.DefineVoid(queue, "cleanup", func(ctx context.Context, args CleanupArgs) error {
    return nil
})
```

---

## Workflow primitives

Typed definitions cover the job name, argument type, and result type. Workflow
primitives are package-level functions because their result type is independent
from any single `Def[A, R]`.

### `FanOut[T any](ctx context.Context, subJobs []fanout.SubJob, opts ...fanout.Option) ([]typed.Result[T], error)`

Spawns sub-jobs and decodes each successful sub-job result as `T`, matching
`jobs.FanOut[T]` behavior.

```go
subs := []typed.SubJob{
    typed.SubJobOf(processItem, ProcessItemArgs{ID: "item-1"}),
}
results, err := typed.FanOut[ProcessItemResult](ctx, subs)
```

### `SubJobOf[A any, R any](def *Def[A, R], args A, opts ...queue.Option) typed.SubJob`

Builds a fan-out sub-job from a typed definition. The sub-job still routes by
the definition's string job name, but `args` must match the definition's
argument type.

### `WaitForSignal[T any](ctx context.Context, name string) (T, error)`

Consumes the oldest pending signal of `name` and decodes the payload as `T`.

```go
approval, err := typed.WaitForSignal[Approval](ctx, "approval")
```

### `WaitForSignalTimeout[T any](ctx context.Context, name string, d time.Duration) (T, bool, error)`

Waits for a typed signal until the durable deadline. It returns `ok=false` when
the deadline wins.

```go
approval, ok, err := typed.WaitForSignalTimeout[Approval](ctx, "approval", time.Hour)
```

### `Signal(ctx context.Context, q *queue.Queue, jobID core.UUID, name string, payload any) error`

Sends a signal to a specific job ID. The payload is still accepted as `any`
because senders often live outside the workflow and may not share a typed
definition handle for the receiving wait.

```go
err := typed.Signal(ctx, queue, jobID, "approval", Approval{ApprovedBy: "alice"})
```

The typed package deliberately does not wrap everything from the root facade.
Use root `jobs.Sleep`, `jobs.CheckSignal`, `jobs.DrainSignals`, result helpers,
and operational APIs when you need them. The typed package also avoids importing
the root package; it delegates to `pkg/fanout`, `pkg/signal`, and `pkg/queue` so
it remains a thin typed layer over the same durable engine.

## `Def[A, R]`

### `(*Def[A, R]) Name() string`

Returns the string job type used for routing.

```go
name := sendEmail.Name()
```

### `(*Def[A, R]) Enqueue(ctx context.Context, args A, opts ...queue.Option) (core.UUID, error)`

Adds a typed job to the queue.

```go
jobID, err := sendEmail.Enqueue(ctx, SendEmailArgs{To: "user@example.com"},
    jobs.QueueOpt("emails"),
    jobs.Retries(5),
)
```

### `(*Def[A, R]) EnqueueRemote(ctx context.Context, args A, opts ...queue.Option) (core.UUID, error)`

Adds a typed job without requiring a local handler registration. This is the
typed wrapper for `Queue.EnqueueRemote`; malformed job names are rejected.

```go
jobID, err := sendEmail.EnqueueRemote(ctx, SendEmailArgs{To: "user@example.com"})
```

### `(*Def[A, R]) EnqueueTx(ctx context.Context, tx *gorm.DB, args A, opts ...queue.Option) (core.UUID, error)`

Adds a typed job inside a caller-owned GORM transaction.

```go
jobID, err := sendEmail.EnqueueTx(ctx, tx, SendEmailArgs{To: "user@example.com"},
    jobs.Unique("email:user@example.com"),
)
```

### `(*Def[A, R]) Call(ctx context.Context, args A) (R, error)`

Runs the definition as a durable nested call from inside another job handler.
The call is checkpointed with the same replay behavior as `jobs.Call`.

```go
receipt, err := chargePayment.Call(ctx, PaymentArgs{OrderID: order.ID, Cents: order.Cents})
```

### `(*Def[A, R]) Load(ctx context.Context, jobID core.UUID) (R, error)`

Decodes the persisted result for a completed job. It returns the same sentinel
errors as `jobs.LoadResult`: `ErrJobNotCompleted`, `ErrJobFailed`,
`ErrJobCancelled`, `ErrNoResult`, and `ErrJobNotFound`.

```go
result, err := sendEmail.Load(ctx, jobID)
```

---

## Complete Example

```go
package main

import (
    "context"
    "fmt"

    jobs "github.com/jdziat/simple-durable-jobs/v4"
    typed "github.com/jdziat/simple-durable-jobs/v4/pkg/typed"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

type SendEmailArgs struct {
    To string `json:"to"`
}

type SendEmailResult struct {
    MessageID string `json:"message_id"`
}

func main() {
    ctx := context.Background()
    db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("typed.db")), &gorm.Config{})
    if err != nil {
        panic(err)
    }
    storage := jobs.NewGormStorage(db)
    if err := storage.Migrate(ctx); err != nil {
        panic(err)
    }
    queue := jobs.New(storage)

    sendEmail := typed.Define[SendEmailArgs, SendEmailResult](queue, "send-email",
        func(ctx context.Context, args SendEmailArgs) (SendEmailResult, error) {
            return SendEmailResult{MessageID: "msg_" + args.To}, nil
        })

    jobID, err := sendEmail.Enqueue(ctx, SendEmailArgs{To: "user@example.com"})
    if err != nil {
        panic(err)
    }

    fmt.Println("enqueued", jobID, "as", sendEmail.Name())
}
```
