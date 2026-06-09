---
title: "Typed API"
weight: 4
---

## Package `typed`

```go
import typed "github.com/jdziat/simple-durable-jobs/v2/pkg/typed"
```

The typed API adds compile-time checked handles over the existing string-keyed
queue. Routing still uses the registered job name, so it works with the same
workers, storage, checkpoints, middleware, and payload codec as `Queue.Register`
and `Queue.Enqueue`.

Keep using `Queue.Register`, `Queue.Enqueue`, and `Queue.EnqueueRemote` directly
when job names are dynamic, configured at runtime, or produced by non-Go
systems.

---

## Definitions

### `Define[A any, R any](q *queue.Queue, name string, fn func(context.Context, A) (R, error), opts ...queue.Option) *Def[A, R]`

Registers a typed handler and returns a typed definition handle. Like
`Queue.Register`, invalid handler registration panics.

```go
sendEmail := typed.Define(queue, "send-email", func(ctx context.Context, args SendEmailArgs) (SendEmailResult, error) {
    return SendEmailResult{MessageID: "msg_123"}, nil
})
```

### `Declare[A any, R any](q *queue.Queue, name string) *Def[A, R]`

Returns a typed handle without registering a local handler. Use this in
producer-only processes that enqueue work for workers running elsewhere.

```go
sendEmail := typed.Declare[SendEmailArgs, SendEmailResult](queue, "send-email")
jobID, err := sendEmail.EnqueueRemote(ctx, SendEmailArgs{To: "user@example.com"})
```

`Declare` cannot validate that the remote worker's handler uses the same
argument and result types. Keep those types synchronized with the worker.

### `DefineVoid[A any](q *queue.Queue, name string, fn func(context.Context, A) error, opts ...queue.Option) *Def[A, struct{}]`

Registers an error-only handler. The definition uses `struct{}` as the typed
result so it can still be called and loaded consistently.

```go
cleanup := typed.DefineVoid(queue, "cleanup", func(ctx context.Context, args CleanupArgs) error {
    return nil
})
```

---

## `Def[A, R]`

### `(*Def[A, R]) Name() string`

Returns the string job type used for routing.

```go
name := sendEmail.Name()
```

### `(*Def[A, R]) Enqueue(ctx context.Context, args A, opts ...queue.Option) (string, error)`

Adds a typed job to the queue.

```go
jobID, err := sendEmail.Enqueue(ctx, SendEmailArgs{To: "user@example.com"},
    jobs.QueueOpt("emails"),
    jobs.Retries(5),
)
```

### `(*Def[A, R]) EnqueueRemote(ctx context.Context, args A, opts ...queue.Option) (string, error)`

Adds a typed job without requiring a local handler registration. This is the
typed wrapper for `Queue.EnqueueRemote`.

```go
jobID, err := sendEmail.EnqueueRemote(ctx, SendEmailArgs{To: "user@example.com"})
```

### `(*Def[A, R]) EnqueueTx(ctx context.Context, tx *gorm.DB, args A, opts ...queue.Option) (string, error)`

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

### `(*Def[A, R]) Load(ctx context.Context, jobID string) (R, error)`

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

    jobs "github.com/jdziat/simple-durable-jobs/v2"
    typed "github.com/jdziat/simple-durable-jobs/v2/pkg/typed"
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

    sendEmail := typed.Define(queue, "send-email", func(ctx context.Context, args SendEmailArgs) (SendEmailResult, error) {
        return SendEmailResult{MessageID: "msg_" + args.To}, nil
    })

    jobID, err := sendEmail.Enqueue(ctx, SendEmailArgs{To: "user@example.com"})
    if err != nil {
        panic(err)
    }

    fmt.Println("enqueued", jobID, "as", sendEmail.Name())
}
```
