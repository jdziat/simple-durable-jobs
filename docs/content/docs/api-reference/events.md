---
title: "Events & Hooks"
weight: 8
---

## Events

### `(*Queue) Events() <-chan Event`

Returns a channel that receives job events. Caller must call `Unsubscribe` when done.

### `(*Queue) Unsubscribe(ch <-chan Event)`

Removes a subscriber channel. The channel is not closed; callers must stop reading before calling this.

### `(*Queue) Emit(event Event)`

Emits an event to all subscribers. Non-blocking; drops events if a subscriber's buffer is full.

### `(*Queue) EmitCustomEvent(jobID, kind string, data map[string]any)`

Emits a custom ephemeral event (not persisted).

### Event Types

Every event implements the `Event` interface. Type-switch on the pointer type in your subscriber loop; each payload's fields are listed below.

```go
// Lifecycle
type JobStarted struct {
    Job       *Job
    Timestamp time.Time
}

type JobCompleted struct {
    Job       *Job
    Duration  time.Duration
    Timestamp time.Time
}

type JobFailed struct {
    Job       *Job
    Error     error
    Timestamp time.Time
}

type JobRetrying struct {
    Job       *Job
    Attempt   int
    Error     error
    NextRunAt time.Time
    Timestamp time.Time
}

type CheckpointSaved struct {
    JobID     string
    CallIndex int
    CallType  string  // e.g. "call", "fanout", "phase"
    Timestamp time.Time
}

// Pause / resume
type JobPaused struct {
    Job       *Job
    Mode      PauseMode
    Timestamp time.Time
}

type JobResumed struct {
    Job       *Job
    Timestamp time.Time
}

type QueuePaused struct {
    Queue     string
    Timestamp time.Time
}

type QueueResumed struct {
    Queue     string
    Timestamp time.Time
}

type WorkerPaused struct {
    WorkerID  string
    Mode      PauseMode
    Timestamp time.Time
}

type WorkerResumed struct {
    WorkerID  string
    Timestamp time.Time
}

// Ephemeral / custom
type CustomEvent struct {
    JobID     string
    Kind      string         // "progress", "phase_change", "log", …
    Data      map[string]any
    Timestamp time.Time
}
```

---

## Hooks

### `(*Queue) OnJobStart(fn func(context.Context, *Job))`

Registers a callback for when a job starts processing.

### `(*Queue) OnJobStartCtx(fn func(context.Context, *Job) context.Context)`

Registers a context-transforming callback that runs when a job starts. The returned `context.Context` is threaded into the handler, so hooks can attach values (OTel spans, tenant IDs, correlation IDs, …) that downstream code reads out of the context. This is the hook the built-in OTel instrumentation (`pkg/otel.Instrument`) uses to re-attach the enqueue-time trace span to the worker-side handler.

### `(*Queue) OnJobComplete(fn func(context.Context, *Job))`

Registers a callback for when a job completes successfully.

### `(*Queue) OnJobFail(fn func(context.Context, *Job, error))`

Registers a callback for when a job fails permanently.

### `(*Queue) OnRetry(fn func(context.Context, *Job, int, error))`

Registers a callback for when a job is being retried.
