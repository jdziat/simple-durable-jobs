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

// Emitted when a signal wakes a waiting job (producer fast path or the
// worker's polling backstop). SignalName is best-effort and may be empty
// on backstop/recovery wakes. Durable-timer (Sleep) deadline wakes without
// a pending signal do NOT emit this event.
type JobResumedBySignal struct {
    JobID      string
    SignalName string
    Timestamp  time.Time
}

// Emitted when a signal is successfully persisted for a job.
type SignalDelivered struct {
    JobID     string
    Name      string
    Timestamp time.Time
}

// Emitted when a worker reclaims a job whose owner is presumed dead, or when a
// worker observes a peer reclaim one of its own in-flight jobs. WorkerID is the
// worker emitting the event; Reason is one of the ReclaimReason* constants:
//
//   - ReclaimReasonStaleLock     ("stale_lock")      — THIS worker's stale-lock
//     reaper recovered a job from a presumed-dead peer. This is the actor side
//     and the true crash leading-indicator ("I recovered N jobs"); alert on it.
//   - ReclaimReasonOwnershipAudit ("ownership_audit") — the ownership audit saw
//     a peer reclaim a job THIS worker was still running. This is the victim
//     side ("a peer took N of my in-flight jobs"), meaning this worker was
//     wrongly presumed dead or stalled.
//
// WARNING: in a multi-process fleet the SAME logical reclaim can surface once
// on the reaper (stale_lock) and once on the victim (ownership_audit), emitted
// by DIFFERENT workers. Keep the two reasons separable and do NOT sum across
// them when counting reclaims.
type JobReclaimed struct {
    JobID     string
    WorkerID  string
    Reason    string
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

### `(*Queue) OnJobReclaimed(fn func(context.Context, jobID, reason string))`

Registers a callback for when a job lease is reclaimed. It fires both when this worker's stale-lock reaper recovers a job from a presumed-dead owner and when the ownership audit observes a peer reclaim a job this worker was running. `reason` is `ReclaimReasonStaleLock` (`"stale_lock"`, the actor/crash-leading-indicator side) or `ReclaimReasonOwnershipAudit` (`"ownership_audit"`, the victim side). See the `JobReclaimed` event above — the same caveat applies: do not sum across reasons, since one logical reclaim can fire on both sides on different workers.
