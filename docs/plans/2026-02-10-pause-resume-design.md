# Pause and Resume Jobs Design

## Overview

Add pause and resume functionality at three levels:
- **Individual job** - Pause/resume specific jobs by ID
- **Queue-level** - Pause/resume entire queues
- **Worker-level** - Pause/resume worker processing

## Pause Modes

Each pause operation can specify a mode:

| Mode | Behavior |
|------|----------|
| **Graceful** | Running jobs complete normally, no new jobs picked up |
| **Aggressive** | Running jobs interrupted immediately via context cancellation |

Default mode is Graceful.

## Resume Behavior

When an interrupted job resumes:
1. Load existing checkpoints
2. Replay checkpointed calls (skip re-execution)
3. Continue from where it left off
4. If no checkpoints exist, restart from beginning

---

## Data Structures

### New Job Status

```go
const (
    StatusPaused JobStatus = "paused"  // Job is paused, won't be picked up
)
```

### New Database Table - `queue_states`

```go
type QueueState struct {
    Queue     string    `gorm:"primaryKey"`
    Paused    bool      `gorm:"default:false"`
    PausedAt  *time.Time
    PausedBy  string    // optional: who paused it
    UpdatedAt time.Time
}
```

### Pause Options

```go
type PauseMode string

const (
    PauseModeGraceful   PauseMode = "graceful"
    PauseModeAggressive PauseMode = "aggressive"
)

type PauseOptions struct {
    Mode PauseMode
}
```

---

## Storage Interface

New methods on `Storage` interface:

```go
// Job-level pause/resume
PauseJob(ctx context.Context, jobID string, mode PauseMode) error
ResumeJob(ctx context.Context, jobID string) error
GetPausedJobs(ctx context.Context, queue string) ([]*Job, error)
IsJobPaused(ctx context.Context, jobID string) (bool, error)

// Queue-level pause/resume
PauseQueue(ctx context.Context, queue string) error
ResumeQueue(ctx context.Context, queue string) error
GetPausedQueues(ctx context.Context) ([]string, error)
IsQueuePaused(ctx context.Context, queue string) (bool, error)

// Cache refresh (called by workers periodically)
RefreshQueueStates(ctx context.Context) (map[string]bool, error)
```

### Dequeue Modification

Before returning a job:
- Check if job's queue is paused (using cached state)
- Skip jobs with `status = 'paused'`
- Query: `WHERE status = 'pending' AND queue NOT IN (paused_queues)`

---

## Worker Changes

### New Fields

```go
type Worker struct {
    // ... existing fields

    paused          atomic.Bool
    pauseMode       PauseMode
    pauseMu         sync.RWMutex

    // Cached queue pause states (refreshed every 5s)
    queueStates     map[string]bool
    queueStatesMu   sync.RWMutex
    lastStateRefresh time.Time
}
```

### Worker Pause Behavior

| Mode | Stop Polling | Stop Heartbeats | In-flight Jobs |
|------|-------------|-----------------|----------------|
| Graceful | Yes | No | Complete normally |
| Aggressive | Yes | Yes | Locks expire, reclaimed by others |

### Queue State Caching

- Worker calls `storage.RefreshQueueStates()` every 5 seconds
- Dequeue checks local cache before fetching jobs
- Avoids hitting DB on every poll cycle

---

## Aggressive Interruption

### Cancellation Mechanism

1. Each running job gets a `context.WithCancel`
2. Worker tracks: `runningJobs map[string]context.CancelFunc`
3. `PauseJob(id, Aggressive)` calls the cancel func for that job
4. Handler receives cancelled context, should respect `ctx.Done()`

### Job-level Aggressive Pause

1. Update job: `status = 'paused'`, clear `locked_by` and `locked_until`
2. Worker's context gets cancelled via cancellation channel
3. Worker detects cancellation, stops execution, doesn't mark as failed

### Queue-level Aggressive Pause

1. Mark queue as paused in DB
2. Find all running jobs in that queue
3. Cancel each one and set `status = 'paused'`

### Handler Requirements

Handlers should check `ctx.Done()` periodically for responsive interruption. Long-running handlers that ignore context will only stop at the next checkpoint or completion.

---

## Public API

### Standalone Functions

```go
// Job-level
func PauseJob(ctx context.Context, storage Storage, jobID string, opts ...PauseOption) error
func ResumeJob(ctx context.Context, storage Storage, jobID string) error
func GetPausedJobs(ctx context.Context, storage Storage, queue string) ([]*Job, error)
func IsJobPaused(ctx context.Context, storage Storage, jobID string) (bool, error)

// Queue-level
func PauseQueue(ctx context.Context, storage Storage, queue string) error
func ResumeQueue(ctx context.Context, storage Storage, queue string) error
func GetPausedQueues(ctx context.Context, storage Storage) ([]string, error)
func IsQueuePaused(ctx context.Context, storage Storage, queue string) (bool, error)
```

### Convenience Methods on Queue

```go
func (q *Queue) PauseJob(ctx context.Context, jobID string, opts ...PauseOption) error
func (q *Queue) ResumeJob(ctx context.Context, jobID string) error
func (q *Queue) Pause(ctx context.Context) error
func (q *Queue) Resume(ctx context.Context) error
func (q *Queue) IsPaused(ctx context.Context) (bool, error)
func (q *Queue) GetPausedJobs(ctx context.Context) ([]*Job, error)
```

### Convenience Methods on Worker

```go
func (w *Worker) Pause(mode PauseMode)
func (w *Worker) Resume()
func (w *Worker) IsPaused() bool
```

### Pause Options Pattern

```go
func WithPauseMode(mode PauseMode) PauseOption
// Default is Graceful
```

---

## Error Handling

### New Errors

```go
var (
    ErrJobAlreadyPaused    = errors.New("job is already paused")
    ErrJobNotPaused        = errors.New("job is not paused")
    ErrQueueAlreadyPaused  = errors.New("queue is already paused")
    ErrQueueNotPaused      = errors.New("queue is not paused")
    ErrCannotPauseStatus   = errors.New("cannot pause job in current status")
)
```

### Pausable Job Statuses

| Status | Can Pause? | Notes |
|--------|-----------|-------|
| pending | Yes | Straightforward |
| running | Yes | Graceful waits, aggressive interrupts |
| waiting | Yes | Paused until resumed, fan-out still tracked |
| completed | No | Already done |
| failed | No | Already terminal |
| cancelled | No | Already terminal |
| paused | No | Already paused |

### Edge Cases

1. **Pause a job that completes before interruption** - No-op, return success
2. **Resume a job whose queue is paused** - Job goes to `pending`, won't be picked up until queue resumes
3. **Aggressive pause during checkpoint save** - Checkpoint completes (atomic), then job pauses
4. **Worker crashes while job paused** - No issue, job stays paused (not locked)
5. **Pause parent job with running fan-out** - Sub-jobs continue, parent stays paused

---

## Events

### New Event Types

```go
const (
    EventJobPaused      EventType = "job_paused"
    EventJobResumed     EventType = "job_resumed"
    EventQueuePaused    EventType = "queue_paused"
    EventQueueResumed   EventType = "queue_resumed"
    EventWorkerPaused   EventType = "worker_paused"
    EventWorkerResumed  EventType = "worker_resumed"
)
```

### Event Payloads

```go
type JobPausedEvent struct {
    JobID string
    Mode  PauseMode
}

type QueuePausedEvent struct {
    Queue string
}
```

---

## Testing

1. **Unit tests** for storage layer pause/resume operations
2. **Unit tests** for worker pause behavior (graceful vs aggressive)
3. **Integration tests:**
   - Pause job mid-execution, resume, verify checkpoint replay
   - Pause queue, verify no jobs dequeued, resume, verify processing continues
   - Aggressive pause, verify context cancellation
   - Pause across worker restart, verify state persists
