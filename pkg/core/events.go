package core

import "time"

// Event is the interface for all queue events.
type Event interface {
	eventMarker()
}

// JobStarted is emitted when a job starts processing.
type JobStarted struct {
	Job       *Job
	Timestamp time.Time
}

func (*JobStarted) eventMarker() {}

// JobCompleted is emitted when a job completes successfully.
type JobCompleted struct {
	Job       *Job
	Duration  time.Duration
	Timestamp time.Time
}

func (*JobCompleted) eventMarker() {}

// JobFailed is emitted when a job fails permanently.
type JobFailed struct {
	Job       *Job
	Error     error
	Timestamp time.Time
}

func (*JobFailed) eventMarker() {}

// JobRetrying is emitted when a job is retried.
type JobRetrying struct {
	Job       *Job
	Attempt   int
	Error     error
	NextRunAt time.Time
	Timestamp time.Time
}

func (*JobRetrying) eventMarker() {}

// CheckpointSaved is emitted when a checkpoint is saved.
type CheckpointSaved struct {
	JobID     string
	CallIndex int
	CallType  string
	Timestamp time.Time
}

func (*CheckpointSaved) eventMarker() {}

// JobPaused is emitted when a job is paused.
type JobPaused struct {
	Job       *Job
	Mode      PauseMode
	Timestamp time.Time
}

func (*JobPaused) eventMarker() {}

// JobResumed is emitted when a job is resumed.
type JobResumed struct {
	Job       *Job
	Timestamp time.Time
}

func (*JobResumed) eventMarker() {}

// JobResumedBySignal is emitted when a persisted signal wakes a job that was
// suspended waiting for signals. SignalName is optional: producer-side wakes set
// it from Signal's name, while recovery/backstop paths may leave it empty if the
// storage backend can confirm a pending signal but cannot cheaply report which
// signal caused the wake. Durable timer wakes, including expired Sleep and
// timeout-deadline resumes without a pending signal, are not signal resumes and
// should not emit this event.
type JobResumedBySignal struct {
	JobID      string
	SignalName string
	Timestamp  time.Time
}

func (*JobResumedBySignal) eventMarker() {}

// SignalDelivered is emitted when a signal is successfully persisted for a job
// (via Signal). It lets an orchestrator observe that a signal landed; the job
// consumes it later via WaitForSignal/CheckSignal/DrainSignals.
type SignalDelivered struct {
	JobID     string
	Name      string
	Timestamp time.Time
}

func (*SignalDelivered) eventMarker() {}

// QueuePaused is emitted when a queue is paused.
type QueuePaused struct {
	Queue     string
	Timestamp time.Time
}

func (*QueuePaused) eventMarker() {}

// QueueResumed is emitted when a queue is resumed.
type QueueResumed struct {
	Queue     string
	Timestamp time.Time
}

func (*QueueResumed) eventMarker() {}

// WorkerPaused is emitted when a worker is paused.
type WorkerPaused struct {
	WorkerID  string
	Mode      PauseMode
	Timestamp time.Time
}

func (*WorkerPaused) eventMarker() {}

// WorkerResumed is emitted when a worker is resumed.
type WorkerResumed struct {
	WorkerID  string
	Timestamp time.Time
}

func (*WorkerResumed) eventMarker() {}

// CustomEvent is emitted by workers to carry arbitrary progress data
// through the Events() channel without requiring new event types.
type CustomEvent struct {
	JobID     string
	Kind      string // "progress", "phase_change", "log", etc.
	Data      map[string]any
	Timestamp time.Time
}

func (*CustomEvent) eventMarker() {}
