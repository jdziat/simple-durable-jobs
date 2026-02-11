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
