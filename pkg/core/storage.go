package core

import (
	"context"
	"time"
)

// Starter is the interface for starting workers.
type Starter interface {
	Start(ctx context.Context) error
}

// Storage defines the persistence layer for jobs.
type Storage interface {
	// Migrate creates the necessary database tables.
	Migrate(ctx context.Context) error

	// Job lifecycle
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context, queues []string, workerID string) (*Job, error)
	Complete(ctx context.Context, jobID string, workerID string) error
	Fail(ctx context.Context, jobID string, workerID string, errMsg string, retryAt *time.Time) error

	// Uniqueness
	EnqueueUnique(ctx context.Context, job *Job, uniqueKey string) error

	// Checkpointing
	SaveCheckpoint(ctx context.Context, cp *Checkpoint) error
	GetCheckpoints(ctx context.Context, jobID string) ([]Checkpoint, error)
	DeleteCheckpoints(ctx context.Context, jobID string) error

	// Scheduling
	GetDueJobs(ctx context.Context, queues []string, limit int) ([]*Job, error)

	// Locking
	Heartbeat(ctx context.Context, jobID string, workerID string) error
	ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error)

	// Queries
	GetJob(ctx context.Context, jobID string) (*Job, error)
	GetJobsByStatus(ctx context.Context, status JobStatus, limit int) ([]*Job, error)

	// Fan-out operations
	CreateFanOut(ctx context.Context, fanOut *FanOut) error
	GetFanOut(ctx context.Context, fanOutID string) (*FanOut, error)
	IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*FanOut, error)
	IncrementFanOutFailed(ctx context.Context, fanOutID string) (*FanOut, error)
	IncrementFanOutCancelled(ctx context.Context, fanOutID string) (*FanOut, error)
	UpdateFanOutStatus(ctx context.Context, fanOutID string, status FanOutStatus) (bool, error)
	GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*FanOut, error)

	// Sub-job operations
	EnqueueBatch(ctx context.Context, jobs []*Job) error
	GetSubJobs(ctx context.Context, fanOutID string) ([]*Job, error)
	GetSubJobResults(ctx context.Context, fanOutID string) ([]*Job, error)
	CancelSubJobs(ctx context.Context, fanOutID string) (int64, error)
	CancelSubJob(ctx context.Context, jobID string) (*FanOut, error)

	// Waiting job operations
	SuspendJob(ctx context.Context, jobID string, workerID string) error
	ResumeJob(ctx context.Context, jobID string) (bool, error)
	GetWaitingJobsToResume(ctx context.Context) ([]*Job, error)

	// Result storage
	SaveJobResult(ctx context.Context, jobID string, workerID string, result []byte) error

	// Job pause operations
	PauseJob(ctx context.Context, jobID string) error
	UnpauseJob(ctx context.Context, jobID string) error
	GetPausedJobs(ctx context.Context, queue string) ([]*Job, error)
	IsJobPaused(ctx context.Context, jobID string) (bool, error)

	// Queue pause operations
	PauseQueue(ctx context.Context, queue string) error
	UnpauseQueue(ctx context.Context, queue string) error
	GetPausedQueues(ctx context.Context) ([]string, error)
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	RefreshQueueStates(ctx context.Context) (map[string]bool, error)
}
