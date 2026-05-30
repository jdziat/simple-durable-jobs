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
	// ReleaseStaleLocks releases locks on jobs whose heartbeat is older than
	// staleDuration and returns the IDs of the jobs that were reclaimed. The
	// caller can use those IDs to cancel in-flight handler contexts on the
	// local worker — without this, an orphaned handler keeps running until
	// its own heartbeat sees ErrJobNotOwned and times out (a slow path
	// added in the heartbeat-abandon fix).
	ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (releasedJobIDs []string, err error)
	// FindOrphanedJobs returns the subset of jobIDs that this worker thinks
	// it owns but the database disagrees with — either because the lock
	// changed hands (locked_by != workerID), the lock was released
	// (locked_by IS NULL), or the job moved to a terminal status
	// (cancelled/completed/failed). The caller is expected to cancel each
	// orphaned job's local handler context via Worker.CancelJob.
	//
	// This is the cross-worker counterpart of the cancellation that
	// completeFanOut and reapStaleLocks perform directly: when another
	// worker in the fleet cancels a sub-job (e.g. via fan-out failure) or
	// reaps a stale lock, THIS worker has no in-process signal that its
	// running handler should stop. Periodic FindOrphanedJobs polling
	// closes that gap with bounded query cost (one row per running job
	// per audit tick).
	FindOrphanedJobs(ctx context.Context, jobIDs []string, workerID string) ([]string, error)

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
	// CancelSubJobs marks all pending/running sub-jobs for a fan-out as
	// cancelled and returns the IDs of the jobs that were cancelled. The
	// caller is expected to invoke worker.CancelJob (or the cross-worker
	// equivalent) for each returned ID so in-flight handler contexts get
	// cancelled, not just the DB row. Without this, an in-flight sub-job
	// continues executing even after its fan-out has been marked failed —
	// observed in production on 2026-05-19.
	CancelSubJobs(ctx context.Context, fanOutID string) (cancelledJobIDs []string, err error)
	CancelSubJob(ctx context.Context, jobID string) (*FanOut, error)

	// Waiting job operations
	//
	// SuspendJob moves a running job into StatusWaiting (e.g. while it
	// waits for fan-out sub-jobs). The name is historical; a rename to
	// MarkWaiting is planned for the next major release.
	SuspendJob(ctx context.Context, jobID string, workerID string) error
	ResumeJob(ctx context.Context, jobID string) (bool, error)
	GetWaitingJobsToResume(ctx context.Context) ([]*Job, error)
	GetStalledFanOutParents(ctx context.Context, olderThan time.Time) ([]*Job, error)

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

// JobFilter holds search criteria for jobs used by UIStorage implementations.
type JobFilter struct {
	Status string
	Queue  string
	Type   string
	Search string
	Since  time.Time
	Until  time.Time
	Limit  int
	Offset int
}
