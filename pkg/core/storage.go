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
	Complete(ctx context.Context, jobID UUID, workerID string) error
	Fail(ctx context.Context, jobID UUID, workerID string, errMsg string, retryAt *time.Time) error

	// Uniqueness
	EnqueueUnique(ctx context.Context, job *Job, uniqueKey string) error

	// Checkpointing
	SaveCheckpoint(ctx context.Context, cp *Checkpoint) error
	GetCheckpoints(ctx context.Context, jobID UUID) ([]Checkpoint, error)
	DeleteCheckpoints(ctx context.Context, jobID UUID) error

	// Scheduling
	GetDueJobs(ctx context.Context, queues []string, limit int) ([]*Job, error)
	ClaimScheduledFire(ctx context.Context, name string, fireTime time.Time) (bool, error)

	// Locking
	Heartbeat(ctx context.Context, jobID UUID, workerID string) error
	// Release returns an owned, still-running job to status=pending, clears
	// locked_by/locked_until/started_at, so a dequeued-but-undelivered job
	// (e.g. dropped at shutdown) is immediately re-runnable instead of
	// waiting for the stale-lock reaper.
	Release(ctx context.Context, jobID UUID, workerID string) error
	// ReleaseStaleLocks releases locks on jobs whose heartbeat is older than
	// staleDuration and returns the IDs of the jobs that were reclaimed. The
	// caller can use those IDs to cancel in-flight handler contexts on the
	// local worker — without this, an orphaned handler keeps running until
	// its own heartbeat sees ErrJobNotOwned and times out (a slow path
	// added in the heartbeat-abandon fix).
	ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (releasedJobIDs []UUID, err error)
	// FindOrphanedJobs returns the subset of jobIDs that this worker thinks
	// it owns but the database disagrees with — either because the lock
	// changed hands (locked_by != workerID), the lock was released
	// (locked_by IS NULL), or the job moved to a terminal status
	// (cancelled/completed/failed). The caller is expected to cancel each
	// orphaned job's local handler context via Worker.CancelJob.
	//
	// Jobs in 'waiting' or 'paused' status are NEVER reported, even though
	// they have a cleared locked_by: such a job has no running handler to
	// cancel (a parent that calls FanOut/Call suspends itself to 'waiting'
	// AFTER its handler has yielded), and is not reclaimable by another
	// worker while in that state (Dequeue takes only 'pending', the reaper
	// only 'running'). Reporting them would make the audit cancel the
	// worker's own about-to-return handler.
	//
	// This is the cross-worker counterpart of the cancellation that
	// completeFanOut and reapStaleLocks perform directly: when another
	// worker in the fleet cancels a sub-job (e.g. via fan-out failure) or
	// reaps a stale lock, THIS worker has no in-process signal that its
	// running handler should stop. Periodic FindOrphanedJobs polling
	// closes that gap with bounded query cost (one row per running job
	// per audit tick).
	FindOrphanedJobs(ctx context.Context, jobIDs []UUID, workerID string) ([]UUID, error)

	// Queries
	GetJob(ctx context.Context, jobID UUID) (*Job, error)
	GetJobsByStatus(ctx context.Context, status JobStatus, limit int) ([]*Job, error)

	// Fan-out operations
	CreateFanOut(ctx context.Context, fanOut *FanOut) error
	GetFanOut(ctx context.Context, fanOutID UUID) (*FanOut, error)
	IncrementFanOutCompleted(ctx context.Context, fanOutID UUID) (*FanOut, error)
	IncrementFanOutFailed(ctx context.Context, fanOutID UUID) (*FanOut, error)
	IncrementFanOutCancelled(ctx context.Context, fanOutID UUID) (*FanOut, error)
	UpdateFanOutStatus(ctx context.Context, fanOutID UUID, status FanOutStatus) (bool, error)
	GetFanOutsByParent(ctx context.Context, parentJobID UUID) ([]*FanOut, error)

	// Sub-job operations
	EnqueueBatch(ctx context.Context, jobs []*Job) error
	GetSubJobs(ctx context.Context, fanOutID UUID) ([]*Job, error)
	GetSubJobResults(ctx context.Context, fanOutID UUID) ([]*Job, error)
	// CancelSubJobs marks all pending/running sub-jobs for a fan-out as
	// cancelled and returns the IDs of the jobs that were cancelled. The
	// caller is expected to invoke worker.CancelJob (or the cross-worker
	// equivalent) for each returned ID so in-flight handler contexts get
	// cancelled, not just the DB row. Without this, an in-flight sub-job
	// continues executing even after its fan-out has been marked failed —
	// observed in production on 2026-05-19.
	CancelSubJobs(ctx context.Context, fanOutID UUID) (cancelledJobIDs []UUID, err error)
	CancelSubJob(ctx context.Context, jobID UUID) (*FanOut, error)

	// Waiting job operations
	//
	// MarkWaiting moves a running job into StatusWaiting (e.g. while it
	// waits for fan-out sub-jobs).
	MarkWaiting(ctx context.Context, jobID UUID, workerID string) error
	ResumeJob(ctx context.Context, jobID UUID) (bool, error)
	GetWaitingJobsToResume(ctx context.Context) ([]*Job, error)
	GetStalledFanOutParents(ctx context.Context, olderThan time.Time) ([]*Job, error)

	// Result storage
	SaveJobResult(ctx context.Context, jobID UUID, workerID string, result []byte) error

	// Job pause operations
	PauseJob(ctx context.Context, jobID UUID) error
	UnpauseJob(ctx context.Context, jobID UUID) error
	GetPausedJobs(ctx context.Context, queue string) ([]*Job, error)
	IsJobPaused(ctx context.Context, jobID UUID) (bool, error)

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
	// Tenant matches jobs owned by exactly this tenant.
	Tenant string
	// MetaContains requires every key/value pair to appear in the job metadata.
	// GormStorage implements this as a portable CAST-LIKE predicate over the
	// serialized JSON text; it is a substring match and may over-match values
	// containing the same JSON-fragment text.
	MetaContains *MetadataMap
	Search       string
	Since        time.Time
	Until        time.Time
	Limit        int
	Offset       int
	// SortKey selects the order column across the full result set. It is
	// validated against a storage-side whitelist; an empty or unknown value
	// falls back to the default (created_at). SortDir is "asc" or "desc"
	// (default desc). Never interpolated raw — see GormStorage.SearchJobs.
	SortKey string
	SortDir string
}
