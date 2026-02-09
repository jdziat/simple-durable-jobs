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
}
