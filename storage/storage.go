// Package storage defines the storage interface for the jobs library.
package storage

import (
	"context"
	"time"

	"github.com/jdziat/simple-durable-jobs"
)

// Storage defines the persistence layer for jobs.
type Storage interface {
	// Migrate creates the necessary database tables.
	Migrate(ctx context.Context) error

	// Job lifecycle
	Enqueue(ctx context.Context, job *jobs.Job) error
	Dequeue(ctx context.Context, queues []string, workerID string) (*jobs.Job, error)
	Complete(ctx context.Context, jobID string, result []byte) error
	Fail(ctx context.Context, jobID string, errMsg string, retryAt *time.Time) error

	// Checkpointing
	SaveCheckpoint(ctx context.Context, cp *jobs.Checkpoint) error
	GetCheckpoints(ctx context.Context, jobID string) ([]jobs.Checkpoint, error)
	DeleteCheckpoints(ctx context.Context, jobID string) error

	// Scheduling
	GetDueJobs(ctx context.Context, queues []string, limit int) ([]*jobs.Job, error)

	// Locking
	Heartbeat(ctx context.Context, jobID string, workerID string) error
	ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error)

	// Queries
	GetJob(ctx context.Context, jobID string) (*jobs.Job, error)
	GetJobsByStatus(ctx context.Context, status jobs.JobStatus, limit int) ([]*jobs.Job, error)
}