package jobs

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
)

// PauseMode determines how pause operations behave.
type PauseMode = core.PauseMode

// PauseOption configures pause behavior.
type PauseOption = queue.PauseOption

// Pause mode constants.
const (
	// PauseModeGraceful lets running jobs complete, stops new jobs from being picked up.
	PauseModeGraceful = core.PauseModeGraceful
	// PauseModeAggressive interrupts running jobs immediately via context cancellation.
	PauseModeAggressive = core.PauseModeAggressive
)

// PauseJob pauses a specific job by ID.
// This delegates to Queue.PauseJob so that running jobs have their context
// cancelled via the running-job registry when PauseModeAggressive is used.
func PauseJob(ctx context.Context, q *Queue, jobID UUID, opts ...PauseOption) error {
	return q.PauseJob(ctx, jobID, opts...)
}

// CancelJob terminally cancels a pending, waiting, or running job. The job
// moves to a terminal cancelled state and is not resumable via ResumeJob; use
// Requeue to replay a cancelled job from scratch. When the target is a fan-out
// parent, its entire descendant fan-out subtree is terminally cancelled in the
// same storage transaction. A locally-running handler's context is cancelled
// after the durable write; a handler that ignores ctx is not force-killed.
// Already-cancelled jobs are a no-op (nil); a job in a non-cancellable terminal
// state returns ErrJobNotCancellable, and a missing job returns ErrJobNotFound.
func CancelJob(ctx context.Context, q *Queue, jobID UUID) error {
	return q.CancelJob(ctx, jobID)
}

// ResumeJob resumes a paused job using storage only.
// It does not emit a JobResumed event; callers wanting events should use Queue.ResumeJob.
func ResumeJob(ctx context.Context, storage Storage, jobID UUID) error {
	return storage.UnpauseJob(ctx, jobID)
}

// IsJobPaused checks if a job is paused.
func IsJobPaused(ctx context.Context, storage Storage, jobID UUID) (bool, error) {
	return storage.IsJobPaused(ctx, jobID)
}

// GetPausedJobs returns all paused jobs in a queue.
func GetPausedJobs(ctx context.Context, storage Storage, queueName string) ([]*Job, error) {
	if err := security.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	return storage.GetPausedJobs(ctx, queueName)
}

// PauseQueue pauses an entire queue.
func PauseQueue(ctx context.Context, storage Storage, queueName string) error {
	if err := security.ValidateQueueName(queueName); err != nil {
		return err
	}
	return storage.PauseQueue(ctx, queueName)
}

// ResumeQueue resumes a paused queue.
func ResumeQueue(ctx context.Context, storage Storage, queueName string) error {
	if err := security.ValidateQueueName(queueName); err != nil {
		return err
	}
	return storage.UnpauseQueue(ctx, queueName)
}

// IsQueuePaused checks if a queue is paused.
func IsQueuePaused(ctx context.Context, storage Storage, queueName string) (bool, error) {
	if err := security.ValidateQueueName(queueName); err != nil {
		return false, err
	}
	return storage.IsQueuePaused(ctx, queueName)
}

// GetPausedQueues returns all paused queue names.
func GetPausedQueues(ctx context.Context, storage Storage) ([]string, error) {
	return storage.GetPausedQueues(ctx)
}

// WithPauseMode sets the pause mode for pause operations.
func WithPauseMode(mode PauseMode) PauseOption {
	return queue.WithPauseMode(mode)
}
