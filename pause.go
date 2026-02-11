package jobs

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// PauseMode determines how pause operations behave.
type PauseMode = core.PauseMode

// Pause mode constants.
const (
	// PauseModeGraceful lets running jobs complete, stops new jobs from being picked up.
	PauseModeGraceful = core.PauseModeGraceful
	// PauseModeAggressive interrupts running jobs immediately via context cancellation.
	PauseModeAggressive = core.PauseModeAggressive
)

// PauseJob pauses a specific job by ID.
func PauseJob(ctx context.Context, storage Storage, jobID string) error {
	return storage.PauseJob(ctx, jobID)
}

// ResumeJob resumes a paused job.
func ResumeJob(ctx context.Context, storage Storage, jobID string) error {
	return storage.UnpauseJob(ctx, jobID)
}

// IsJobPaused checks if a job is paused.
func IsJobPaused(ctx context.Context, storage Storage, jobID string) (bool, error) {
	return storage.IsJobPaused(ctx, jobID)
}

// GetPausedJobs returns all paused jobs in a queue.
func GetPausedJobs(ctx context.Context, storage Storage, queue string) ([]*Job, error) {
	if err := security.ValidateQueueName(queue); err != nil {
		return nil, err
	}
	return storage.GetPausedJobs(ctx, queue)
}

// PauseQueue pauses an entire queue.
func PauseQueue(ctx context.Context, storage Storage, queue string) error {
	if err := security.ValidateQueueName(queue); err != nil {
		return err
	}
	return storage.PauseQueue(ctx, queue)
}

// ResumeQueue resumes a paused queue.
func ResumeQueue(ctx context.Context, storage Storage, queue string) error {
	if err := security.ValidateQueueName(queue); err != nil {
		return err
	}
	return storage.UnpauseQueue(ctx, queue)
}

// IsQueuePaused checks if a queue is paused.
func IsQueuePaused(ctx context.Context, storage Storage, queue string) (bool, error) {
	if err := security.ValidateQueueName(queue); err != nil {
		return false, err
	}
	return storage.IsQueuePaused(ctx, queue)
}

// GetPausedQueues returns all paused queue names.
func GetPausedQueues(ctx context.Context, storage Storage) ([]string, error) {
	return storage.GetPausedQueues(ctx)
}

// PauseOption configures pause behavior.
type PauseOption interface {
	applyPause(*pauseOptions)
}

type pauseOptions struct {
	mode core.PauseMode
}

type pauseModeOpt struct {
	mode core.PauseMode
}

func (o pauseModeOpt) applyPause(opts *pauseOptions) {
	opts.mode = o.mode
}

// WithPauseMode sets the pause mode for pause operations.
func WithPauseMode(mode PauseMode) PauseOption {
	return pauseModeOpt{mode: mode}
}
