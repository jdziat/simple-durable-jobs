package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// FanOut spawns sub-jobs in parallel and waits for all results.
// Checkpoints progress - safe to retry if parent crashes.
// When sub-jobs are created, the parent job is suspended.
// The worker should detect SuspendError and stop processing.
func FanOut[T any](ctx context.Context, subJobs []SubJob, opts ...Option) ([]Result[T], error) {
	if len(subJobs) == 0 {
		return nil, nil
	}

	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil, fmt.Errorf("fanout.FanOut must be used within a job handler")
	}

	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return nil, fmt.Errorf("fanout.FanOut: call state not initialized")
	}

	// Build config
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(cfg)
	}

	// Get call index for checkpointing
	cs.Mu.Lock()
	callIndex := cs.CallIndex
	cs.CallIndex++
	checkpoint, hasCheckpoint := cs.Checkpoints[callIndex]
	cs.Mu.Unlock()

	var fanOutID string

	if hasCheckpoint {
		// Resume: extract fan-out ID from checkpoint
		var fanOutCP core.FanOutCheckpoint
		if err := json.Unmarshal(checkpoint.Result, &fanOutCP); err != nil {
			return nil, fmt.Errorf("failed to unmarshal fan-out checkpoint: %w", err)
		}
		fanOutID = fanOutCP.FanOutID

		// Check if fan-out is complete - if so, collect results
		fanOut, err := jc.Storage.GetFanOut(ctx, fanOutID)
		if err != nil {
			return nil, fmt.Errorf("failed to get fan-out: %w", err)
		}
		if fanOut != nil && (fanOut.Status == core.FanOutCompleted || fanOut.Status == core.FanOutFailed) {
			// Fan-out is done, collect results
			return CollectResults[T](ctx, fanOutID)
		}

		// Fan-out not complete, suspend again
		if err := jc.Storage.SuspendJob(ctx, jc.Job.ID, jc.WorkerID); err != nil {
			return nil, fmt.Errorf("failed to suspend job: %w", err)
		}
		return nil, &SuspendError{FanOutID: fanOutID}
	}

	// First execution: create fan-out and sub-jobs
	fanOutID = uuid.New().String()

	// Create FanOut record
	fanOut := &core.FanOut{
		ID:           fanOutID,
		ParentJobID:  jc.Job.ID,
		TotalCount:   len(subJobs),
		Strategy:     cfg.strategy,
		Threshold:    cfg.threshold,
		CancelOnFail: cfg.cancelOnFailure,
	}
	if cfg.totalTimeout > 0 {
		timeout := time.Now().Add(cfg.totalTimeout)
		fanOut.TimeoutAt = &timeout
	}

	if err := jc.Storage.CreateFanOut(ctx, fanOut); err != nil {
		return nil, fmt.Errorf("failed to create fan-out: %w", err)
	}

	// Create sub-jobs
	jobs := make([]*core.Job, len(subJobs))
	for i, sj := range subJobs {
		args, err := json.Marshal(sj.Args)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal sub-job args: %w", err)
		}

		// Determine queue name with fallback chain
		queue := sj.Queue
		if queue == "" {
			queue = cfg.queue
		}
		if queue == "" {
			queue = jc.Job.Queue
		}

		// Validate queue name to prevent invalid sub-job queues
		if err := security.ValidateQueueName(queue); err != nil {
			return nil, fmt.Errorf("invalid sub-job queue name %q: %w", queue, err)
		}

		priority := sj.Priority
		if priority == 0 {
			priority = cfg.priority
		}

		// Determine retries with fallback, then clamp to security limits
		retries := sj.Retries
		if retries == 0 {
			retries = cfg.retries
		}
		retries = security.ClampRetries(retries)

		parentID := jc.Job.ID
		rootID := jc.Job.RootJobID
		if rootID == nil {
			rootID = &jc.Job.ID
		}

		jobs[i] = &core.Job{
			ID:          uuid.New().String(),
			Type:        sj.Type,
			Args:        args,
			Queue:       queue,
			Priority:    priority,
			MaxRetries:  retries,
			ParentJobID: &parentID,
			RootJobID:   rootID,
			FanOutID:    &fanOutID,
			FanOutIndex: i,
		}
	}

	if err := jc.Storage.EnqueueBatch(ctx, jobs); err != nil {
		return nil, fmt.Errorf("failed to enqueue sub-jobs: %w", err)
	}

	// Save checkpoint
	cpData, _ := json.Marshal(core.FanOutCheckpoint{
		FanOutID:  fanOutID,
		CallIndex: callIndex,
	})
	cp := &core.Checkpoint{
		ID:        uuid.New().String(),
		JobID:     jc.Job.ID,
		CallIndex: callIndex,
		CallType:  "fanout",
		Result:    cpData,
	}
	if err := jc.SaveCheckpoint(ctx, cp); err != nil {
		return nil, fmt.Errorf("failed to save fan-out checkpoint: %w", err)
	}

	// Suspend parent job and wait for resume
	if err := jc.Storage.SuspendJob(ctx, jc.Job.ID, jc.WorkerID); err != nil {
		return nil, fmt.Errorf("failed to suspend job: %w", err)
	}

	// Signal to worker that this job should not continue
	return nil, &SuspendError{FanOutID: fanOutID}
}

// SuspendError signals the worker to stop processing and wait for resume.
type SuspendError struct {
	FanOutID string
}

func (e *SuspendError) Error() string {
	return fmt.Sprintf("job suspended waiting for fan-out %s", e.FanOutID)
}

// IsSuspendError checks if an error is a suspend signal.
func IsSuspendError(err error) bool {
	_, ok := err.(*SuspendError)
	return ok
}

// CollectResults gathers results from completed sub-jobs.
// Called when parent job resumes after fan-out.
func CollectResults[T any](ctx context.Context, fanOutID string) ([]Result[T], error) {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil, fmt.Errorf("CollectResults must be used within a job handler")
	}

	fanOut, err := jc.Storage.GetFanOut(ctx, fanOutID)
	if err != nil {
		return nil, fmt.Errorf("failed to get fan-out: %w", err)
	}
	if fanOut == nil {
		return nil, fmt.Errorf("fan-out not found: %s", fanOutID)
	}

	jobs, err := jc.Storage.GetSubJobResults(ctx, fanOutID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sub-job results: %w", err)
	}

	// Pre-allocate results slice with correct size
	results := make([]Result[T], fanOut.TotalCount)
	for _, job := range jobs {
		if job.FanOutIndex >= len(results) {
			continue
		}
		results[job.FanOutIndex] = Result[T]{
			Index: job.FanOutIndex,
		}
		if job.Status == core.StatusFailed {
			results[job.FanOutIndex].Err = fmt.Errorf("%s", job.LastError)
		} else if job.Result != nil {
			var value T
			if err := json.Unmarshal(job.Result, &value); err != nil {
				results[job.FanOutIndex].Err = fmt.Errorf("failed to unmarshal result: %w", err)
			} else {
				results[job.FanOutIndex].Value = value
			}
		}
	}

	// Check if fan-out failed
	if fanOut.Status == core.FanOutFailed {
		failures := make([]SubJobFailure, 0)
		for _, job := range jobs {
			if job.Status == core.StatusFailed {
				failures = append(failures, SubJobFailure{
					Index:   job.FanOutIndex,
					JobID:   job.ID,
					Error:   job.LastError,
					Attempt: job.Attempt,
				})
			}
		}
		return results, &Error{
			FanOutID:    fanOutID,
			TotalCount:  fanOut.TotalCount,
			FailedCount: fanOut.FailedCount,
			Strategy:    fanOut.Strategy,
			Failures:    failures,
		}
	}

	return results, nil
}
