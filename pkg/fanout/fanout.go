package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
)

// fanOutSuspender is the optional storage capability that performs a fan-out
// parent's first-execution suspend atomically (fan-out record + checkpoint +
// parent→waiting + children, in one transaction). GormStorage implements it;
// storages that don't are driven through the legacy split-write path.
type fanOutSuspender interface {
	SuspendForFanOut(ctx context.Context, parentID core.UUID, workerID string, fanOut *core.FanOut, checkpoint *core.Checkpoint, subJobs []*core.Job) error
}

// FanOut spawns sub-jobs in parallel and waits for all results.
// Checkpoints progress - safe to retry if parent crashes.
// When sub-jobs are created, the parent job is moved to StatusWaiting.
// Fan-out children inherit the parent job's Tenant and Metadata.
// The worker should detect WaitingError and stop processing.
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
	checkpoint, hasCheckpoint := cs.Checkpoints[intctx.CheckpointKey{Index: callIndex, Type: "fanout"}]
	cs.Mu.Unlock()

	var fanOutID core.UUID

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
		if fanOut != nil && (fanOut.Status == core.FanOutCompleted || fanOut.Status == core.FanOutFailed || fanOut.Status == core.FanOutCancelled) {
			// Fan-out is terminal (completed, failed, or cancelled) — collect
			// results rather than rebuild sub-jobs. CollectResults maps cancelled
			// children to ErrSubJobCancelled.
			return CollectResults[T](ctx, fanOutID)
		}
		if fanOut == nil {
			return nil, fmt.Errorf("fan-out not found: %s", fanOutID)
		}

		jobs, err := buildSubJobs(subJobs, cfg, jc, fanOutID)
		if err != nil {
			return nil, err
		}
		if len(jobs) != fanOut.TotalCount {
			return nil, fmt.Errorf("fan-out %s expected %d sub-jobs, got %d", fanOutID, fanOut.TotalCount, len(jobs))
		}

		persistedSubJobs, err := jc.Storage.GetSubJobs(ctx, fanOutID)
		if err != nil {
			return nil, fmt.Errorf("failed to get sub-jobs: %w", err)
		}
		if len(persistedSubJobs) < fanOut.TotalCount {
			if err := jc.Storage.EnqueueBatch(ctx, jobs); err != nil {
				return nil, fmt.Errorf("failed to enqueue sub-jobs: %w", err)
			}
		}

		// Fan-out not complete, mark waiting again.
		if err := jc.Storage.MarkWaiting(ctx, jc.Job.ID, jc.WorkerID); err != nil {
			return nil, fmt.Errorf("failed to mark job waiting: %w", err)
		}
		return nil, &WaitingError{FanOutID: fanOutID}
	}

	// First execution: create fan-out and sub-jobs
	fanOutID = core.NewID()
	jobs, err := buildSubJobs(subJobs, cfg, jc, fanOutID)
	if err != nil {
		return nil, err
	}

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

	// Build the resume checkpoint (records the fan-out ID for this call index so
	// a replay collects results instead of rebuilding sub-jobs).
	cpData, _ := json.Marshal(core.FanOutCheckpoint{
		FanOutID:  fanOutID,
		CallIndex: callIndex,
	})
	cp := &core.Checkpoint{
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: callIndex,
		CallType:  "fanout",
		Result:    cpData,
	}

	// Preferred path: atomically create the fan-out, write the checkpoint, mark
	// the parent waiting, and enqueue the children in a single transaction. A
	// crash can then never strand a waiting parent with missing children — either
	// all four land (the fan-out proceeds) or none do (the parent stays
	// running+locked and the stale-lock reaper reclaims it). Children become
	// visible only at commit, by which point the parent is already waiting, so the
	// legacy "mark waiting before children are visible" ResumeJob race is moot.
	if suspender, ok := jc.Storage.(fanOutSuspender); ok {
		if err := suspender.SuspendForFanOut(ctx, jc.Job.ID, jc.WorkerID, fanOut, cp, jobs); err != nil {
			return nil, fmt.Errorf("failed to suspend for fan-out: %w", err)
		}
		return nil, &WaitingError{FanOutID: fanOutID}
	}

	// Legacy fallback for storage backends without atomic suspend: the original
	// four separate writes. This retains the documented crash window between
	// MarkWaiting and EnqueueBatch; GormStorage implements the atomic path above.
	if err := jc.Storage.CreateFanOut(ctx, fanOut); err != nil {
		return nil, fmt.Errorf("failed to create fan-out: %w", err)
	}
	if err := jc.SaveCheckpoint(ctx, cp); err != nil {
		return nil, fmt.Errorf("failed to save fan-out checkpoint: %w", err)
	}

	// CRITICAL: Mark parent waiting BEFORE enqueuing sub-jobs.
	// If we enqueue first, a fast worker can complete a sub-job and call
	// ResumeJob before we update the status — ResumeJob finds status=running
	// instead of waiting, and the parent is stuck forever.
	if err := jc.Storage.MarkWaiting(ctx, jc.Job.ID, jc.WorkerID); err != nil {
		return nil, fmt.Errorf("failed to mark job waiting: %w", err)
	}

	// Now enqueue sub-jobs — parent is already in waiting status,
	// so ResumeJob will succeed when children complete.
	if err := jc.Storage.EnqueueBatch(ctx, jobs); err != nil {
		// If enqueue fails after marking waiting, resume the parent so
		// it does not get stuck in waiting with no sub-jobs behind it.
		if _, resumeErr := jc.Storage.ResumeJob(ctx, jc.Job.ID); resumeErr != nil {
			// Log but don't mask the original error
			_ = resumeErr
		}
		return nil, fmt.Errorf("failed to enqueue sub-jobs: %w", err)
	}

	// First-execution cancel-race guard. A CancelJobTerminal(parent) that landed
	// after MarkWaiting but before this enqueue committed would have flipped the
	// fan-out to cancelled while its subtree walk saw zero children — leaving the
	// children we just enqueued to run as orphans against a cancelled fan-out.
	// Re-read the fan-out; if it already reads cancelled, terminally cancel the
	// children we just enqueued.
	//
	// This NARROWS, not closes, the window: this is a separate autocommit read,
	// so a cancel that commits after it (invisible here under READ COMMITTED) can
	// still leave already-dispatched children running — the inherent at-least-once
	// behavior a cancel cannot undo for in-flight work. What IS guaranteed in every
	// interleaving: the persisted fan-out counts reconcile to sum==total (here, or
	// via the operator cancel's own subtree walk, or via natural completion), and
	// the terminally-cancelled parent is never resumed (ResumeJob's CAS matches
	// only waiting/paused).
	if fo, foErr := jc.Storage.GetFanOut(ctx, fanOutID); foErr == nil && fo != nil && fo.Status == core.FanOutCancelled {
		if _, cancelErr := jc.Storage.CancelSubJobs(ctx, fanOutID); cancelErr != nil {
			// Best-effort cleanup: the operator cancel's own subtree reconcile and
			// the natural-completion accounting still converge sum==total.
			_ = cancelErr
		}
	}

	// Signal to worker that this job should not continue
	return nil, &WaitingError{FanOutID: fanOutID}
}

func buildSubJobs(subJobs []SubJob, cfg *config, jc *intctx.JobContext, fanOutID core.UUID) ([]*core.Job, error) {
	jobs := make([]*core.Job, len(subJobs))
	for i, sj := range subJobs {
		if err := security.ValidateJobTypeName(sj.Type); err != nil {
			return nil, fmt.Errorf("invalid sub-job type %q: %w", sj.Type, err)
		}
		if jc.HandlerLookup != nil {
			if _, found := jc.HandlerLookup(sj.Type); !found {
				return nil, fmt.Errorf("no handler registered for sub-job type %q", sj.Type)
			}
		}

		args, err := json.Marshal(sj.Args)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal sub-job args: %w", err)
		}
		if len(args) > security.MaxJobArgsSize {
			return nil, fmt.Errorf("%w: sub-job type %q arguments are %d bytes, limit is %d", core.ErrJobArgsTooLarge, sj.Type, len(args), security.MaxJobArgsSize)
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
		if !sj.PrioritySet {
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

		// Fan-out children inherit the parent's tenant and metadata so tenant
		// scope and queryable context flow consistently through the workflow.
		jobID := core.NewID()
		jobs[i] = &core.Job{
			ID:          jobID,
			Type:        sj.Type,
			Args:        args,
			Queue:       queue,
			Tenant:      jc.Job.Tenant,
			Metadata:    cloneMetadata(jc.Job.Metadata),
			Priority:    priority,
			MaxRetries:  retries,
			Timeout:     sj.Timeout,
			ParentJobID: &parentID,
			RootJobID:   rootID,
			FanOutID:    &fanOutID,
			FanOutIndex: i,
			UniqueKey:   fmt.Sprintf("fanout-%s-%d", fanOutID, i), // Prevent duplicate sub-jobs on replay
		}
	}
	return jobs, nil
}

func cloneMetadata(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// WaitingError signals the worker that the job has moved into
// core.StatusWaiting, pending completion of its fan-out sub-jobs. The
// worker uses this signal to stop processing the current handler without
// treating the outcome as a failure.
type WaitingError struct {
	FanOutID core.UUID
}

func (e *WaitingError) Error() string {
	return fmt.Sprintf("job waiting for fan-out %s", e.FanOutID)
}

// WorkflowWaiting marks this as a self-suspension signal so the worker treats it
// as "stop, do not fail" via core.IsWaiting (shared with signal waits).
func (e *WaitingError) WorkflowWaiting() bool { return true }

// IsWaitingError reports whether err is a waiting-for-fan-out signal
// produced by FanOut.
func IsWaitingError(err error) bool {
	_, ok := err.(*WaitingError)
	return ok
}

// CollectResults gathers results from completed sub-jobs.
// Called when parent job resumes after fan-out.
func CollectResults[T any](ctx context.Context, fanOutID core.UUID) ([]Result[T], error) {
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

	jobs, err := jc.Storage.GetSubJobs(ctx, fanOutID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sub-jobs: %w", err)
	}

	results := make([]Result[T], fanOut.TotalCount)
	for i := range results {
		results[i].Index = i
		results[i].Err = ErrSubJobIncomplete
	}

	for _, job := range jobs {
		if job.FanOutIndex < 0 || job.FanOutIndex >= len(results) {
			continue
		}
		results[job.FanOutIndex].Index = job.FanOutIndex
		switch job.Status {
		case core.StatusCompleted:
			results[job.FanOutIndex].Err = nil
			if job.Result != nil {
				var value T
				if err := json.Unmarshal(job.Result, &value); err != nil {
					results[job.FanOutIndex].Err = fmt.Errorf("failed to unmarshal result: %w", err)
				} else {
					results[job.FanOutIndex].Value = value
				}
			}
		case core.StatusFailed:
			results[job.FanOutIndex].Err = fmt.Errorf("%s", job.LastError)
		case core.StatusCancelled:
			results[job.FanOutIndex].Err = ErrSubJobCancelled
		default:
			results[job.FanOutIndex].Err = ErrSubJobIncomplete
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
		return results, core.NoRetry(&Error{
			FanOutID:    fanOutID,
			TotalCount:  fanOut.TotalCount,
			FailedCount: fanOut.FailedCount,
			Strategy:    fanOut.Strategy,
			Failures:    failures,
		})
	}

	return results, nil
}
