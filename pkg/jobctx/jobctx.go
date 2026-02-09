// Package jobctx provides public access to job context for handlers.
package jobctx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
)

// JobFromContext returns the current Job from context, or nil if not in a job handler.
// Use this to get the job ID for logging or progress tracking.
func JobFromContext(ctx context.Context) *core.Job {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil
	}
	return jc.Job
}

// JobIDFromContext returns the current job ID from context, or empty string if not in a job handler.
func JobIDFromContext(ctx context.Context) string {
	job := JobFromContext(ctx)
	if job == nil {
		return ""
	}
	return job.ID
}

// SavePhaseCheckpoint saves a phase result to the checkpoint store.
// The phase name is used as the CallType for lookup on resume.
// Returns nil if not running within a job handler.
func SavePhaseCheckpoint(ctx context.Context, phaseName string, result any) error {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil // Not in a job context, silently skip
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal phase result: %w", err)
	}

	cp := &core.Checkpoint{
		ID:        uuid.New().String(),
		JobID:     jc.Job.ID,
		CallIndex: -1, // Use -1 to indicate phase checkpoint (not a Call index)
		CallType:  phaseName,
		Result:    resultBytes,
	}

	return jc.SaveCheckpoint(ctx, cp)
}

// LoadPhaseCheckpoint loads a previously saved phase result from the checkpoint store.
// Returns (result, true) if found, (zero, false) if not found or not in job context.
func LoadPhaseCheckpoint[T any](ctx context.Context, phaseName string) (T, bool) {
	var zero T

	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return zero, false
	}

	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return zero, false
	}

	// Search checkpoints for matching phase name
	cs.Mu.Lock()
	defer cs.Mu.Unlock()

	for _, cp := range cs.Checkpoints {
		if cp.CallType == phaseName && cp.CallIndex == -1 {
			var result T
			if err := json.Unmarshal(cp.Result, &result); err != nil {
				return zero, false
			}
			return result, true
		}
	}

	return zero, false
}
