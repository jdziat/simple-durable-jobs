// Package call provides the durable Call function for nested job calls.
package call

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/internal/handler"
)

// Call executes a durable nested job call.
// Results are checkpointed; on replay, cached results are returned without re-execution.
func Call[T any](ctx context.Context, name string, args any) (T, error) {
	var zero T

	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return zero, fmt.Errorf("jobs.Call must be used within a job handler")
	}

	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return zero, fmt.Errorf("jobs.Call: call state not initialized")
	}

	cs.Mu.Lock()
	callIndex := cs.CallIndex
	cs.CallIndex++
	checkpoint, hasCheckpoint := cs.Checkpoints[callIndex]
	cs.Mu.Unlock()

	// If we have a checkpoint, return the cached result
	if hasCheckpoint {
		if checkpoint.Error != "" {
			return zero, fmt.Errorf("%s", checkpoint.Error)
		}
		var result T
		if err := json.Unmarshal(checkpoint.Result, &result); err != nil {
			return zero, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		return result, nil
	}

	// Execute the nested call
	h, ok := jc.HandlerLookup(name)
	if !ok {
		return zero, fmt.Errorf("no handler registered for %q", name)
	}

	hnd, ok := h.(*handler.Handler)
	if !ok {
		return zero, fmt.Errorf("invalid handler type for %q", name)
	}

	result, err := handler.ExecuteCall[T](ctx, hnd, args)

	// Save checkpoint
	cp := &core.Checkpoint{
		ID:        uuid.New().String(),
		JobID:     jc.Job.ID,
		CallIndex: callIndex,
		CallType:  name,
	}

	if err != nil {
		cp.Error = err.Error()
	} else {
		resultBytes, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return zero, fmt.Errorf("failed to marshal result: %w", marshalErr)
		}
		cp.Result = resultBytes
	}

	if saveErr := jc.SaveCheckpoint(ctx, cp); saveErr != nil {
		return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
	}

	return result, err
}
