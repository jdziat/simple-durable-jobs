// Package call provides the durable Call function for nested job calls.
package call

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
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
	key := intctx.CheckpointKey{Index: callIndex, Type: name}
	checkpoint, hasCheckpoint := cs.Checkpoints[key]
	var mismatched *core.Checkpoint
	if !hasCheckpoint {
		for cpKey, cp := range cs.Checkpoints {
			if cpKey.Index == callIndex && cpKey.Type != name {
				mismatched = cp
				break
			}
		}
	}
	cs.Mu.Unlock()

	// If we have a checkpoint, return the cached result
	if hasCheckpoint {
		if checkpoint.Error != "" {
			return zero, core.RehydrateCheckpointError(
				checkpoint.Error,
				checkpoint.ErrorKind,
				time.Duration(checkpoint.ErrorDelayNanos),
			)
		}
		var result T
		if err := json.Unmarshal(checkpoint.Result, &result); err != nil {
			return zero, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		return result, nil
	}
	if mismatched != nil {
		if !jc.BestEffortReplay {
			return zero, fmt.Errorf("jobs.Call determinism violation at index %d: checkpoint type %q does not match requested call %q", callIndex, mismatched.CallType, name)
		}
		if jc.Logger != nil {
			jc.Logger.Warn("jobs.Call best-effort replay: checkpoint type mismatch, re-executing",
				"index", callIndex, "checkpoint_type", mismatched.CallType, "requested", name, "job_id", jc.Job.ID)
		}
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

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return zero, fmt.Errorf("failed to marshal args: %w", err)
	}
	if len(argsBytes) > security.MaxJobArgsSize {
		return zero, fmt.Errorf("%w: call %q arguments are %d bytes, limit is %d", core.ErrJobArgsTooLarge, name, len(argsBytes), security.MaxJobArgsSize)
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
		kind, delay := core.CheckpointErrorKind(err)
		cp.ErrorKind = kind
		cp.ErrorDelayNanos = int64(delay)
	} else {
		resultBytes, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return zero, fmt.Errorf("failed to marshal result: %w", marshalErr)
		}
		if len(resultBytes) > security.MaxResultSize {
			return zero, core.NoRetry(fmt.Errorf("jobs: call %q result is %d bytes, limit is %d", name, len(resultBytes), security.MaxResultSize))
		}
		cp.Result = resultBytes
	}

	if saveErr := jc.SaveCheckpoint(ctx, cp); saveErr != nil {
		return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
	}

	return result, err
}
