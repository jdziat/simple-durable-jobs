package call

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v2/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/security"
)

// Call executes a durable nested job call.
//
// Call assigns a monotonic call index and checkpoints by both index and name.
// It must be invoked from one goroutine in a deterministic order across
// replays; concurrent Calls in one handler can assign indexes nondeterministically.
// On replay, a checkpoint matches only when the call index and name both match.
// Keep the sequence and names of Calls stable, or replay fails with a
// determinism violation unless BestEffortReplay is enabled, in which case the
// mismatch is logged and the call is re-executed.
//
// A nested Call result is checkpointed once, but handler execution is
// at-least-once: a crash after the nested handler runs and before SaveCheckpoint
// succeeds will run the handler again on replay. Called handlers with side
// effects must be idempotent.
//
// Error-only handlers may be called only as Call[any] or Call[struct{}].
// Requesting a concrete result type from an error-only handler returns a
// non-retryable error.
func Call[T any](ctx context.Context, name string, args any) (T, error) {
	return CallWithCheckpointCtx[T](ctx, ctx, name, args)
}

// CallWithCheckpointCtx is like Call but accepts separate contexts for
// handler execution and checkpoint persistence.
//
// execCtx governs the nested handler call — cancellation and deadline from
// execCtx propagate to the activity. checkpointCtx is used only for the
// SaveCheckpoint write after the handler returns. Separating the two allows
// callers to apply a per-activity deadline (via execCtx) while ensuring the
// checkpoint write always uses a long-lived context (checkpointCtx = the
// workflow's root context) so a deadline hit at the end of a long activity
// does not cause a successful result to be lost.
//
// When execCtx == checkpointCtx the behaviour is identical to Call.
func CallWithCheckpointCtx[T any](execCtx, checkpointCtx context.Context, name string, args any) (T, error) {
	var zero T

	// Retrieve job context and call state from execCtx (the execution context
	// carries the workflow state injected by the worker).
	jc := intctx.GetJobContext(execCtx)
	if jc == nil {
		return zero, fmt.Errorf("jobs.Call must be used within a job handler")
	}

	cs := intctx.GetCallState(execCtx)
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
			return zero, core.RehydrateCheckpointErrorWithCause(
				checkpoint.Error,
				checkpoint.ErrorCause,
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

	// Execute the nested call using execCtx so deadline/cancellation applies to
	// the handler, not to the checkpoint write.
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

	result, err := handler.ExecuteCall[T](execCtx, hnd, args)

	// Save checkpoint using checkpointCtx. When checkpointCtx != execCtx the
	// caller has provided a longer-lived context (e.g. the workflow root) so
	// the write succeeds even if execCtx's deadline fired just after the handler
	// returned.
	cp := &core.Checkpoint{
		ID:        uuid.New().String(),
		JobID:     jc.Job.ID,
		CallIndex: callIndex,
		CallType:  name,
	}

	if err != nil {
		message, cause, kind, delay := core.CheckpointErrorFields(err)
		cp.Error = message
		cp.ErrorCause = cause
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

	if saveErr := jc.SaveCheckpoint(checkpointCtx, cp); saveErr != nil {
		return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
	}

	return result, err
}
