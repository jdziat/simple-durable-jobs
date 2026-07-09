package call

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
)

// checkpointIndexCleaner is an optional storage capability used to clear an
// orphaned prior-type checkpoint at one call index during a BestEffortReplay
// type-mismatch re-execution. Storage backends that do not implement it simply
// leave the (benign, GC'd-with-the-job) orphan in place.
type checkpointIndexCleaner interface {
	DeleteCheckpointAtIndex(ctx context.Context, jobID core.UUID, callIndex int) error
}

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
// Retry semantics: a successful result and a terminal (NoRetry) error are
// checkpointed and replayed verbatim without re-invoking the handler. A
// RETRYABLE error — a plain error or a RetryAfterError — is NOT checkpointed;
// it propagates and the nested handler re-executes when the OUTER job retries.
// Nested-Call retries are therefore governed by the outer job's MaxRetries;
// there is no separate per-Call retry count (jobs.DefaultCallRetries is not
// wired to any behavior). A handler that catches a retryable nested-Call error
// and branches on it will re-run that Call on any later resume, so keep such
// handlers deterministic and idempotent. A checkpoint matches by call index and
// name only (not args): changing which Calls run, or their names/order, across a
// replay fails loud with a determinism violation, but reissuing the SAME-named
// Call at the same index with DIFFERENT args silently returns the prior cached
// result — so branch the sequence/name of Calls, never just their args.
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
		// The new call_type will be saved as a fresh row at this index, leaving the
		// mismatched prior-type row orphaned (the unique key includes call_type).
		// Clear it so the index holds exactly one checkpoint. Cleanup is best-effort:
		// the orphan is never read (the correct-type row shadows it on lookup) and is
		// GC'd with the job, so a delete failure must not abort a working replay.
		if cleaner, ok := jc.Storage.(checkpointIndexCleaner); ok {
			if delErr := cleaner.DeleteCheckpointAtIndex(checkpointCtx, jc.Job.ID, callIndex); delErr != nil {
				if jc.Logger != nil {
					jc.Logger.Warn("jobs.Call best-effort replay: failed to clear orphaned checkpoint",
						"index", callIndex, "job_id", jc.Job.ID, "error", delErr)
				}
			} else {
				// Mirror the SQL delete (which clears every row at this index): drop
				// all in-memory entries at callIndex so storage and the call state
				// stay symmetric, not just the single mismatched type.
				cs.Mu.Lock()
				for k := range cs.Checkpoints {
					if k.Index == callIndex {
						delete(cs.Checkpoints, k)
					}
				}
				cs.Mu.Unlock()
			}
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
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: callIndex,
		CallType:  name,
	}

	if err != nil {
		// Only a TERMINAL (NoRetry) nested-Call error is checkpointed. A retryable
		// error — a plain error or a RetryAfterError, both of which cause the outer
		// job to retry — must NOT be frozen as a checkpoint: a checkpointed error is
		// replayed verbatim on every outer-job retry WITHOUT re-invoking the nested
		// handler (see the replay branch above), so freezing a transient failure
		// would make the nested Call retry zero times and dead-letter the workflow
		// on a stale error. Propagating it un-checkpointed lets the nested handler
		// re-execute when the outer job retries; nested-Call retries are therefore
		// governed by the outer job's MaxRetries. A self-suspension (waiting) error
		// is likewise not a terminal outcome, so it too is left un-checkpointed.
		var noRetry *core.NoRetryError
		if !errors.As(err, &noRetry) {
			return zero, err
		}
		message, cause, kind, delay := core.CheckpointErrorFields(err)
		cp.Error = message
		cp.ErrorCause = cause
		cp.ErrorKind = kind
		cp.ErrorDelayNanos = int64(delay)
		if saveErr := jc.SaveCheckpoint(checkpointCtx, cp); saveErr != nil {
			return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
		}
		return zero, err
	}

	resultBytes, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return zero, fmt.Errorf("failed to marshal result: %w", marshalErr)
	}
	if len(resultBytes) > security.MaxResultSize {
		return zero, core.NoRetry(fmt.Errorf("jobs: call %q result is %d bytes, limit is %d", name, len(resultBytes), security.MaxResultSize))
	}
	cp.Result = resultBytes
	if saveErr := jc.SaveCheckpoint(checkpointCtx, cp); saveErr != nil {
		return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
	}

	return result, nil
}
