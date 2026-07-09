// Package jobctx provides public access to job context for handlers.
package jobctx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

// DefaultVersion is the sentinel version used for code paths that existed
// before a version marker was introduced.
const DefaultVersion = -1

const versionCheckpointPrefix = "jobs.version:"

// ErrUnsupportedWorkflowVersion is returned when a recorded workflow-code
// version falls outside the caller's supported range.
var ErrUnsupportedWorkflowVersion = errors.New("jobs: unsupported workflow version")

// ErrReservedPhaseName is returned by the phase-checkpoint APIs when the phase
// name uses the reserved "jobs.version:" prefix. That prefix is owned by
// GetVersion's workflow-version markers (stored at the same CallIndex -1), so a
// phase sharing it would collide with and clobber a version marker.
var ErrReservedPhaseName = fmt.Errorf("jobs: phase name uses the reserved %q prefix", versionCheckpointPrefix)

// JobFromContext returns the current Job from context, or nil if not in a job handler.
// Use this to get the job ID for logging or progress tracking.
func JobFromContext(ctx context.Context) *core.Job {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil
	}
	return jc.Job
}

// JobIDFromContext returns the current job ID from context, or empty UUID if not in a job handler.
func JobIDFromContext(ctx context.Context) core.UUID {
	job := JobFromContext(ctx)
	if job == nil {
		return core.NilUUID
	}
	return job.ID
}

// GetVersion records or replays a workflow-code version marker for changeID.
//
// On first execution it saves maxSupported as a named checkpoint and returns it.
// On replay it returns the previously recorded version, even if maxSupported has
// since increased. If the recorded version is outside [minSupported,
// maxSupported], ErrUnsupportedWorkflowVersion is returned.
//
// Version markers are stored at CallIndex -1 with an internal CallType prefix,
// so they do not collide with phase checkpoints and are ignored by Strict
// determinism's unconsumed-Call check.
func GetVersion(ctx context.Context, changeID string, minSupported, maxSupported int) (int, error) {
	if minSupported > maxSupported {
		return 0, fmt.Errorf("%w: change %q supports [%d, %d]", ErrUnsupportedWorkflowVersion, changeID, minSupported, maxSupported)
	}

	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return maxSupported, nil
	}

	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return 0, fmt.Errorf("jobs.GetVersion: call state not initialized")
	}

	key := intctx.CheckpointKey{Index: -1, Type: versionCheckpointType(changeID)}
	cs.Mu.Lock()
	cp, ok := cs.Checkpoints[key]
	cs.Mu.Unlock()

	if ok {
		var recorded int
		if err := json.Unmarshal(cp.Result, &recorded); err != nil {
			return 0, fmt.Errorf("jobs.GetVersion: unmarshal version marker %q: %w", changeID, err)
		}
		if recorded < minSupported || recorded > maxSupported {
			return 0, fmt.Errorf("%w: change %q recorded version %d outside supported range [%d, %d]", ErrUnsupportedWorkflowVersion, changeID, recorded, minSupported, maxSupported)
		}
		return recorded, nil
	}

	resultBytes, err := json.Marshal(maxSupported)
	if err != nil {
		return 0, fmt.Errorf("marshal workflow version: %w", err)
	}

	cp = &core.Checkpoint{
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: -1,
		CallType:  key.Type,
		Result:    resultBytes,
	}
	if err := jc.SaveCheckpoint(ctx, cp); err != nil {
		return 0, err
	}

	cs.Mu.Lock()
	cs.Checkpoints[key] = cp
	cs.Mu.Unlock()

	return maxSupported, nil
}

// SavePhaseCheckpoint saves a phase result to the checkpoint store.
// The phase name is used as the CallType for lookup on resume.
// Returns nil if not running within a job handler. Returns ErrReservedPhaseName
// if phaseName uses the reserved "jobs.version:" prefix.
//
// On success the result is also reflected in the in-memory call state, so a
// LoadPhaseCheckpoint later in the SAME run returns it (mirroring GetVersion).
func SavePhaseCheckpoint(ctx context.Context, phaseName string, result any) error {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil // Not in a job context, silently skip
	}
	if strings.HasPrefix(phaseName, versionCheckpointPrefix) {
		return fmt.Errorf("%w: %q", ErrReservedPhaseName, phaseName)
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal phase result: %w", err)
	}

	cp := &core.Checkpoint{
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: -1, // Use -1 to indicate phase checkpoint (not a Call index)
		CallType:  phaseName,
		Result:    resultBytes,
	}

	if err := jc.SaveCheckpoint(ctx, cp); err != nil {
		return err
	}

	// Reflect the just-saved checkpoint in the in-memory call state so a same-run
	// LoadPhaseCheckpoint returns it instead of (zero,false). GetVersion does the
	// same write-back for its version markers.
	if cs := intctx.GetCallState(ctx); cs != nil {
		cs.Mu.Lock()
		cs.Checkpoints[intctx.CheckpointKey{Index: -1, Type: phaseName}] = cp
		cs.Mu.Unlock()
	}
	return nil
}

// SavePhaseCheckpointTx saves a phase result through a caller-owned GORM
// transaction. Unlike SavePhaseCheckpoint, it returns an error outside a job
// handler because silently skipping a transactional checkpoint would break the
// caller's atomicity guarantee. Returns ErrReservedPhaseName if phaseName uses
// the reserved "jobs.version:" prefix.
//
// Unlike SavePhaseCheckpoint, it does NOT update the in-memory call state: the
// write is bound to the caller's transaction, which may not be committed yet (or
// may roll back), so caching it as visible would be unsound. A same-run
// LoadPhaseCheckpoint therefore will not observe it; the value is read back on
// the next replay after the caller commits.
func SavePhaseCheckpointTx(ctx context.Context, tx *gorm.DB, phaseName string, result any) error {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return fmt.Errorf("jobs.SavePhaseCheckpointTx: not in a job handler")
	}
	if strings.HasPrefix(phaseName, versionCheckpointPrefix) {
		return fmt.Errorf("%w: %q", ErrReservedPhaseName, phaseName)
	}

	txCheckpointer, ok := jc.Storage.(storage.TxCheckpointer)
	if !ok {
		return core.ErrStorageNoTxCheckpoint
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal phase result: %w", err)
	}

	cp := &core.Checkpoint{
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: -1, // Use -1 to indicate phase checkpoint (not a Call index)
		CallType:  phaseName,
		Result:    resultBytes,
	}

	return txCheckpointer.SaveCheckpointTx(ctx, tx, cp)
}

// ErrPhaseCheckpointDecode is returned by LoadPhaseCheckpointErr when a phase
// checkpoint EXISTS but its stored result cannot be decoded into T — a corruption
// or a T-type mismatch, NOT a legitimate absence.
var ErrPhaseCheckpointDecode = errors.New("jobs: phase checkpoint decode failed")

// LoadPhaseCheckpoint loads a previously saved phase result from the checkpoint store.
// Returns (result, true) if found, (zero, false) if not found or not in job context.
//
// NOTE: an undecodable checkpoint (present but corrupt / wrong T) is reported as
// (zero, false) here — i.e. treated as absent, so the phase RE-RUNS. Use
// LoadPhaseCheckpointErr to distinguish a genuine absence from a decode failure
// and fail loud instead of silently re-executing.
func LoadPhaseCheckpoint[T any](ctx context.Context, phaseName string) (T, bool) {
	result, ok, _ := LoadPhaseCheckpointErr[T](ctx, phaseName)
	return result, ok
}

// LoadPhaseCheckpointErr is like LoadPhaseCheckpoint but surfaces a decode
// failure. Three outcomes:
//   - (result, true, nil):  checkpoint found and decoded — skip the phase.
//   - (zero, false, nil):   legitimately absent (no context, not written) — run the phase.
//   - (zero, false, err):   checkpoint EXISTS but did not decode into T (err wraps
//     ErrPhaseCheckpointDecode) — do NOT treat as absent; fail loud rather than
//     silently re-execute a phase whose result was actually persisted.
func LoadPhaseCheckpointErr[T any](ctx context.Context, phaseName string) (T, bool, error) {
	var zero T

	// A reserved-prefix name can never have been written by SavePhaseCheckpoint,
	// so treat it as absent rather than aliasing a GetVersion marker.
	if strings.HasPrefix(phaseName, versionCheckpointPrefix) {
		return zero, false, nil
	}

	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return zero, false, nil
	}

	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return zero, false, nil
	}

	cs.Mu.Lock()
	cp, ok := cs.Checkpoints[intctx.CheckpointKey{Index: -1, Type: phaseName}]
	cs.Mu.Unlock()

	if !ok {
		return zero, false, nil
	}

	var result T
	if err := json.Unmarshal(cp.Result, &result); err != nil {
		return zero, false, fmt.Errorf("%w: phase %q: %v", ErrPhaseCheckpointDecode, phaseName, err)
	}
	return result, true, nil
}

func versionCheckpointType(changeID string) string {
	return versionCheckpointPrefix + changeID
}
