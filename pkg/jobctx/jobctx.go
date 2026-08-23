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

// ErrDuplicatePhaseName is returned by the phase-checkpoint APIs when a run
// saves a SECOND phase under a name it already saved. The name IS the
// checkpoint's identity (CallIndex -1 plus the name), so the second save would
// overwrite the first and leave two distinct phases sharing one record — after
// which a replay skips both, including one whose body never ran. Calls cannot
// collide this way: each carries its own ascending CallIndex.
//
// Reusing a name across RUNS is replay and stays allowed. The returned error is
// wrapped NoRetry: see duplicatePhaseNameError.
var ErrDuplicatePhaseName = errors.New("jobs: phase name already saved in this run")

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
// A run that was ALREADY IN FLIGHT when the marker was deployed has no marker to
// replay, because the code that produced its checkpoints never called
// GetVersion. Such a run is pinned to DefaultVersion rather than handed
// maxSupported: it is detected by the durable evidence it carries — an indexed
// checkpoint (Call, fan-out, signal wait, sleep) recorded by an earlier
// execution at or beyond the call cursor GetVersion is standing on, which only a
// run that already executed past this point can have. DefaultVersion is then
// recorded like any other, so every later replay of that run reads it back from
// the marker. This is what lets an in-flight run keep its originally recorded
// path; handing it maxSupported instead makes it issue the NEW branch's Call at
// an index whose checkpoint holds the OLD call's type, which is a determinism
// violation on every attempt until the job dead-letters.
//
// If DefaultVersion falls outside [minSupported, maxSupported] — the second
// deploy, which drops the old branch by raising minSupported — an in-flight run
// gets ErrUnsupportedWorkflowVersion and nothing is recorded, rather than
// silently taking a branch its checkpoints cannot support.
//
// The detection is evidence-based and one-sided: a run that passed this point
// without recording ANY indexed durable step at or after it is indistinguishable
// from a first execution and receives maxSupported. That is harmless — there is
// no recorded step at those indices for the new branch to collide with. It is
// also why the marker belongs BEFORE the durable operations it guards, not after
// them.
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

	// No marker. Either this is genuinely the first execution to reach this
	// point (record maxSupported), or an earlier execution already ran past it
	// under code that had no marker to record (pin to DefaultVersion). See the
	// godoc for why HasUnreachedCallCheckpoints separates the two soundly.
	record := maxSupported
	if cs.HasUnreachedCallCheckpoints() {
		record = DefaultVersion
		if record < minSupported || record > maxSupported {
			return 0, fmt.Errorf("%w: change %q is reached by a run whose durable steps predate the marker, which pins it to version %d, outside supported range [%d, %d]",
				ErrUnsupportedWorkflowVersion, changeID, record, minSupported, maxSupported)
		}
	}

	resultBytes, err := json.Marshal(record)
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

	return record, nil
}

// SavePhaseCheckpoint saves a phase result to the checkpoint store.
// The phase name is used as the CallType for lookup on resume.
// Returns nil if not running within a job handler. Returns ErrReservedPhaseName
// if phaseName uses the reserved "jobs.version:" prefix, or
// ErrDuplicatePhaseName if this run already saved a phase under that name.
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

	// Refuse a name this run already used rather than upsert over the earlier
	// phase, the way Schedule refuses a duplicate schedule name.
	cs := intctx.GetCallState(ctx)
	if cs != nil && !cs.ReservePhaseName(phaseName) {
		return duplicatePhaseNameError(phaseName)
	}

	if err := jc.SaveCheckpoint(ctx, cp); err != nil {
		if cs != nil {
			cs.ReleasePhaseName(phaseName)
		}
		return err
	}

	// Reflect the just-saved checkpoint in the in-memory call state so a same-run
	// LoadPhaseCheckpoint returns it instead of (zero,false). GetVersion does the
	// same write-back for its version markers.
	if cs != nil {
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
// the reserved "jobs.version:" prefix, or ErrDuplicatePhaseName if this run
// already saved a phase under that name.
//
// The duplicate-name claim is taken when the row is written, which a later
// ROLLBACK cannot take back — the rollback happens in the caller's transaction,
// invisible here. Retrying the phase after a rollback therefore has to go
// through a replay: return the error and let the job run again, which is also
// what makes the retry see the rolled-back state.
//
// Unlike SavePhaseCheckpoint, it does NOT update the in-memory call state: the
// write is bound to the caller's transaction, which may not be committed yet (or
// may roll back), so caching it as visible would be unsound. A same-run
// LoadPhaseCheckpoint therefore will not observe it; the value is read back on
// the next replay after the caller commits.
//
// GormStorage additionally ownership-fences this write inside the supplied
// transaction when the JobContext carries a WorkerID. A stale handler therefore
// receives core.ErrJobNotOwned and its business effect rolls back with the
// checkpoint. Custom TxCheckpointer implementations without the additive
// OwnedTxCheckpointer capability retain the v4 compatibility behaviour.
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

	cs := intctx.GetCallState(ctx)
	if cs != nil && !cs.ReservePhaseName(phaseName) {
		return duplicatePhaseNameError(phaseName)
	}

	var saveErr error
	if owned, ok := jc.Storage.(storage.OwnedTxCheckpointer); ok && jc.WorkerID != "" {
		saveErr = owned.SaveCheckpointTxOwned(ctx, tx, cp, jc.WorkerID)
	} else {
		saveErr = txCheckpointer.SaveCheckpointTx(ctx, tx, cp)
	}
	if saveErr != nil {
		if cs != nil {
			cs.ReleasePhaseName(phaseName)
		}
		return saveErr
	}
	return nil
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

// duplicatePhaseNameError reports the duplicate as terminal. Retrying cannot
// help — the name is wrong in the code — and it actively hides the defect: the
// retry replays the first phase's checkpoint for BOTH phases, skips them both
// and completes, which is the silent outcome this refusal exists to prevent.
// Strict determinism's violation reports the same way.
func duplicatePhaseNameError(phaseName string) error {
	return core.NoRetry(fmt.Errorf("%w: %q", ErrDuplicatePhaseName, phaseName))
}

func versionCheckpointType(changeID string) string {
	return versionCheckpointPrefix + changeID
}
