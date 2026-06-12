// Package jobctx provides public access to job context for handlers.
package jobctx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v2/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/storage"
)

// DefaultVersion is the sentinel version used for code paths that existed
// before a version marker was introduced.
const DefaultVersion = -1

const versionCheckpointPrefix = "jobs.version:"

// ErrUnsupportedWorkflowVersion is returned when a recorded workflow-code
// version falls outside the caller's supported range.
var ErrUnsupportedWorkflowVersion = errors.New("jobs: unsupported workflow version")

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
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: -1, // Use -1 to indicate phase checkpoint (not a Call index)
		CallType:  phaseName,
		Result:    resultBytes,
	}

	return jc.SaveCheckpoint(ctx, cp)
}

// SavePhaseCheckpointTx saves a phase result through a caller-owned GORM
// transaction. Unlike SavePhaseCheckpoint, it returns an error outside a job
// handler because silently skipping a transactional checkpoint would break the
// caller's atomicity guarantee.
func SavePhaseCheckpointTx(ctx context.Context, tx *gorm.DB, phaseName string, result any) error {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return fmt.Errorf("jobs.SavePhaseCheckpointTx: not in a job handler")
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

	cs.Mu.Lock()
	cp, ok := cs.Checkpoints[intctx.CheckpointKey{Index: -1, Type: phaseName}]
	cs.Mu.Unlock()

	if !ok {
		return zero, false
	}

	var result T
	if err := json.Unmarshal(cp.Result, &result); err != nil {
		return zero, false
	}
	return result, true
}

func versionCheckpointType(changeID string) string {
	return versionCheckpointPrefix + changeID
}
