package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v3/pkg/internal/context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errStorage embeds *minimalStorage and lets a test inject errors (or nil
// overrides) into selected storage methods, so we can drive FanOut and
// CollectResults down their error-handling branches without touching the
// shared mock in fanout_unit_test.go.
type errStorage struct {
	*minimalStorage

	getFanOutErr    error
	getFanOutNil    bool // force GetFanOut to return (nil, nil)
	getSubJobsErr   error
	suspendErr      error
	createFanOutErr error
	saveCheckpoint  func(ctx context.Context, cp *core.Checkpoint) error
	resumeErr       error
}

func newErrStorage() *errStorage {
	return &errStorage{minimalStorage: newMinimalStorage()}
}

func (s *errStorage) GetFanOut(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	if s.getFanOutErr != nil {
		return nil, s.getFanOutErr
	}
	if s.getFanOutNil {
		return nil, nil
	}
	return s.minimalStorage.GetFanOut(ctx, fanOutID)
}

func (s *errStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	if s.getSubJobsErr != nil {
		return nil, s.getSubJobsErr
	}
	return s.minimalStorage.GetSubJobs(ctx, fanOutID)
}

func (s *errStorage) MarkWaiting(ctx context.Context, jobID, workerID string) error {
	if s.suspendErr != nil {
		return s.suspendErr
	}
	return s.minimalStorage.MarkWaiting(ctx, jobID, workerID)
}

func (s *errStorage) CreateFanOut(ctx context.Context, fo *core.FanOut) error {
	if s.createFanOutErr != nil {
		return s.createFanOutErr
	}
	return s.minimalStorage.CreateFanOut(ctx, fo)
}

func (s *errStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	if s.resumeErr != nil {
		return false, s.resumeErr
	}
	return s.minimalStorage.ResumeJob(ctx, jobID)
}

// makeErrJobCtx mirrors makeJobCtx but wires the parent job + SaveCheckpoint
// against an errStorage so its overridable methods are reachable. An optional
// saveCheckpoint override lets a test force a checkpoint-save failure.
func makeErrJobCtx(store *errStorage, parentJobID, queueName string) *intctx.JobContext {
	job := &core.Job{ID: parentJobID, Queue: queueName}
	store.jobs[parentJobID] = job

	return &intctx.JobContext{
		Job:      job,
		Storage:  store,
		WorkerID: "test-worker",
		HandlerLookup: func(name string) (any, bool) {
			return nil, true // all handlers "exist"
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			if store.saveCheckpoint != nil {
				return store.saveCheckpoint(ctx, cp)
			}
			return store.SaveCheckpoint(ctx, cp)
		},
	}
}

// buildCtxErr builds an execution context for an errStorage-backed job, mirroring
// buildCtx from fanout_unit_test.go.
func buildCtxErr(jc *intctx.JobContext, checkpoints []core.Checkpoint) context.Context {
	ctx := context.Background()
	ctx = intctx.WithJobContext(ctx, jc)
	ctx = intctx.WithCallState(ctx, checkpoints)
	return ctx
}

// ---------------------------------------------------------------------------
// FanOut() — resume-path error branches
// ---------------------------------------------------------------------------

func resumeCheckpoint(parentID, fanOutID string) core.Checkpoint {
	cpData, _ := json.Marshal(core.FanOutCheckpoint{FanOutID: fanOutID, CallIndex: 0})
	return core.Checkpoint{
		ID:        "cp-resume",
		JobID:     parentID,
		CallIndex: 0,
		CallType:  "fanout",
		Result:    cpData,
	}
}

func TestGen_FanOut_Resume_GetFanOutError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-getfo-err"
	jc := makeErrJobCtx(store, parentID, "default")
	store.getFanOutErr = errors.New("boom getfanout")

	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, "fo-x")})
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get fan-out")
}

func TestGen_FanOut_Resume_FanOutNil(t *testing.T) {
	store := newErrStorage()
	parentID := "p-fo-nil"
	jc := makeErrJobCtx(store, parentID, "default")
	store.getFanOutNil = true // checkpoint exists but no fan-out record

	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, "fo-missing")})
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fan-out not found")
}

func TestGen_FanOut_Resume_BuildSubJobsError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-build-err"
	jc := makeErrJobCtx(store, parentID, "default")

	fanOutID := "fo-build"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutPending,
	}

	// An invalid sub-job type makes buildSubJobs fail inside the resume path.
	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, fanOutID)})
	_, err := FanOut[string](ctx, []SubJob{{Type: "123-invalid", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sub-job type")
}

func TestGen_FanOut_Resume_TotalCountMismatch(t *testing.T) {
	store := newErrStorage()
	parentID := "p-count-mismatch"
	jc := makeErrJobCtx(store, parentID, "default")

	fanOutID := "fo-mismatch"
	// Stored TotalCount is 5, but we pass a single sub-job on replay.
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  5,
		Status:      core.FanOutPending,
	}

	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, fanOutID)})
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 5 sub-jobs, got 1")
}

func TestGen_FanOut_Resume_GetSubJobsError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-getsubjobs-err"
	jc := makeErrJobCtx(store, parentID, "default")

	fanOutID := "fo-getsubs"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutPending,
	}
	store.getSubJobsErr = errors.New("boom getsubjobs")

	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, fanOutID)})
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get sub-jobs")
}

func TestGen_FanOut_Resume_EnqueueBatchError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-resume-enqueue-err"
	jc := makeErrJobCtx(store, parentID, "default")

	fanOutID := "fo-resume-enq"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutPending,
	}
	// No persisted sub-jobs exist (len < TotalCount), so the resume path
	// tries to EnqueueBatch and we force that to fail.
	store.enqueueBatchErr = errors.New("boom enqueue")

	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, fanOutID)})
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue sub-jobs")
}

func TestGen_FanOut_Resume_MarkWaitingError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-resume-suspend-err"
	jc := makeErrJobCtx(store, parentID, "default")

	fanOutID := "fo-resume-suspend"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutPending,
	}
	// Pre-populate the persisted sub-job so the EnqueueBatch branch is skipped
	// and we reach MarkWaiting directly.
	store.jobs["sub-existing"] = &core.Job{
		ID:          "sub-existing",
		FanOutID:    &fanOutID,
		FanOutIndex: 0,
		Status:      core.StatusRunning,
	}
	store.suspendErr = errors.New("boom suspend")

	ctx := buildCtxErr(jc, []core.Checkpoint{resumeCheckpoint(parentID, fanOutID)})
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to mark job waiting")
}

// ---------------------------------------------------------------------------
// FanOut() — first-execution error branches
// ---------------------------------------------------------------------------

func TestGen_FanOut_First_CreateFanOutError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-create-fo-err"
	jc := makeErrJobCtx(store, parentID, "default")
	store.createFanOutErr = errors.New("boom createfanout")

	ctx := buildCtxErr(jc, nil)
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create fan-out")
}

func TestGen_FanOut_First_SaveCheckpointError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-save-cp-err"
	jc := makeErrJobCtx(store, parentID, "default")
	store.saveCheckpoint = func(ctx context.Context, cp *core.Checkpoint) error {
		return errors.New("boom savecheckpoint")
	}

	ctx := buildCtxErr(jc, nil)
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save fan-out checkpoint")
}

func TestGen_FanOut_First_MarkWaitingError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-first-suspend-err"
	jc := makeErrJobCtx(store, parentID, "default")
	store.suspendErr = errors.New("boom suspend first")

	ctx := buildCtxErr(jc, nil)
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to mark job waiting")
}

func TestGen_FanOut_First_EnqueueFails_ResumeAlsoErrors(t *testing.T) {
	// Covers the branch where, after a failed EnqueueBatch in first execution,
	// the recovery ResumeJob call itself returns an error (the error is
	// swallowed; the original enqueue error is still surfaced).
	store := newErrStorage()
	parentID := "p-first-enqueue-resume-err"
	jc := makeErrJobCtx(store, parentID, "default")
	store.enqueueBatchErr = errors.New("boom enqueue first")
	store.resumeErr = errors.New("boom resume recovery")

	ctx := buildCtxErr(jc, nil)
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: "x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue sub-jobs")
}

// ---------------------------------------------------------------------------
// buildSubJobs() — args marshal failure
// ---------------------------------------------------------------------------

func TestGen_FanOut_UnmarshalableArgs_ReturnsMarshalError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-bad-args"
	jc := makeErrJobCtx(store, parentID, "default")
	ctx := buildCtxErr(jc, nil)

	// A channel cannot be JSON-marshaled, forcing the json.Marshal error path
	// inside buildSubJobs.
	_, err := FanOut[string](ctx, []SubJob{{Type: "do-work", Args: make(chan int)}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal sub-job args")
}

// ---------------------------------------------------------------------------
// CollectResults() — error branches
// ---------------------------------------------------------------------------

func TestGen_CollectResults_GetFanOutError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-cr-getfo-err"
	jc := makeErrJobCtx(store, parentID, "default")
	store.getFanOutErr = errors.New("boom cr getfanout")

	ctx := buildCtxErr(jc, nil)
	_, err := CollectResults[string](ctx, "fo-any")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get fan-out")
}

func TestGen_CollectResults_GetSubJobsError(t *testing.T) {
	store := newErrStorage()
	parentID := "p-cr-getsubs-err"
	jc := makeErrJobCtx(store, parentID, "default")

	fanOutID := "fo-cr-subs"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutCompleted,
	}
	store.getSubJobsErr = errors.New("boom cr getsubjobs")

	ctx := buildCtxErr(jc, nil)
	_, err := CollectResults[string](ctx, fanOutID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get sub-jobs")
}

func TestGen_CollectResults_OutOfRangeFanOutIndex_Skipped(t *testing.T) {
	store := newErrStorage()
	parentID := "p-cr-oob"
	jc := makeErrJobCtx(store, parentID, "default")
	ctx := buildCtxErr(jc, nil)

	fanOutID := "fo-cr-oob"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutCompleted,
	}
	// Sub-jobs whose FanOutIndex is out of range are skipped (continue branch).
	store.jobs["sub-oob-neg"] = &core.Job{
		ID:          "sub-oob-neg",
		FanOutID:    &fanOutID,
		FanOutIndex: -1,
		Status:      core.StatusCompleted,
	}
	store.jobs["sub-oob-high"] = &core.Job{
		ID:          "sub-oob-high",
		FanOutID:    &fanOutID,
		FanOutIndex: 99,
		Status:      core.StatusCompleted,
	}

	results, err := CollectResults[string](ctx, fanOutID)
	require.NoError(t, err)
	// Only the single in-range slot exists; both out-of-range jobs are ignored,
	// leaving it incomplete.
	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0].Err, ErrSubJobIncomplete)
}

func TestGen_CollectResults_NonTerminalStatus_DefaultBranch(t *testing.T) {
	store := newErrStorage()
	parentID := "p-cr-default"
	jc := makeErrJobCtx(store, parentID, "default")
	ctx := buildCtxErr(jc, nil)

	fanOutID := "fo-cr-default"
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutCompleted,
	}
	// A running (non-terminal) sub-job exercises the default switch branch,
	// which marks the slot ErrSubJobIncomplete.
	store.jobs["sub-running"] = &core.Job{
		ID:          "sub-running",
		FanOutID:    &fanOutID,
		FanOutIndex: 0,
		Status:      core.StatusRunning,
	}

	results, err := CollectResults[string](ctx, fanOutID)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0].Err, ErrSubJobIncomplete)
}
