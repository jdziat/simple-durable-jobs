package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// minimalStorage is a minimal in-memory core.Storage for fanout unit tests.
// ---------------------------------------------------------------------------

type minimalStorage struct {
	jobs            map[string]*core.Job
	fanOuts         map[string]*core.FanOut
	checkpoints     map[string][]*core.Checkpoint
	suspended       map[string]bool
	enqueueBatchErr error
}

func newMinimalStorage() *minimalStorage {
	return &minimalStorage{
		jobs:        make(map[string]*core.Job),
		fanOuts:     make(map[string]*core.FanOut),
		checkpoints: make(map[string][]*core.Checkpoint),
		suspended:   make(map[string]bool),
	}
}

func (s *minimalStorage) Migrate(ctx context.Context) error { return nil }
func (s *minimalStorage) Enqueue(ctx context.Context, job *core.Job) error {
	s.jobs[job.ID] = job
	return nil
}
func (s *minimalStorage) EnqueueUnique(ctx context.Context, job *core.Job, key string) error {
	s.jobs[job.ID] = job
	return nil
}
func (s *minimalStorage) EnqueueBatch(ctx context.Context, jobs []*core.Job) error {
	if s.enqueueBatchErr != nil {
		return s.enqueueBatchErr
	}
	for _, j := range jobs {
		if j.UniqueKey != "" {
			duplicate := false
			for _, existing := range s.jobs {
				if existing.UniqueKey == j.UniqueKey {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}
		s.jobs[j.ID] = j
	}
	return nil
}
func (s *minimalStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	return nil, nil
}
func (s *minimalStorage) Complete(ctx context.Context, jobID, workerID string) error { return nil }
func (s *minimalStorage) Release(ctx context.Context, jobID, workerID string) error  { return nil }
func (s *minimalStorage) Fail(ctx context.Context, jobID, workerID, errMsg string, retryAt *time.Time) error {
	return nil
}
func (s *minimalStorage) GetJob(ctx context.Context, jobID string) (*core.Job, error) {
	return s.jobs[jobID], nil
}
func (s *minimalStorage) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
	var out []core.Checkpoint
	for _, cp := range s.checkpoints[jobID] {
		out = append(out, *cp)
	}
	return out, nil
}
func (s *minimalStorage) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	s.checkpoints[cp.JobID] = append(s.checkpoints[cp.JobID], cp)
	return nil
}
func (s *minimalStorage) DeleteCheckpoints(ctx context.Context, jobID string) error {
	delete(s.checkpoints, jobID)
	return nil
}
func (s *minimalStorage) GetDueJobs(ctx context.Context, queues []string, limit int) ([]*core.Job, error) {
	return nil, nil
}
func (s *minimalStorage) ClaimScheduledFire(ctx context.Context, name string, fireTime time.Time) (bool, error) {
	return true, nil
}
func (s *minimalStorage) Heartbeat(ctx context.Context, jobID, workerID string) error { return nil }
func (s *minimalStorage) ReleaseStaleLocks(ctx context.Context, d time.Duration) ([]string, error) {
	return nil, nil
}
func (s *minimalStorage) FindOrphanedJobs(ctx context.Context, jobIDs []string, workerID string) ([]string, error) {
	return nil, nil
}
func (s *minimalStorage) GetJobsByStatus(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	return nil, nil
}
func (s *minimalStorage) CreateFanOut(ctx context.Context, fo *core.FanOut) error {
	s.fanOuts[fo.ID] = fo
	return nil
}
func (s *minimalStorage) GetFanOut(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	return s.fanOuts[fanOutID], nil
}
func (s *minimalStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	if fo, ok := s.fanOuts[fanOutID]; ok {
		fo.CompletedCount++
		return fo, nil
	}
	return nil, nil
}
func (s *minimalStorage) IncrementFanOutFailed(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	if fo, ok := s.fanOuts[fanOutID]; ok {
		fo.FailedCount++
		return fo, nil
	}
	return nil, nil
}
func (s *minimalStorage) IncrementFanOutCancelled(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	if fo, ok := s.fanOuts[fanOutID]; ok {
		fo.CancelledCount++
		return fo, nil
	}
	return nil, nil
}
func (s *minimalStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
	if fo, ok := s.fanOuts[fanOutID]; ok {
		fo.Status = status
		return true, nil
	}
	return false, nil
}
func (s *minimalStorage) GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*core.FanOut, error) {
	return nil, nil
}
func (s *minimalStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	var out []*core.Job
	for _, j := range s.jobs {
		if j.FanOutID != nil && *j.FanOutID == fanOutID {
			out = append(out, j)
		}
	}
	return out, nil
}
func (s *minimalStorage) GetSubJobResults(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	var out []*core.Job
	for _, j := range s.jobs {
		if j.FanOutID != nil && *j.FanOutID == fanOutID &&
			(j.Status == core.StatusCompleted || j.Status == core.StatusFailed) {
			out = append(out, j)
		}
	}
	return out, nil
}
func (s *minimalStorage) CancelSubJobs(ctx context.Context, fanOutID string) ([]string, error) {
	return nil, nil
}
func (s *minimalStorage) CancelSubJob(ctx context.Context, jobID string) (*core.FanOut, error) {
	return nil, nil
}
func (s *minimalStorage) SuspendJob(ctx context.Context, jobID, workerID string) error {
	s.suspended[jobID] = true
	return nil
}
func (s *minimalStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	delete(s.suspended, jobID)
	return true, nil
}
func (s *minimalStorage) GetWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	return nil, nil
}
func (s *minimalStorage) GetStalledFanOutParents(ctx context.Context, olderThan time.Time) ([]*core.Job, error) {
	return nil, nil
}
func (s *minimalStorage) SaveJobResult(ctx context.Context, jobID, workerID string, result []byte) error {
	return nil
}
func (s *minimalStorage) PauseJob(ctx context.Context, jobID string) error   { return nil }
func (s *minimalStorage) UnpauseJob(ctx context.Context, jobID string) error { return nil }
func (s *minimalStorage) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
	return false, nil
}
func (s *minimalStorage) GetPausedJobs(ctx context.Context, queue string) ([]*core.Job, error) {
	return nil, nil
}
func (s *minimalStorage) PauseQueue(ctx context.Context, queue string) error   { return nil }
func (s *minimalStorage) UnpauseQueue(ctx context.Context, queue string) error { return nil }
func (s *minimalStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	return false, nil
}
func (s *minimalStorage) GetPausedQueues(ctx context.Context) ([]string, error) { return nil, nil }
func (s *minimalStorage) RefreshQueueStates(ctx context.Context) (map[string]bool, error) {
	return make(map[string]bool), nil
}

// ---------------------------------------------------------------------------
// Helpers for building job execution contexts
// ---------------------------------------------------------------------------

func makeJobCtx(store *minimalStorage, parentJobID, queue string) *intctx.JobContext {
	job := &core.Job{
		ID:    parentJobID,
		Queue: queue,
	}
	store.jobs[parentJobID] = job

	return &intctx.JobContext{
		Job:      job,
		Storage:  store,
		WorkerID: "test-worker",
		HandlerLookup: func(name string) (any, bool) {
			return nil, true // all handlers "exist"
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			return store.SaveCheckpoint(ctx, cp)
		},
	}
}

func buildCtx(jc *intctx.JobContext, checkpoints []core.Checkpoint) context.Context {
	ctx := context.Background()
	ctx = intctx.WithJobContext(ctx, jc)
	ctx = intctx.WithCallState(ctx, checkpoints)
	return ctx
}

func countSubJobs(store *minimalStorage, fanOutID string) int {
	count := 0
	for _, j := range store.jobs {
		if j.FanOutID != nil && *j.FanOutID == fanOutID {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// FanOut() — error paths and first-execution paths
// ---------------------------------------------------------------------------

func TestFanOut_EmptySubJobs_ReturnsNilNil(t *testing.T) {
	// Zero sub-jobs: FanOut returns early with nil, nil.
	ctx := context.Background()
	results, err := FanOut[int](ctx, nil)
	assert.NoError(t, err)
	assert.Nil(t, results)
}

func TestFanOut_NoJobContext_ReturnsError(t *testing.T) {
	ctx := context.Background()
	subs := []SubJob{{Type: "job", Args: "x"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be used within a job handler")
}

func TestFanOut_NoCallState_ReturnsError(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-1", "default")

	ctx := context.Background()
	ctx = intctx.WithJobContext(ctx, jc)
	// Intentionally omit WithCallState.

	subs := []SubJob{{Type: "job", Args: "x"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "call state not initialized")
}

func TestFanOut_FirstExecution_SuspendsAndReturnsError(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-first", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{
		{Type: "do-work", Args: "item-1"},
		{Type: "do-work", Args: "item-2"},
	}
	results, err := FanOut[string](ctx, subs)
	// FanOut returns a WaitingError on first execution.
	require.Error(t, err)
	assert.Nil(t, results)
	assert.True(t, IsWaitingError(err), "expected WaitingError, got: %v", err)

	// Parent job should be marked waiting.
	assert.True(t, store.suspended["parent-first"])

	// A checkpoint should have been saved.
	cps := store.checkpoints["parent-first"]
	require.NotEmpty(t, cps)
	assert.Equal(t, "fanout", cps[0].CallType)

	// Two sub-jobs should have been enqueued.
	var subJobCount int
	for _, j := range store.jobs {
		if j.ParentJobID != nil {
			subJobCount++
		}
	}
	assert.Equal(t, 2, subJobCount)
}

func TestFanOut_FirstExecution_InheritsTenantAndMetadata(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-metadata", "default")
	jc.Job.Tenant = "tenant-a"
	jc.Job.Metadata = map[string]string{"env": "prod", "trace": "abc"}
	ctx := buildCtx(jc, nil)

	_, err := FanOut[string](ctx, []SubJob{
		{Type: "do-work", Args: "item-1"},
		{Type: "do-work", Args: "item-2"},
	})
	require.Error(t, err)
	require.True(t, IsWaitingError(err))

	subJobs := make([]*core.Job, 0, 2)
	for _, job := range store.jobs {
		if job.ParentJobID != nil {
			subJobs = append(subJobs, job)
		}
	}
	require.Len(t, subJobs, 2)
	for _, job := range subJobs {
		assert.Equal(t, "tenant-a", job.Tenant)
		assert.Equal(t, map[string]string{"env": "prod", "trace": "abc"}, job.Metadata)
	}

	subJobs[0].Metadata["env"] = "mutated"
	assert.Equal(t, "prod", jc.Job.Metadata["env"])
	assert.Equal(t, "prod", subJobs[1].Metadata["env"])
}

func TestFanOut_HandlerLookup_MissingHandler_ReturnsError(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-lookup", "default")

	// Override HandlerLookup to reject all job types.
	jc.HandlerLookup = func(name string) (any, bool) {
		return nil, false
	}

	ctx := buildCtx(jc, nil)
	subs := []SubJob{{Type: "unknown-type", Args: "x"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no handler registered")
}

func TestFanOut_InvalidSubJobType_ReturnsErrorBeforeCreatingState(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-invalid-type", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{{Type: "123-invalid", Args: "x"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sub-job type")
	assert.Empty(t, store.fanOuts)
	assert.Empty(t, store.checkpoints["parent-invalid-type"])
	assert.False(t, store.suspended["parent-invalid-type"])
	assert.Equal(t, 1, len(store.jobs), "only the parent job should exist")
}

func TestFanOut_OversizedSubJobArgs_ReturnsErrorBeforeCreatingState(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-oversized", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{{Type: "do-work", Args: strings.Repeat("x", security.MaxJobArgsSize+1)}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrJobArgsTooLarge)
	assert.Empty(t, store.fanOuts)
	assert.Empty(t, store.checkpoints["parent-oversized"])
	assert.False(t, store.suspended["parent-oversized"])
	assert.Equal(t, 1, len(store.jobs), "only the parent job should exist")
}

func TestFanOut_WithTimeout_SetsTimeoutAt(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-timeout", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{{Type: "do-work", Args: 1}}
	_, err := FanOut[int](ctx, subs, WithTimeout(30*time.Second))
	// Still returns WaitingError on first execution.
	require.Error(t, err)
	assert.True(t, IsWaitingError(err))

	// The FanOut record should have a TimeoutAt set.
	for _, fo := range store.fanOuts {
		assert.NotNil(t, fo.TimeoutAt, "expected TimeoutAt to be set")
	}
}

func TestFanOut_SubJobTimeout_SetsJobTimeout(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-sub-timeout", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{
		Sub("do-work", "x", queue.Timeout(45*time.Second)),
		{Type: "do-work", Args: "y", Timeout: 90 * time.Second},
	}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	require.True(t, IsWaitingError(err))

	var timeouts []time.Duration
	for _, job := range store.jobs {
		if job.ParentJobID != nil {
			timeouts = append(timeouts, job.Timeout)
		}
	}
	require.ElementsMatch(t, []time.Duration{45 * time.Second, 90 * time.Second}, timeouts)
}

func TestFanOut_SubJobPriorityOverrideHonorsExplicitZero(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-sub-priority", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{
		Sub("do-work", "explicit-zero", queue.Priority(0)),
		Sub("do-work", "unset"),
		Sub("do-work", "explicit-nine", queue.Priority(9)),
	}
	_, err := FanOut[string](ctx, subs, WithPriority(5))
	require.Error(t, err)
	require.True(t, IsWaitingError(err))

	prioritiesByIndex := make(map[int]int)
	for _, job := range store.jobs {
		if job.ParentJobID != nil {
			prioritiesByIndex[job.FanOutIndex] = job.Priority
		}
	}

	require.Len(t, prioritiesByIndex, 3)
	assert.Equal(t, 0, prioritiesByIndex[0])
	assert.Equal(t, 5, prioritiesByIndex[1])
	assert.Equal(t, 9, prioritiesByIndex[2])
}

func TestFanOut_ResumeWithCompletedFanOut_ReturnsResults(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-resume"
	jc := makeJobCtx(store, parentID, "default")

	// Simulate first execution: save a checkpoint and create a FanOut.
	fanOutID := "fo-complete"
	cpData, _ := json.Marshal(core.FanOutCheckpoint{FanOutID: fanOutID, CallIndex: 0})
	existingCP := core.Checkpoint{
		ID:        "cp-1",
		JobID:     parentID,
		CallIndex: 0,
		CallType:  "fanout",
		Result:    cpData,
	}

	// Pre-populate: completed FanOut + sub-job results.
	resultBytes, _ := json.Marshal("done")
	foIndex := 0
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:             fanOutID,
		ParentJobID:    parentID,
		TotalCount:     1,
		CompletedCount: 1,
		Status:         core.FanOutCompleted,
	}
	store.jobs["sub-1"] = &core.Job{
		ID:          "sub-1",
		FanOutID:    &fanOutID,
		FanOutIndex: foIndex,
		Status:      core.StatusCompleted,
		Result:      resultBytes,
	}

	ctx := buildCtx(jc, []core.Checkpoint{existingCP})

	subs := []SubJob{{Type: "do-work", Args: "item"}}
	results, err := FanOut[string](ctx, subs)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "done", results[0].Value)
}

func TestFanOut_ResumeWithPendingFanOut_SuspendsAgain(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-still-waiting"
	jc := makeJobCtx(store, parentID, "default")

	fanOutID := "fo-pending"
	cpData, _ := json.Marshal(core.FanOutCheckpoint{FanOutID: fanOutID, CallIndex: 0})
	existingCP := core.Checkpoint{
		ID:        "cp-2",
		JobID:     parentID,
		CallIndex: 0,
		CallType:  "fanout",
		Result:    cpData,
	}

	// FanOut still pending (sub-jobs not done yet).
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  2,
		Status:      core.FanOutPending,
	}

	ctx := buildCtx(jc, []core.Checkpoint{existingCP})
	subs := []SubJob{{Type: "do-work", Args: "x"}, {Type: "do-work", Args: "y"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.True(t, IsWaitingError(err))
	// Parent should be marked waiting again.
	assert.True(t, store.suspended[parentID])
}

func TestFanOut_ResumeWithPendingFanOut_ReEnqueuesMissingSubJobs(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-recover-zero-subjobs"
	jc := makeJobCtx(store, parentID, "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{
		{Type: "do-work", Args: "x"},
		{Type: "do-work", Args: "y"},
	}

	store.enqueueBatchErr = errors.New("temporary enqueue failure")
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue sub-jobs")
	assert.False(t, store.suspended[parentID], "enqueue failure should resume the parent")
	assert.Equal(t, 1, len(store.jobs), "only the parent job should exist before replay")

	cps, err := store.GetCheckpoints(context.Background(), parentID)
	require.NoError(t, err)
	require.Len(t, cps, 1)

	var fanOutCP core.FanOutCheckpoint
	require.NoError(t, json.Unmarshal(cps[0].Result, &fanOutCP))
	require.Equal(t, 0, countSubJobs(store, fanOutCP.FanOutID))

	store.enqueueBatchErr = nil
	replayCtx := buildCtx(jc, cps)
	_, err = FanOut[string](replayCtx, subs)
	require.Error(t, err)
	assert.True(t, IsWaitingError(err))
	assert.True(t, store.suspended[parentID])
	assert.Equal(t, 2, countSubJobs(store, fanOutCP.FanOutID))
}

func TestFanOut_InvalidSubJobQueue_ReturnsError(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-badq", "default")
	// Use empty queue name in the sub-job so the parent job's queue fallback
	// hits the invalid name path. We force this by providing an empty job queue.
	jc.Job.Queue = "" // This will cause ValidateQueueName to fail.
	ctx := buildCtx(jc, nil)

	subs := []SubJob{{Type: "do-work", Args: "x"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sub-job queue")
}

// ---------------------------------------------------------------------------
// CollectResults()
// ---------------------------------------------------------------------------

func TestCollectResults_NoJobContext_ReturnsError(t *testing.T) {
	ctx := context.Background()
	_, err := CollectResults[int](ctx, "fo-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be used within a job handler")
}

func TestCollectResults_FanOutNotFound_ReturnsError(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-cr", "default")
	ctx := buildCtx(jc, nil)

	_, err := CollectResults[string](ctx, "nonexistent-fo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fan-out not found")
}

func TestCollectResults_CompletedFanOut_ReturnsValues(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-collect"
	jc := makeJobCtx(store, parentID, "default")
	ctx := buildCtx(jc, nil)

	fanOutID := "fo-collect"
	resultBytes, _ := json.Marshal(99)
	foIndex := 0
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:             fanOutID,
		ParentJobID:    parentID,
		TotalCount:     1,
		CompletedCount: 1,
		Status:         core.FanOutCompleted,
	}
	store.jobs["sub-collect"] = &core.Job{
		ID:          "sub-collect",
		FanOutID:    &fanOutID,
		FanOutIndex: foIndex,
		Status:      core.StatusCompleted,
		Result:      resultBytes,
	}

	results, err := CollectResults[int](ctx, fanOutID)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, 99, results[0].Value)
}

func TestCollectResults_CancelledSubJobAtNonZeroIndex_ReturnsCancelledErr(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-cancelled-sub"
	jc := makeJobCtx(store, parentID, "default")
	ctx := buildCtx(jc, nil)

	fanOutID := "fo-cancelled-sub"
	successBytes, _ := json.Marshal("ok")
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:             fanOutID,
		ParentJobID:    parentID,
		TotalCount:     3,
		CompletedCount: 1,
		CancelledCount: 1,
		Status:         core.FanOutCompleted,
	}
	store.jobs["sub-success"] = &core.Job{
		ID:          "sub-success",
		FanOutID:    &fanOutID,
		FanOutIndex: 0,
		Status:      core.StatusCompleted,
		Result:      successBytes,
	}
	store.jobs["sub-cancelled"] = &core.Job{
		ID:          "sub-cancelled",
		FanOutID:    &fanOutID,
		FanOutIndex: 2,
		Status:      core.StatusCancelled,
	}

	results, err := CollectResults[string](ctx, fanOutID)
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, 0, results[0].Index)
	assert.Equal(t, "ok", results[0].Value)
	assert.NoError(t, results[0].Err)
	assert.Equal(t, 2, results[2].Index)
	assert.ErrorIs(t, results[2].Err, ErrSubJobCancelled)
	assert.Equal(t, 1, results[1].Index)
	assert.ErrorIs(t, results[1].Err, ErrSubJobIncomplete)
}

func TestCollectResults_FailedFanOut_ReturnsErrorWithResults(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-failed-fo"
	jc := makeJobCtx(store, parentID, "default")
	ctx := buildCtx(jc, nil)

	fanOutID := "fo-failed"
	foIndex := 0
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		FailedCount: 1,
		Status:      core.FanOutFailed,
	}
	store.jobs["sub-failed"] = &core.Job{
		ID:          "sub-failed",
		FanOutID:    &fanOutID,
		FanOutIndex: foIndex,
		Status:      core.StatusFailed,
		LastError:   "network timeout",
	}

	results, err := CollectResults[string](ctx, fanOutID)
	require.Error(t, err)
	// Results are returned even on failure.
	assert.Len(t, results, 1)
	assert.NotNil(t, results[0].Err)

	var foErr *Error
	assert.True(t, errors.As(err, &foErr))
	assert.Equal(t, 1, foErr.FailedCount)

	var noRetryErr *core.NoRetryError
	assert.True(t, errors.As(err, &noRetryErr))
}

func TestCollectResults_BadResultJSON_ReturnsDecodeError(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-badjson"
	jc := makeJobCtx(store, parentID, "default")
	ctx := buildCtx(jc, nil)

	fanOutID := "fo-badjson"
	foIndex := 0
	store.fanOuts[fanOutID] = &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutCompleted,
	}
	store.jobs["sub-badjson"] = &core.Job{
		ID:          "sub-badjson",
		FanOutID:    &fanOutID,
		FanOutIndex: foIndex,
		Status:      core.StatusCompleted,
		Result:      []byte("not-valid-json-for-int!!!"),
	}

	results, err := CollectResults[int](ctx, fanOutID)
	require.NoError(t, err) // FanOut itself succeeded
	require.Len(t, results, 1)
	// The individual result should carry the unmarshal error.
	assert.NotNil(t, results[0].Err)
	assert.Contains(t, results[0].Err.Error(), "unmarshal")
}

func TestFanOut_ResumeWithBadCheckpointJSON_ReturnsError(t *testing.T) {
	store := newMinimalStorage()
	parentID := "parent-badcp"
	jc := makeJobCtx(store, parentID, "default")

	badCP := core.Checkpoint{
		ID:        "cp-bad",
		JobID:     parentID,
		CallIndex: 0,
		CallType:  "fanout",
		Result:    []byte("{invalid-json"),
	}
	ctx := buildCtx(jc, []core.Checkpoint{badCP})

	subs := []SubJob{{Type: "do-work", Args: "x"}}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal fan-out checkpoint")
}

// TestFanOut_AssignsIdempotencyKeyToSubJobs verifies that FanOut stamps a
// deterministic UniqueKey on every sub-job it creates, of the form
// "fanout-<fanOutID>-<index>". EnqueueBatch relies on these keys to skip
// duplicates when a parent workflow replays after a crash mid fan-out.
func TestFanOut_AssignsIdempotencyKeyToSubJobs(t *testing.T) {
	store := newMinimalStorage()
	jc := makeJobCtx(store, "parent-idem", "default")
	ctx := buildCtx(jc, nil)

	subs := []SubJob{
		{Type: "do-work", Args: "a"},
		{Type: "do-work", Args: "b"},
		{Type: "do-work", Args: "c"},
	}
	_, err := FanOut[string](ctx, subs)
	require.Error(t, err)
	require.True(t, IsWaitingError(err))

	// There should be exactly one FanOut record; pull its ID.
	require.Len(t, store.fanOuts, 1)
	var fanOutID string
	for id := range store.fanOuts {
		fanOutID = id
	}

	// Every sub-job's UniqueKey must be fanout-<id>-<index>, with each
	// index appearing exactly once.
	seen := map[int]string{}
	for _, j := range store.jobs {
		if j.ParentJobID == nil {
			continue
		}
		expected := fmt.Sprintf("fanout-%s-%d", fanOutID, j.FanOutIndex)
		assert.Equal(t, expected, j.UniqueKey,
			"sub-job index %d must have deterministic UniqueKey", j.FanOutIndex)
		assert.Empty(t, seen[j.FanOutIndex], "duplicate FanOutIndex %d", j.FanOutIndex)
		seen[j.FanOutIndex] = j.UniqueKey
	}
	assert.Len(t, seen, 3, "expected one UniqueKey per sub-job index")
}
