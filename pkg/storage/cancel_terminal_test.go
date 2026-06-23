package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func seedCancelJob(t *testing.T, ctx context.Context, s *GormStorage, id core.UUID, status core.JobStatus) {
	t.Helper()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID:     id,
		Type:   "cancel.fixture",
		Queue:  "default",
		Status: status,
	}).Error)
}

func seedCancelFanOut(t *testing.T, ctx context.Context, s *GormStorage, id, parentID core.UUID, total int, status core.FanOutStatus) {
	t.Helper()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.FanOut{
		ID:          id,
		ParentJobID: parentID,
		TotalCount:  total,
		Strategy:    core.StrategyFailFast,
		Status:      status,
	}).Error)
}

func seedCancelSubJob(t *testing.T, ctx context.Context, s *GormStorage, id, fanOutID core.UUID, index int, status core.JobStatus) {
	t.Helper()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID:          id,
		Type:        "cancel.child",
		Queue:       "default",
		Status:      status,
		FanOutID:    &fanOutID,
		FanOutIndex: index,
	}).Error)
}

func requireJobStatus(t *testing.T, ctx context.Context, s *GormStorage, id core.UUID, status core.JobStatus) {
	t.Helper()
	job, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, status, job.Status)
}

func requirePersistedFanOutAccounted(t *testing.T, ctx context.Context, s *GormStorage, id core.UUID) core.FanOut {
	t.Helper()
	var fo core.FanOut
	require.NoError(t, s.db.WithContext(ctx).First(&fo, "id = ?", id).Error)
	assert.Equal(t, fo.TotalCount, fo.CompletedCount+fo.FailedCount+fo.CancelledCount)
	return fo
}

func TestCancelJobTerminal_PendingToCancelled(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	jobID := testUUID("cancel-pending")
	seedCancelJob(t, ctx, s, jobID, core.StatusPending)

	require.NoError(t, s.CancelJobTerminal(ctx, jobID))

	job, err := s.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusCancelled, job.Status)
	assert.Equal(t, "cancelled by user", job.LastError)
	assert.NotNil(t, job.CompletedAt)
	assert.Empty(t, job.PreviousStatus)
}

func TestCancelJobTerminal_WaitingIsTerminalAndNotResumable(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	jobID := testUUID("cancel-waiting")
	seedCancelJob(t, ctx, s, jobID, core.StatusWaiting)

	require.NoError(t, s.CancelJobTerminal(ctx, jobID))

	resumed, err := s.ResumeJob(ctx, jobID)
	require.NoError(t, err)
	assert.False(t, resumed)
	err = s.UnpauseJob(ctx, jobID)
	require.ErrorIs(t, err, core.ErrJobNotPaused)
	requireJobStatus(t, ctx, s, jobID, core.StatusCancelled)
}

func TestCancelJobTerminal_RunningReleasesConcurrencySlot(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	jobID := testUUID("cancel-running-slot")
	slot := uniqueSlotName(t)
	seedCancelJob(t, ctx, s, jobID, core.StatusRunning)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobID, "worker-1", 1, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), slotsHeldBy(t, ctx, s, jobID))

	require.NoError(t, s.CancelJobTerminal(ctx, jobID))

	requireJobStatus(t, ctx, s, jobID, core.StatusCancelled)
	assert.Equal(t, int64(0), slotsHeldBy(t, ctx, s, jobID))
}

func TestCancelJobTerminal_SubtreeCancelsChildrenAndGrandchildren(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parentID := testUUID("cancel-tree-parent")
	childA := testUUID("cancel-tree-child-a")
	childB := testUUID("cancel-tree-child-b")
	grandchild := testUUID("cancel-tree-grandchild")
	foRoot := testUUID("cancel-tree-fo-root")
	foNested := testUUID("cancel-tree-fo-nested")

	seedCancelJob(t, ctx, s, parentID, core.StatusWaiting)
	seedCancelFanOut(t, ctx, s, foRoot, parentID, 2, core.FanOutPending)
	seedCancelSubJob(t, ctx, s, childA, foRoot, 0, core.StatusWaiting)
	seedCancelSubJob(t, ctx, s, childB, foRoot, 1, core.StatusPending)
	seedCancelFanOut(t, ctx, s, foNested, childA, 1, core.FanOutPending)
	seedCancelSubJob(t, ctx, s, grandchild, foNested, 0, core.StatusPending)

	require.NoError(t, s.CancelJobTerminal(ctx, parentID))

	for _, id := range []core.UUID{parentID, childA, childB, grandchild} {
		requireJobStatus(t, ctx, s, id, core.StatusCancelled)
	}
	for _, id := range []core.UUID{foRoot, foNested} {
		fo := requirePersistedFanOutAccounted(t, ctx, s, id)
		assert.Equal(t, core.FanOutCancelled, fo.Status)
	}
}

func TestCancelJobTerminal_PreFrozenFailFastFanOutReconcilesCounts(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parentID := testUUID("cancel-frozen-parent")
	foID := testUUID("cancel-frozen-fo")
	failedChild := testUUID("cancel-frozen-failed")
	pendingA := testUUID("cancel-frozen-a")
	pendingB := testUUID("cancel-frozen-b")

	seedCancelJob(t, ctx, s, parentID, core.StatusWaiting)
	seedCancelFanOut(t, ctx, s, foID, parentID, 3, core.FanOutFailed)
	require.NoError(t, s.db.WithContext(ctx).Model(&core.FanOut{}).
		Where("id = ?", foID).
		Updates(map[string]any{"failed_count": 1}).Error)
	seedCancelSubJob(t, ctx, s, failedChild, foID, 0, core.StatusFailed)
	seedCancelSubJob(t, ctx, s, pendingA, foID, 1, core.StatusPending)
	seedCancelSubJob(t, ctx, s, pendingB, foID, 2, core.StatusPending)

	require.NoError(t, s.CancelJobTerminal(ctx, parentID))

	fo := requirePersistedFanOutAccounted(t, ctx, s, foID)
	assert.Equal(t, core.FanOutFailed, fo.Status)
	assert.Equal(t, 1, fo.FailedCount)
	assert.Equal(t, 2, fo.CancelledCount)
}

func TestCancelJobTerminal_IdempotencyAndSentinels(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	cancelledID := testUUID("cancel-idempotent")
	completedID := testUUID("cancel-completed")
	failedID := testUUID("cancel-failed")
	missingID := testUUID("cancel-missing")

	seedCancelJob(t, ctx, s, cancelledID, core.StatusCancelled)
	seedCancelJob(t, ctx, s, completedID, core.StatusCompleted)
	seedCancelJob(t, ctx, s, failedID, core.StatusFailed)

	require.NoError(t, s.CancelJobTerminal(ctx, cancelledID))
	assert.ErrorIs(t, s.CancelJobTerminal(ctx, completedID), core.ErrJobNotCancellable)
	assert.ErrorIs(t, s.CancelJobTerminal(ctx, failedID), core.ErrJobNotCancellable)
	assert.ErrorIs(t, s.CancelJobTerminal(ctx, missingID), core.ErrJobNotFound)
}

func TestCancelJobTerminal_CancelledFanOutsNotCompletablePending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parentID := testUUID("cancel-completable-parent")
	foID := testUUID("cancel-completable-fo")
	childID := testUUID("cancel-completable-child")

	seedCancelJob(t, ctx, s, parentID, core.StatusWaiting)
	seedCancelFanOut(t, ctx, s, foID, parentID, 1, core.FanOutPending)
	seedCancelSubJob(t, ctx, s, childID, foID, 0, core.StatusPending)

	require.NoError(t, s.CancelJobTerminal(ctx, parentID))

	var fo core.FanOut
	require.NoError(t, s.db.WithContext(ctx).First(&fo, "id = ?", foID).Error)
	require.Equal(t, core.FanOutCancelled, fo.Status)

	found, err := s.GetCompletablePendingFanOuts(ctx, time.Now().Add(time.Second))
	require.NoError(t, err)
	for _, candidate := range found {
		assert.NotEqual(t, foID, candidate.ID)
	}
}

func TestCancelJobTerminal_WrapsSentinels(t *testing.T) {
	assert.True(t, errors.Is(core.ErrJobNotCancellable, core.ErrJobNotCancellable))
}

func seedRunningLockedJob(t *testing.T, ctx context.Context, s *GormStorage, id core.UUID, worker string, fanOutID *core.UUID, index int) {
	t.Helper()
	until := time.Now().Add(time.Hour)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID:          id,
		Type:        "cancel.fixture",
		Queue:       "default",
		Status:      core.StatusRunning,
		FanOutID:    fanOutID,
		FanOutIndex: index,
		LockedBy:    worker,
		LockedUntil: &until,
	}).Error)
}

// TestCancelJobTerminal_PreservesLockedByForOwnershipAudit is the production-path
// guard for the ownership-audit contract: a running job cancelled by a peer must
// keep locked_by = its owning worker so that worker's ~5s ownership audit
// (FindOrphanedJobs, which flags a terminal row only when locked_by <> ”)
// short-circuits the live handler. Clearing locked_by would defer the stop to
// the ~minutes heartbeat fallback. Exercises both the directly-cancelled root
// and a subtree-cancelled child (the shared cancelFanOutChildrenAndReconcile).
func TestCancelJobTerminal_PreservesLockedByForOwnershipAudit(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	rootID := testUUID("cancel-lockedby-root")
	seedRunningLockedJob(t, ctx, s, rootID, "worker-remote", nil, 0)

	parentID := testUUID("cancel-lockedby-parent")
	foID := testUUID("cancel-lockedby-fo")
	childID := testUUID("cancel-lockedby-child")
	seedCancelJob(t, ctx, s, parentID, core.StatusWaiting)
	seedCancelFanOut(t, ctx, s, foID, parentID, 1, core.FanOutPending)
	seedRunningLockedJob(t, ctx, s, childID, "worker-remote", &foID, 0)

	require.NoError(t, s.CancelJobTerminal(ctx, rootID))
	require.NoError(t, s.CancelJobTerminal(ctx, parentID))

	for _, id := range []core.UUID{rootID, childID} {
		job, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, core.StatusCancelled, job.Status)
		assert.Equal(t, "worker-remote", job.LockedBy, "cancel must not clear locked_by (ownership-audit fast-path)")
	}

	orphaned, err := s.FindOrphanedJobs(ctx, []core.UUID{rootID, childID}, "worker-remote")
	require.NoError(t, err)
	assert.Contains(t, orphaned, rootID, "directly-cancelled running root must still be flagged for its owner")
	assert.Contains(t, orphaned, childID, "subtree-cancelled running child must still be flagged for its owner")
}

// TestCancelSubJobs_PreservesLockedBy guards the pre-existing CancelOnFail path
// (which shares cancelFanOutChildrenAndReconcile) against the same regression,
// and pins that it does not write a last_error on peer-cancelled siblings.
func TestCancelSubJobs_PreservesLockedBy(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parentID := testUUID("cancelsub-lockedby-parent")
	foID := testUUID("cancelsub-lockedby-fo")
	childID := testUUID("cancelsub-lockedby-child")
	seedCancelJob(t, ctx, s, parentID, core.StatusWaiting)
	seedCancelFanOut(t, ctx, s, foID, parentID, 1, core.FanOutPending)
	seedRunningLockedJob(t, ctx, s, childID, "worker-remote", &foID, 0)

	_, err := s.CancelSubJobs(ctx, foID)
	require.NoError(t, err)

	job, err := s.GetJob(ctx, childID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusCancelled, job.Status)
	assert.Equal(t, "worker-remote", job.LockedBy, "CancelSubJobs must not clear locked_by")
	assert.Empty(t, job.LastError, "CancelSubJobs preserves empty last_error on peer-cancelled siblings")
}
