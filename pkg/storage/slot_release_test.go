package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// slotsHeldBy counts the live + sentinel ConcurrencySlot rows keyed to jobID.
func slotsHeldBy(t *testing.T, ctx context.Context, s *GormStorage, jobID core.UUID) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.WithContext(ctx).
		Model(&core.ConcurrencySlot{}).
		Where("job_id = ?", jobID).
		Count(&n).Error)
	return n
}

// ST-02: aggressive pause cancels a running job; the cancel must release the
// job's fleet concurrency slot in the same tx (the worker's deferred release
// never runs for a job cancelled out from under it), or the slot orphans until
// its TTL and silently shrinks the cap.
func TestPauseJobAggressive_ReleasesConcurrencySlot(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	jobID := testUUID("pause-cancel-slot")
	slot := uniqueSlotName(t)

	seedTestJob(t, ctx, s, jobID, core.StatusRunning)
	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobID, "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), slotsHeldBy(t, ctx, s, jobID), "slot acquired")

	require.NoError(t, s.PauseJob(ctx, jobID)) // aggressive: running -> cancelled

	got, err := s.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, core.StatusCancelled, got.Status, "running job is cancelled by aggressive pause")
	assert.Equal(t, int64(0), slotsHeldBy(t, ctx, s, jobID), "cancel must release the concurrency slot in-tx")
}

// ST-02: CancelSubJob (singular) cancels a running sub-job; like CancelSubJobs
// (plural) and terminal completion it must release the sub-job's slot in-tx.
func TestCancelSubJob_ReleasesConcurrencySlot(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	parentID := testUUID("cancelsub-parent")
	fanOutID := testUUID("cancelsub-fanout")
	subID := testUUID("cancelsub-running")
	slot := uniqueSlotName(t)

	seedTestJob(t, ctx, s, parentID, core.StatusWaiting)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          fanOutID,
		ParentJobID: parentID,
		TotalCount:  1,
		Status:      core.FanOutPending,
	}))
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID:       subID,
		Type:     "fixture.subjob",
		Queue:    "default",
		Status:   core.StatusRunning,
		FanOutID: &fanOutID,
	}).Error)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, subID, "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), slotsHeldBy(t, ctx, s, subID), "slot acquired")

	fanOut, err := s.CancelSubJob(ctx, subID)
	require.NoError(t, err)
	require.NotNil(t, fanOut, "sub-job has a fan-out")

	got, err := s.GetJob(ctx, subID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, core.StatusCancelled, got.Status)
	assert.Equal(t, int64(0), slotsHeldBy(t, ctx, s, subID), "cancel must release the concurrency slot in-tx")
}
