package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

// saveCheckpoints writes n distinct checkpoints for a job. Call indexes are
// unique so the (job_id, call_index, call_type) upsert never collapses them.
func saveCheckpoints(t *testing.T, ctx context.Context, s *GormStorage, jobID string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
			JobID:     jobID,
			CallIndex: i,
			CallType:  "call",
			Result:    []byte(`{"phase":"ok"}`),
		}))
	}
}

func checkpointCount(t *testing.T, ctx context.Context, s *GormStorage, jobID string) int {
	t.Helper()
	cps, err := s.GetCheckpoints(ctx, jobID)
	require.NoError(t, err)
	return len(cps)
}

// runningJob enqueues a pending job and dequeues it to running under workerID.
func runningJob(t *testing.T, ctx context.Context, s *GormStorage, workerID string) *core.Job {
	t.Helper()
	job := newTestJob("default", "gc.task")
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{"default"}, workerID)
	require.NoError(t, err)
	require.NotNil(t, got)
	return got
}

// TestComplete_DeletesCheckpointsOnSuccess proves the plain Complete path GCs
// checkpoints transactionally when the opt-in is enabled.
func TestComplete_DeletesCheckpointsOnSuccess(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 2)
	require.Equal(t, 2, checkpointCount(t, ctx, s, got.ID))

	require.NoError(t, s.Complete(ctx, got.ID, "worker-1"))

	assert.Equal(t, 0, checkpointCount(t, ctx, s, got.ID))
	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusCompleted, row.Status)
}

// TestComplete_DefaultKeepsCheckpoints proves the DEFAULT (opt-in OFF) preserves
// completed-job checkpoints so the dashboard can still render phase results —
// the UI guarantee the review verdict required.
func TestComplete_DefaultKeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 2)

	require.NoError(t, s.Complete(ctx, got.ID, "worker-1"))

	assert.Equal(t, 2, checkpointCount(t, ctx, s, got.ID))
	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCompleted, row.Status)
}

// TestCompleteWithResult_DeletesCheckpoints drives the production success path
// with the opt-in enabled: checkpoints GC'd, status completed, result persisted.
func TestCompleteWithResult_DeletesCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 2)
	result := []byte(`{"ok":true}`)

	fo, err := s.CompleteWithResult(ctx, got.ID, "worker-1", result)
	require.NoError(t, err)
	assert.Nil(t, fo) // non-fan-out job

	assert.Equal(t, 0, checkpointCount(t, ctx, s, got.ID))
	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusCompleted, row.Status)
	assert.Equal(t, result, row.Result)
}

// TestCompleteWithResult_DefaultKeepsCheckpoints proves the production success
// path KEEPS checkpoints when the opt-in is off (default).
func TestCompleteWithResult_DefaultKeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 3)

	_, err := s.CompleteWithResult(ctx, got.ID, "worker-1", []byte(`{"ok":true}`))
	require.NoError(t, err)

	assert.Equal(t, 3, checkpointCount(t, ctx, s, got.ID))
}

// TestFailTerminalWithResult_KeepsCheckpoints proves a terminal dead-letter
// NEVER deletes checkpoints, even with the success opt-in enabled.
func TestFailTerminalWithResult_KeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 2)

	fo, err := s.FailTerminalWithResult(ctx, got.ID, "worker-1", "boom")
	require.NoError(t, err)
	assert.Nil(t, fo)

	assert.Equal(t, 2, checkpointCount(t, ctx, s, got.ID))
	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusFailed, row.Status)
}

// TestFail_RetryKeepsCheckpoints is the replay/crash-equivalent test: a
// retryable failure moves the job back to pending and must keep checkpoints so
// the next attempt's replay can read them. Proves we did NOT GC too eagerly.
func TestFail_RetryKeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 2)

	future := time.Now().Add(time.Hour)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-1", "transient", &future))

	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusPending, row.Status)
	assert.Equal(t, 2, checkpointCount(t, ctx, s, got.ID))
}

// TestComplete_NotOwned_KeepsCheckpoints proves a Complete by the wrong worker
// returns ErrJobNotOwned and deletes nothing (the new owner owns that job).
func TestComplete_NotOwned_KeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	got := runningJob(t, ctx, s, "worker-1")
	saveCheckpoints(t, ctx, s, got.ID, 2)

	err := s.Complete(ctx, got.ID, "worker-2")
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Equal(t, 2, checkpointCount(t, ctx, s, got.ID))

	// CompleteWithResult on the not-owned path must also keep them.
	fo, err := s.CompleteWithResult(ctx, got.ID, "worker-2", []byte(`{"x":1}`))
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Nil(t, fo)
	assert.Equal(t, 2, checkpointCount(t, ctx, s, got.ID))
}

// TestCheckpointsTableBoundedAfterManyCompletions is THE regression test that
// would have caught finding #48: with the opt-in enabled, running many
// successful jobs that each write checkpoints leaves the checkpoints table at
// zero rows — bounded growth.
func TestCheckpointsTableBoundedAfterManyCompletions(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	const iterations = 25
	for i := 0; i < iterations; i++ {
		got := runningJob(t, ctx, s, "worker-1")
		saveCheckpoints(t, ctx, s, got.ID, 3)
		_, err := s.CompleteWithResult(ctx, got.ID, "worker-1", []byte(`{"ok":true}`))
		require.NoError(t, err)
	}

	var total int64
	require.NoError(t, s.db.Model(&core.Checkpoint{}).Count(&total).Error)
	assert.Equal(t, int64(0), total)
}

// TestFanOutSubJobSuccess_DeletesCheckpointsAndIncrements guards against the new
// delete disturbing P2's fan-out accounting: with the opt-in enabled the
// sub-job's checkpoints are GC'd in the same tx that increments completed_count.
func TestFanOutSubJobSuccess_DeletesCheckpointsAndIncrements(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetDeleteCheckpointsOnComplete(true)

	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")
	saveCheckpoints(t, ctx, s, job.ID, 2)

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-1", []byte(`{"sub":true}`))
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.CompletedCount)

	assert.Equal(t, 0, checkpointCount(t, ctx, s, job.ID))

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 1, after.CompletedCount)
}

// TestFanOutSubJobSuccess_DefaultKeepsCheckpoints confirms the default (opt-in
// off) preserves a fan-out sub-job's checkpoints while still incrementing.
func TestFanOutSubJobSuccess_DefaultKeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")
	saveCheckpoints(t, ctx, s, job.ID, 2)

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-1", []byte(`{"sub":true}`))
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.CompletedCount)
	assert.Equal(t, 2, checkpointCount(t, ctx, s, job.ID))
}
