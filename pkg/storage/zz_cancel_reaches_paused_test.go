package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cancellation used to enumerate {pending, waiting, running} and skip `paused`
// — and CancelSubJobs skipped `waiting` too. Both are LIVE, resumable states, so
// a terminal cancel left descendants that could still run: the dashboard's
// Resume button would execute work an operator had explicitly cancelled. The
// fan-out row was also frozen below its total, permanently violating the
// completed+failed+cancelled == total invariant that cancel-job.md promises is
// restored.
//
// FALSE-GREEN TRAP: seeding only `pending` children passes with or without the
// fix. The discriminating children are the paused and waiting ones.

func seedChild(t *testing.T, s *GormStorage, fanOutID core.UUID, status core.JobStatus) core.UUID {
	t.Helper()
	j := &core.Job{
		ID: core.NewID(), Type: "child", Queue: "default",
		Status: status, FanOutID: &fanOutID,
	}
	require.NoError(t, s.db.Create(j).Error)
	return j.ID
}

func TestCancelJobTerminal_ReachesPausedAndWaitingDescendants(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	parent := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.db.Create(parent).Error)

	fo := &core.FanOut{
		ID: core.NewID(), ParentJobID: parent.ID, Status: core.FanOutPending, TotalCount: 4,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo))

	pending := seedChild(t, s, fo.ID, core.StatusPending)
	running := seedChild(t, s, fo.ID, core.StatusRunning)
	paused := seedChild(t, s, fo.ID, core.StatusPaused)   // the discriminating one
	waiting := seedChild(t, s, fo.ID, core.StatusWaiting) // and this one

	require.NoError(t, s.CancelJobTerminal(ctx, parent.ID))

	for name, id := range map[string]core.UUID{
		"pending": pending, "running": running, "paused": paused, "waiting": waiting,
	} {
		var got core.Job
		require.NoError(t, s.db.First(&got, "id = ?", id).Error)
		assert.Equal(t, core.StatusCancelled, got.Status,
			"%s descendant must be cancelled — a live, resumable child surviving its parent's "+
				"terminal cancel is the defect", name)
	}

	var after core.FanOut
	require.NoError(t, s.db.First(&after, "id = ?", fo.ID).Error)
	assert.Equal(t, after.TotalCount,
		after.CompletedCount+after.FailedCount+after.CancelledCount,
		"INV-FANOUT-COUNTS: completed+failed+cancelled must equal total after a terminal cancel")
}

// TestResumeJob_LeavesOperatorPauseAlone is the converse. Every caller of
// storage.ResumeJob is an automatic fan-out-completion path, so accepting
// `paused` let a background event silently undo a human's decision.
func TestResumeJob_LeavesOperatorPauseAlone(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	j := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusPaused}
	require.NoError(t, s.db.Create(j).Error)

	resumed, err := s.ResumeJob(ctx, j.ID)
	require.NoError(t, err)
	assert.False(t, resumed, "fan-out completion must not auto-resume an operator-paused job")

	// The operator's own path still works, and restores the pre-pause status.
	var before core.Job
	require.NoError(t, s.db.First(&before, "id = ?", j.ID).Error)
	assert.Equal(t, core.StatusPaused, before.Status)
}

// TestGetWaitingJobsToResume_IncludesCancelledFanOuts covers the interaction the
// cancel widening creates. Descendant fan-outs can now end `cancelled`, a status
// the recovery backstop did not match — so a waiting parent of one would have
// hung forever, trading one stuck-job bug for another.
func TestGetWaitingJobsToResume_IncludesCancelledFanOuts(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	parent := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.db.Create(parent).Error)
	fo := &core.FanOut{ID: core.NewID(), ParentJobID: parent.ID, Status: core.FanOutCancelled, TotalCount: 1}
	require.NoError(t, s.db.Create(fo).Error)

	jobs, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)

	found := false
	for _, j := range jobs {
		if j.ID == parent.ID {
			found = true
		}
	}
	assert.True(t, found,
		"a waiting parent whose fan-out ended CANCELLED has nothing left to wait for and must be "+
			"picked up by the recovery backstop")
}

// TestCancelSubJobs_ReachesPausedAndWaitingSiblings covers the CancelOnFail
// path, which the CancelJobTerminal test above does NOT exercise.
//
// The 10x gate proved this gap by reverting only the CancelSubJobs call site to
// {pending, running} — every test in storage, queue, fanout and worker still
// passed, including the three added with the fix. A change nothing fails on is
// a change nothing protects.
func TestCancelSubJobs_ReachesPausedAndWaitingSiblings(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	parent := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.db.Create(parent).Error)
	fo := &core.FanOut{ID: core.NewID(), ParentJobID: parent.ID, Status: core.FanOutPending, TotalCount: 4}
	require.NoError(t, s.CreateFanOut(ctx, fo))

	pending := seedChild(t, s, fo.ID, core.StatusPending)
	running := seedChild(t, s, fo.ID, core.StatusRunning)
	paused := seedChild(t, s, fo.ID, core.StatusPaused)   // discriminating
	waiting := seedChild(t, s, fo.ID, core.StatusWaiting) // discriminating

	cancelled, err := s.CancelSubJobs(ctx, fo.ID)
	require.NoError(t, err)
	assert.Len(t, cancelled, 4, "every live sibling must be reported as cancelled")

	for name, id := range map[string]core.UUID{
		"pending": pending, "running": running, "paused": paused, "waiting": waiting,
	} {
		var got core.Job
		require.NoError(t, s.db.First(&got, "id = ?", id).Error)
		assert.Equal(t, core.StatusCancelled, got.Status,
			"%s sibling must be cancelled on the CancelOnFail path — a `waiting` child (durable Sleep, "+
				"WaitForSignal, nested fan-out) wakes up later and runs otherwise", name)
	}

	var after core.FanOut
	require.NoError(t, s.db.First(&after, "id = ?", fo.ID).Error)
	assert.Equal(t, after.TotalCount,
		after.CompletedCount+after.FailedCount+after.CancelledCount,
		"INV-FANOUT-COUNTS must hold on the CancelOnFail path too")
}

// TestCancelJobTerminal_CancelsADirectlyPausedJob pins the top-level contract
// change. Widening the target predicate means CancelJob on a paused job now
// SUCCEEDS where it returned an error, which is user-visible and belongs in
// UPGRADE.md — it is asserted here so it cannot change back unnoticed.
//
// FALSE-GREEN TRAP: asserting only the resulting status would pass under the old
// behaviour if the job happened to be cancellable by another route. The
// discriminating assertion is that the CALL returns nil.
func TestCancelJobTerminal_CancelsADirectlyPausedJob(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	j := &core.Job{ID: core.NewID(), Type: "solo", Queue: "default", Status: core.StatusPaused}
	require.NoError(t, s.db.Create(j).Error)

	require.NoError(t, s.CancelJobTerminal(ctx, j.ID),
		"cancelling a paused job must succeed; it previously required resume-then-cancel")

	var got core.Job
	require.NoError(t, s.db.First(&got, "id = ?", j.ID).Error)
	assert.Equal(t, core.StatusCancelled, got.Status)
}
