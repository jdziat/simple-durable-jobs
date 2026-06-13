package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: GetQueueStats / GetQueueDepthStats
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_GetQueueDepthStats_CountsByStatus(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	seed := []struct {
		queue  string
		status core.JobStatus
		count  int
	}{
		{"alpha", core.StatusPending, 2},
		{"alpha", core.StatusRunning, 1},
		{"alpha", core.StatusCompleted, 3},
		{"beta", core.StatusFailed, 1},
		{"beta", core.StatusPaused, 1},
	}
	for _, sd := range seed {
		for i := 0; i < sd.count; i++ {
			j := newTestJob(sd.queue, "work")
			j.Status = sd.status
			require.NoError(t, s.Enqueue(ctx, j))
		}
	}

	// Pause queue "beta" so the IsPaused branch is exercised.
	require.NoError(t, s.PauseQueue(ctx, "beta"))

	stats, err := s.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, stats)

	byQueue := make(map[string]*struct {
		pending, running, completed, failed, paused int64
		isPaused                                    bool
	})
	for _, qs := range stats {
		byQueue[qs.Name] = &struct {
			pending, running, completed, failed, paused int64
			isPaused                                    bool
		}{qs.Pending, qs.Running, qs.Completed, qs.Failed, qs.Paused, qs.IsPaused}
	}

	require.Contains(t, byQueue, "alpha")
	assert.Equal(t, int64(2), byQueue["alpha"].pending)
	assert.Equal(t, int64(1), byQueue["alpha"].running)
	assert.Equal(t, int64(3), byQueue["alpha"].completed)
	assert.False(t, byQueue["alpha"].isPaused)

	require.Contains(t, byQueue, "beta")
	assert.Equal(t, int64(1), byQueue["beta"].failed)
	assert.Equal(t, int64(1), byQueue["beta"].paused)
	assert.True(t, byQueue["beta"].isPaused)
}

func TestZZ_GetQueueStats_DelegatesToDepthStats(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	j := newTestJob("gamma", "work")
	j.Status = core.StatusPending
	require.NoError(t, s.Enqueue(ctx, j))

	stats, err := s.GetQueueStats(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, "gamma", stats[0].Name)
	assert.Equal(t, int64(1), stats[0].Pending)
}

func TestZZ_GetQueueDepthStats_Empty(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	stats, err := s.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	assert.Empty(t, stats)
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: RetryJob
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_RetryJob_FailedJobResetToPending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	now := time.Now()
	j := newTestJob("default", "work")
	j.Status = core.StatusFailed
	j.Attempt = 3
	j.LastError = "boom"
	j.LockedBy = "worker-1"
	j.LockedUntil = &now
	j.StartedAt = &now
	j.CompletedAt = &now
	require.NoError(t, s.Enqueue(ctx, j))

	updated, err := s.RetryJob(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, updated.Status)
	assert.Equal(t, 0, updated.Attempt)
	assert.Empty(t, updated.LastError)
	assert.Empty(t, updated.LockedBy)
	assert.Nil(t, updated.LockedUntil)
	assert.Nil(t, updated.StartedAt)
	assert.Nil(t, updated.CompletedAt)

	// Verify it persisted.
	persisted, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, persisted.Status)
}

func TestZZ_RetryJob_CancelledJobAllowed(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	j := newTestJob("default", "work")
	j.Status = core.StatusCancelled
	require.NoError(t, s.Enqueue(ctx, j))

	updated, err := s.RetryJob(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, updated.Status)
}

func TestZZ_RetryJob_RejectsNonRetryableStatus(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	j := newTestJob("default", "work")
	j.Status = core.StatusRunning
	require.NoError(t, s.Enqueue(ctx, j))

	_, err := s.RetryJob(ctx, j.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot retry")
}

func TestZZ_RetryJob_MissingJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	_, err := s.RetryJob(ctx, "does-not-exist")
	require.Error(t, err)
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: DeleteJob
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_DeleteJob_RemovesJobAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	j := newTestJob("default", "work")
	j.Status = core.StatusCompleted
	require.NoError(t, s.Enqueue(ctx, j))

	cp := &core.Checkpoint{
		ID:        "cp-1",
		JobID:     j.ID,
		CallIndex: 0,
		CallType:  "Call",
		Result:    []byte(`"ok"`),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp))

	require.NoError(t, s.DeleteJob(ctx, j.ID))

	gone, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	assert.Nil(t, gone)

	checkpoints, err := s.GetCheckpoints(ctx, j.ID)
	require.NoError(t, err)
	assert.Empty(t, checkpoints)
}

func TestZZ_DeleteJob_NonexistentIsNoError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.DeleteJob(ctx, "nope"))
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: PurgeJobs
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_PurgeJobs_ByQueueAndStatus(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	for i := 0; i < 3; i++ {
		j := newTestJob("q1", "work")
		j.Status = core.StatusCompleted
		require.NoError(t, s.Enqueue(ctx, j))
	}
	keep := newTestJob("q1", "work")
	keep.Status = core.StatusPending
	require.NoError(t, s.Enqueue(ctx, keep))

	other := newTestJob("q2", "work")
	other.Status = core.StatusCompleted
	require.NoError(t, s.Enqueue(ctx, other))

	deleted, err := s.PurgeJobs(ctx, "q1", core.StatusCompleted)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	// q1 pending kept.
	_, err = s.GetJob(ctx, keep.ID)
	require.NoError(t, err)
	// q2 completed kept (different queue).
	_, err = s.GetJob(ctx, other.ID)
	require.NoError(t, err)
}

func TestZZ_PurgeJobs_AllQueuesWhenQueueEmpty(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	for _, q := range []string{"qa", "qb"} {
		j := newTestJob(q, "work")
		j.Status = core.StatusFailed
		require.NoError(t, s.Enqueue(ctx, j))
	}

	deleted, err := s.PurgeJobs(ctx, "", core.StatusFailed)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: GetWorkflowRoots
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_GetWorkflowRoots_ReturnsParentsWithChildren(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	root := newTestJob("default", "workflow")
	root.Status = core.StatusRunning
	require.NoError(t, s.Enqueue(ctx, root))

	// A standalone job with no children must NOT appear.
	standalone := newTestJob("default", "standalone")
	standalone.Status = core.StatusRunning
	require.NoError(t, s.Enqueue(ctx, standalone))

	child := newTestJob("default", "child")
	child.Status = core.StatusRunning
	child.ParentJobID = &root.ID
	require.NoError(t, s.Enqueue(ctx, child))

	roots, total, err := s.GetWorkflowRoots(ctx, "", 0, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, roots, 1)
	assert.Equal(t, root.ID, roots[0].ID)
}

func TestZZ_GetWorkflowRoots_StatusFilterAndPaging(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	rootDone := newTestJob("default", "wf")
	rootDone.Status = core.StatusCompleted
	require.NoError(t, s.Enqueue(ctx, rootDone))
	childA := newTestJob("default", "child")
	childA.ParentJobID = &rootDone.ID
	require.NoError(t, s.Enqueue(ctx, childA))

	rootRunning := newTestJob("default", "wf")
	rootRunning.Status = core.StatusRunning
	require.NoError(t, s.Enqueue(ctx, rootRunning))
	childB := newTestJob("default", "child")
	childB.ParentJobID = &rootRunning.ID
	require.NoError(t, s.Enqueue(ctx, childB))

	// Filter by completed status only.
	roots, total, err := s.GetWorkflowRoots(ctx, "completed", 0, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, roots, 1)
	assert.Equal(t, rootDone.ID, roots[0].ID)

	// Multi-status, with explicit limit and offset to exercise paging branch.
	rootsAll, totalAll, err := s.GetWorkflowRoots(ctx, "completed,running", 1, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), totalAll)
	assert.Len(t, rootsAll, 1)
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: GetFanOutsByParents / GetSubJobsByFanOuts
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_GetFanOutsByParents_And_SubJobsByFanOuts(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent1 := newTestJob("default", "parent")
	parent1.Status = core.StatusWaiting
	require.NoError(t, s.Enqueue(ctx, parent1))
	parent2 := newTestJob("default", "parent")
	parent2.Status = core.StatusWaiting
	require.NoError(t, s.Enqueue(ctx, parent2))

	fo1 := &core.FanOut{
		ID:          "fo-1",
		ParentJobID: parent1.ID,
		TotalCount:  2,
		Strategy:    core.StrategyFailFast,
		Status:      core.FanOutPending,
	}
	fo2 := &core.FanOut{
		ID:          "fo-2",
		ParentJobID: parent2.ID,
		TotalCount:  1,
		Strategy:    core.StrategyCollectAll,
		Status:      core.FanOutPending,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo1))
	require.NoError(t, s.CreateFanOut(ctx, fo2))

	// Sub-jobs belonging to fo1.
	for i := 0; i < 2; i++ {
		sub := newTestJob("default", "sub")
		sub.Status = core.StatusPending
		foID := fo1.ID
		sub.FanOutID = &foID
		sub.FanOutIndex = i
		sub.ParentJobID = &parent1.ID
		require.NoError(t, s.Enqueue(ctx, sub))
	}
	// One sub-job for fo2.
	sub2 := newTestJob("default", "sub")
	sub2.Status = core.StatusPending
	fo2ID := fo2.ID
	sub2.FanOutID = &fo2ID
	sub2.ParentJobID = &parent2.ID
	require.NoError(t, s.Enqueue(ctx, sub2))

	fanOuts, err := s.GetFanOutsByParents(ctx, []string{parent1.ID, parent2.ID})
	require.NoError(t, err)
	require.Len(t, fanOuts, 2)

	subJobs, err := s.GetSubJobsByFanOuts(ctx, []string{fo1.ID, fo2.ID})
	require.NoError(t, err)
	assert.Len(t, subJobs, 3)

	// Verify ordering within a single fan-out (fan_out_index ascending).
	onlyFo1, err := s.GetSubJobsByFanOuts(ctx, []string{fo1.ID})
	require.NoError(t, err)
	require.Len(t, onlyFo1, 2)
	assert.Equal(t, 0, onlyFo1[0].FanOutIndex)
	assert.Equal(t, 1, onlyFo1[1].FanOutIndex)
}

func TestZZ_GetFanOutsByParents_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	fanOuts, err := s.GetFanOutsByParents(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, fanOuts)
}

func TestZZ_GetSubJobsByFanOuts_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	jobs, err := s.GetSubJobsByFanOuts(ctx, []string{})
	require.NoError(t, err)
	assert.Nil(t, jobs)
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm_ui.go: SearchJobs (remaining filter branches)
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_SearchJobs_FiltersAndPaging(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	base := time.Now()
	mk := func(id, queue, jobType string, status core.JobStatus, created time.Time) {
		j := &core.Job{
			ID:        id,
			Type:      jobType,
			Queue:     queue,
			Status:    status,
			Args:      []byte(`{}`),
			CreatedAt: created,
		}
		require.NoError(t, s.Enqueue(ctx, j))
	}

	mk("j-old", "q1", "typeA", core.StatusPending, base.Add(-2*time.Hour))
	mk("j-mid", "q1", "typeA", core.StatusPending, base.Add(-1*time.Hour))
	mk("j-new", "q1", "typeB", core.StatusRunning, base)
	mk("j-other", "q2", "typeA", core.StatusPending, base)

	// Status + Queue + Type filters together.
	res, total, err := s.SearchJobs(ctx, core.JobFilter{
		Status: string(core.StatusPending),
		Queue:  "q1",
		Type:   "typeA",
		Limit:  10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, res, 2)

	// Since / Until time window filters.
	windowed, wtotal, err := s.SearchJobs(ctx, core.JobFilter{
		Since: base.Add(-90 * time.Minute),
		Until: base.Add(-30 * time.Minute),
		Limit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), wtotal)
	require.Len(t, windowed, 1)
	assert.Equal(t, "j-mid", windowed[0].ID)

	// Default limit branch (Limit <= 0) and offset.
	page, ptotal, err := s.SearchJobs(ctx, core.JobFilter{Offset: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(4), ptotal)
	assert.Len(t, page, 3)
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm.go: Enqueue partial branches (defaults + unique-key dedup)
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_Enqueue_AppliesDefaults(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	j := &core.Job{Type: "work"} // no ID, no Status, no Queue
	require.NoError(t, s.Enqueue(ctx, j))
	assert.NotEmpty(t, j.ID)
	assert.Equal(t, core.StatusPending, j.Status)
	assert.Equal(t, "default", j.Queue)

	stored, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, "default", stored.Queue)
	assert.Equal(t, core.StatusPending, stored.Status)
}

func TestZZ_Enqueue_UniqueKeyDuplicateRejected(t *testing.T) {
	// Plain Enqueue dedup is enforced on every backend: SQLite/PostgreSQL via the
	// idx_jobs_active_unique partial unique index, MySQL via the equivalent
	// active_unique_key generated-column unique index (migration 2).
	ctx := context.Background()
	s := newTestStorage(t)

	first := &core.Job{Type: "work", UniqueKey: "dup-key"}
	require.NoError(t, s.Enqueue(ctx, first))

	second := &core.Job{Type: "work", UniqueKey: "dup-key"}
	err := s.Enqueue(ctx, second)
	require.ErrorIs(t, err, core.ErrDuplicateJob)
}

func TestZZ_Enqueue_UniqueKeyDistinctSucceeds(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, &core.Job{Type: "work", UniqueKey: "key-a"}))
	require.NoError(t, s.Enqueue(ctx, &core.Job{Type: "work", UniqueKey: "key-b"}))
}

// ─────────────────────────────────────────────────────────────────────────────
// gorm.go: Dequeue / dequeueSQLite remaining branches
// ─────────────────────────────────────────────────────────────────────────────

func TestZZ_Dequeue_AllQueuesPausedReturnsNil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("paused-q", "work")))
	require.NoError(t, s.PauseQueue(ctx, "paused-q"))

	job, err := s.Dequeue(ctx, []string{"paused-q"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, job)
}

func TestZZ_Dequeue_NoPendingReturnsNil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job, err := s.Dequeue(ctx, []string{"empty-q"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, job)
}

func TestZZ_Dequeue_SkipsFutureRunAtJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	future := time.Now().Add(time.Hour)
	delayed := newTestJob("default", "work")
	delayed.RunAt = &future
	require.NoError(t, s.Enqueue(ctx, delayed))

	job, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, job, "future-scheduled job must not be dequeued")
}

func TestZZ_Dequeue_PicksHighestPriorityPendingJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	low := newTestJob("default", "work")
	low.Priority = 1
	require.NoError(t, s.Enqueue(ctx, low))

	high := newTestJob("default", "work")
	high.Priority = 10
	require.NoError(t, s.Enqueue(ctx, high))

	job, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, high.ID, job.ID)
	assert.Equal(t, core.StatusRunning, job.Status)
	assert.Equal(t, "worker-1", job.LockedBy)
	assert.Equal(t, 1, job.Attempt)
	require.NotNil(t, job.LockedUntil)
	require.NotNil(t, job.StartedAt)
}
