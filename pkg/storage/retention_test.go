package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func TestDeleteTerminalJobsOlderThan_StatusWindows(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	recent := time.Now().Add(-10 * time.Minute).UTC()

	seedRetentionJob(t, s, "completed-old", core.StatusCompleted, old)
	seedRetentionJob(t, s, "completed-new", core.StatusCompleted, recent)
	seedRetentionJob(t, s, "failed-old", core.StatusFailed, old)
	seedRetentionJob(t, s, "failed-new", core.StatusFailed, recent)
	seedRetentionJob(t, s, "cancelled-old", core.StatusCancelled, old)
	seedRetentionJob(t, s, "cancelled-new", core.StatusCancelled, recent)
	for _, status := range []core.JobStatus{
		core.StatusPending,
		core.StatusRunning,
		core.StatusWaiting,
		core.StatusPaused,
	} {
		seedRetentionJob(t, s, string(status)+"-old", status, old)
	}

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	deleted, err = s.DeleteTerminalJobsOlderThan(ctx, core.StatusFailed, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	deleted, err = s.DeleteTerminalJobsOlderThan(ctx, core.StatusCancelled, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	assertRetentionMissing(t, s, "completed-old")
	assertRetentionExists(t, s, "completed-new")
	assertRetentionMissing(t, s, "failed-old")
	assertRetentionExists(t, s, "failed-new")
	assertRetentionMissing(t, s, "cancelled-old")
	assertRetentionExists(t, s, "cancelled-new")
	for _, status := range []core.JobStatus{
		core.StatusPending,
		core.StatusRunning,
		core.StatusWaiting,
		core.StatusPaused,
	} {
		assertRetentionExists(t, s, string(status)+"-old")
	}
}

func TestDeleteTerminalJobsOlderThan_BatchLimitAndZeroWindow(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	for i := 0; i < 3; i++ {
		seedRetentionJob(t, s, fmt.Sprintf("batch-%d", i), core.StatusCompleted, old)
	}

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	deleted, err = s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	var remaining int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("status = ?", core.StatusCompleted).Count(&remaining).Error)
	assert.Equal(t, int64(1), remaining)
}

func TestDeleteTerminalJobsOlderThan_ConcurrentPasses(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	for i := 0; i < 10; i++ {
		seedRetentionJob(t, s, fmt.Sprintf("concurrent-%d", i), core.StatusCompleted, old)
	}

	const workers = 2
	var wg sync.WaitGroup
	deleted := make([]int64, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			deleted[i], errs[i] = s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 10)
		}(i)
	}
	close(start)
	wg.Wait()

	var total int64
	for i, err := range errs {
		require.NoErrorf(t, err, "retention worker %d", i)
		total += deleted[i]
	}
	assert.Equal(t, int64(10), total, "concurrent passes must not double-count")

	var remaining int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("status = ?", core.StatusCompleted).Count(&remaining).Error)
	assert.Equal(t, int64(0), remaining)
}

func TestDeleteTerminalJobsOlderThan_WorkflowGuardAndFanOutCleanup(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	recent := time.Now().Add(-10 * time.Minute).UTC()
	rootID := "ret-root"

	seedRetentionJob(t, s, rootID, core.StatusWaiting, old)
	seedRetentionWorkflowJob(t, s, "ret-sub-parent", core.StatusCompleted, old, &rootID, &rootID, nil)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("ret-fanout"),
		ParentJobID: retentionUUID("ret-sub-parent"),
		TotalCount:  1,
		Status:      core.FanOutPending,
	}))
	fanOutID := "ret-fanout"
	seedRetentionWorkflowJob(t, s, "ret-live-child", core.StatusRunning, old, ptrString("ret-sub-parent"), &rootID, &fanOutID)

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	assertRetentionExists(t, s, "ret-sub-parent")
	assertRetentionFanOutExists(t, s, "ret-fanout")

	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", retentionUUID("ret-live-child")).
		Updates(map[string]any{"status": core.StatusCompleted, "completed_at": recent}).Error)
	require.NoError(t, s.db.Model(&core.FanOut{}).
		Where("id = ?", retentionUUID("ret-fanout")).
		Update("status", core.FanOutCompleted).Error)

	deleted, err = s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assertRetentionMissing(t, s, "ret-sub-parent")
	assertRetentionExists(t, s, "ret-live-child")
	assertRetentionFanOutMissing(t, s, "ret-fanout")
}

func TestDeleteTerminalJobsOlderThan_RootChildGuardBlocksDeletion(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	rootID := "ret-root-live-child"

	seedRetentionJob(t, s, rootID, core.StatusCompleted, old)
	seedRetentionWorkflowJob(t, s, "ret-root-only-live-child", core.StatusRunning, old, nil, &rootID, nil)

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	assertRetentionExists(t, s, rootID)
	assertRetentionExists(t, s, "ret-root-only-live-child")
}

func TestDeleteTerminalJobsOlderThan_S04RegressionNoFanOutLeakOrStrandedLiveDescendants(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	rootID := "s04-root"

	seedRetentionJob(t, s, rootID, core.StatusCompleted, old)
	seedRetentionWorkflowJob(t, s, "s04-sub-parent", core.StatusCompleted, old, &rootID, &rootID, nil)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("s04-fanout"),
		ParentJobID: retentionUUID("s04-sub-parent"),
		TotalCount:  2,
		Status:      core.FanOutPending,
	}))
	fanOutID := "s04-fanout"
	seedRetentionWorkflowJob(t, s, "s04-grand-0", core.StatusRunning, old, ptrString("s04-sub-parent"), &rootID, &fanOutID)
	seedRetentionWorkflowJob(t, s, "s04-grand-1", core.StatusPending, old, ptrString("s04-sub-parent"), &rootID, &fanOutID)

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	assertRetentionExists(t, s, rootID)
	assertRetentionExists(t, s, "s04-sub-parent")
	assertRetentionExists(t, s, "s04-grand-0")
	assertRetentionExists(t, s, "s04-grand-1")

	var leakedFanOuts int64
	require.NoError(t, s.db.Model(&core.FanOut{}).
		Where("NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = fan_outs.parent_job_id)").
		Count(&leakedFanOuts).Error)
	assert.Equal(t, int64(0), leakedFanOuts, "retention must not orphan fan_outs")

	var strandedLiveDescendants int64
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("status NOT IN ?", []core.JobStatus{core.StatusCompleted, core.StatusFailed, core.StatusCancelled}).
		Where("(parent_job_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM jobs p WHERE p.id = jobs.parent_job_id)) OR (root_job_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM jobs r WHERE r.id = jobs.root_job_id))").
		Count(&strandedLiveDescendants).Error)
	assert.Equal(t, int64(0), strandedLiveDescendants, "retention must not strand live descendants")
}

func TestCleanAbandonedFanOuts_UnblocksTerminalParentRetention(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	cutoff := now.Add(-1 * time.Minute)

	seedRetentionJob(t, s, "abandoned-parent", core.StatusCompleted, old)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("abandoned-fanout"),
		ParentJobID: retentionUUID("abandoned-parent"),
		TotalCount:  2,
		Status:      core.FanOutPending,
	}))
	setFanOutCreatedAt(t, s, "abandoned-fanout", old)

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "pending abandoned fan-out should block parent retention before cleanup")
	assertRetentionExists(t, s, "abandoned-parent")
	assertRetentionFanOutExists(t, s, "abandoned-fanout")

	cleaned, err := s.CleanAbandonedFanOuts(ctx, cutoff)
	require.NoError(t, err)
	assert.Equal(t, int64(1), cleaned)
	assertRetentionFanOutMissing(t, s, "abandoned-fanout")

	deleted, err = s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assertRetentionMissing(t, s, "abandoned-parent")
}

func TestCleanAbandonedFanOuts_Guards(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	cutoff := now.Add(-1 * time.Minute)

	seedRetentionJob(t, s, "guard-waiting-parent", core.StatusWaiting, old)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("guard-waiting-fanout"),
		ParentJobID: retentionUUID("guard-waiting-parent"),
		TotalCount:  2,
		Status:      core.FanOutPending,
	}))
	setFanOutCreatedAt(t, s, "guard-waiting-fanout", old)

	seedRetentionJob(t, s, "guard-with-subjobs-parent", core.StatusCompleted, old)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("guard-with-subjobs-fanout"),
		ParentJobID: retentionUUID("guard-with-subjobs-parent"),
		TotalCount:  2,
		Status:      core.FanOutPending,
	}))
	setFanOutCreatedAt(t, s, "guard-with-subjobs-fanout", old)
	fanOutID := retentionUUID("guard-with-subjobs-fanout")
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{{
		ID:       retentionUUID("guard-subjob"),
		Type:     "guard.sub",
		Queue:    "default",
		Status:   core.StatusPending,
		FanOutID: &fanOutID,
	}}))

	seedRetentionJob(t, s, "guard-recent-parent", core.StatusCompleted, old)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("guard-recent-fanout"),
		ParentJobID: retentionUUID("guard-recent-parent"),
		TotalCount:  2,
		Status:      core.FanOutPending,
	}))
	setFanOutCreatedAt(t, s, "guard-recent-fanout", now)

	seedRetentionJob(t, s, "guard-completed-parent", core.StatusCompleted, old)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID:          retentionUUID("guard-completed-fanout"),
		ParentJobID: retentionUUID("guard-completed-parent"),
		TotalCount:  2,
		Status:      core.FanOutCompleted,
	}))
	setFanOutCreatedAt(t, s, "guard-completed-fanout", old)

	cleaned, err := s.CleanAbandonedFanOuts(ctx, cutoff)
	require.NoError(t, err)
	assert.Equal(t, int64(0), cleaned)
	assertRetentionFanOutExists(t, s, "guard-waiting-fanout")
	assertRetentionFanOutExists(t, s, "guard-with-subjobs-fanout")
	assertRetentionFanOutExists(t, s, "guard-recent-fanout")
	assertRetentionFanOutExists(t, s, "guard-completed-fanout")
}

func TestDeleteConsumedSignalsOlderThan_PrunesOnlyConsumedOlderThanWindow(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	recent := time.Now().Add(-10 * time.Minute).UTC()

	seedRetentionSignal(t, s, "consumed-old", "job-a", "ctx", &old)
	seedRetentionSignal(t, s, "consumed-new", "job-a", "ctx", &recent)
	seedRetentionSignal(t, s, "pending-old", "job-a", "ctx", nil)

	deleted, err := s.DeleteConsumedSignalsOlderThan(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	assertRetentionSignalMissing(t, s, "consumed-old")
	assertRetentionSignalExists(t, s, "consumed-new")
	assertRetentionSignalExists(t, s, "pending-old")
}

func TestDeleteConsumedSignalsOlderThan_BatchLimitZeroWindowAndBoundary(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	insideWindow := time.Now().Add(-time.Hour + time.Second).UTC()

	seedRetentionSignal(t, s, "batch-1", "job-a", "ctx", &old)
	seedRetentionSignal(t, s, "batch-2", "job-a", "ctx", &old)
	seedRetentionSignal(t, s, "inside-window", "job-a", "ctx", &insideWindow)

	deleted, err := s.DeleteConsumedSignalsOlderThan(ctx, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	deleted, err = s.DeleteConsumedSignalsOlderThan(ctx, time.Hour, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assertRetentionSignalExists(t, s, "inside-window")

	deleted, err = s.DeleteConsumedSignalsOlderThan(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assertRetentionSignalExists(t, s, "inside-window")
}

func seedRetentionJob(t *testing.T, s *GormStorage, id string, status core.JobStatus, completedAt time.Time) {
	t.Helper()
	require.NoError(t, s.db.Create(&core.Job{
		ID:          retentionUUID(id),
		Type:        "retention.test",
		Queue:       "default",
		Status:      status,
		CompletedAt: &completedAt,
	}).Error)
}

func seedRetentionWorkflowJob(t *testing.T, s *GormStorage, id string, status core.JobStatus, completedAt time.Time, parentID, rootID, fanOutID *string) {
	t.Helper()
	job := &core.Job{
		ID:          retentionUUID(id),
		Type:        "retention.workflow",
		Queue:       "default",
		Status:      status,
		ParentJobID: ptrRetentionUUID(parentID),
		RootJobID:   ptrRetentionUUID(rootID),
		FanOutID:    ptrRetentionUUID(fanOutID),
	}
	if status == core.StatusCompleted || status == core.StatusFailed || status == core.StatusCancelled {
		job.CompletedAt = &completedAt
	}
	require.NoError(t, s.db.Create(job).Error)
}

func seedRetentionSignal(t *testing.T, s *GormStorage, id, jobID, name string, consumedAt *time.Time) {
	t.Helper()
	seedTestJob(t, context.Background(), s, retentionUUID(jobID), core.StatusCompleted)
	require.NoError(t, s.db.Create(&core.Signal{
		ID:         retentionUUID(id),
		JobID:      retentionUUID(jobID),
		Name:       name,
		Payload:    []byte(`"payload"`),
		ConsumedAt: consumedAt,
	}).Error)
}

func setFanOutCreatedAt(t *testing.T, s *GormStorage, id string, createdAt time.Time) {
	t.Helper()
	require.NoError(t, s.db.Model(&core.FanOut{}).
		Where("id = ?", retentionUUID(id)).
		Update("created_at", createdAt.UTC()).Error)
}

func ptrString(v string) *string {
	return &v
}

func retentionUUID(v string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(v)).String())
}

func ptrRetentionUUID(v *string) *core.UUID {
	if v == nil {
		return nil
	}
	u := retentionUUID(*v)
	return &u
}

func assertRetentionExists(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", retentionUUID(id)).Count(&count).Error)
	assert.Equal(t, int64(1), count, "job should exist: %s", id)
}

func assertRetentionSignalExists(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Signal{}).Where("id = ?", retentionUUID(id)).Count(&count).Error)
	assert.Equal(t, int64(1), count, "signal should exist: %s", id)
}

func assertRetentionFanOutExists(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.FanOut{}).Where("id = ?", retentionUUID(id)).Count(&count).Error)
	assert.Equal(t, int64(1), count, "fan-out should exist: %s", id)
}

func assertRetentionSignalMissing(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Signal{}).Where("id = ?", retentionUUID(id)).Count(&count).Error)
	assert.Equal(t, int64(0), count, "signal should be deleted: %s", id)
}

func assertRetentionFanOutMissing(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.FanOut{}).Where("id = ?", retentionUUID(id)).Count(&count).Error)
	assert.Equal(t, int64(0), count, "fan-out should be deleted: %s", id)
}

func assertRetentionMissing(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", retentionUUID(id)).Count(&count).Error)
	assert.Equal(t, int64(0), count, "job should be deleted: %s", id)
}
