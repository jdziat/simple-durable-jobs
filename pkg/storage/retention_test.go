package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
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
		ID:          id,
		Type:        "retention.test",
		Queue:       "default",
		Status:      status,
		CompletedAt: &completedAt,
	}).Error)
}

func seedRetentionSignal(t *testing.T, s *GormStorage, id, jobID, name string, consumedAt *time.Time) {
	t.Helper()
	require.NoError(t, s.db.Create(&core.Signal{
		ID:         id,
		JobID:      jobID,
		Name:       name,
		Payload:    []byte(`"payload"`),
		ConsumedAt: consumedAt,
	}).Error)
}

func assertRetentionExists(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", id).Count(&count).Error)
	assert.Equal(t, int64(1), count, "job should exist: %s", id)
}

func assertRetentionSignalExists(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Signal{}).Where("id = ?", id).Count(&count).Error)
	assert.Equal(t, int64(1), count, "signal should exist: %s", id)
}

func assertRetentionSignalMissing(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Signal{}).Where("id = ?", id).Count(&count).Error)
	assert.Equal(t, int64(0), count, "signal should be deleted: %s", id)
}

func assertRetentionMissing(t *testing.T, s *GormStorage, id string) {
	t.Helper()
	var count int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", id).Count(&count).Error)
	assert.Equal(t, int64(0), count, "job should be deleted: %s", id)
}
