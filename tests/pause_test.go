package jobs_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGormStorage_PauseJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	err = store.PauseJob(ctx, job.ID)
	require.NoError(t, err)

	paused, err := store.IsJobPaused(ctx, job.ID)
	require.NoError(t, err)
	assert.True(t, paused)

	updated, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusPaused, updated.Status)
}

func TestGormStorage_PauseJob_AlreadyPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	_ = store.Enqueue(ctx, job)
	_ = store.PauseJob(ctx, job.ID)

	err := store.PauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrJobAlreadyPaused)
}

func TestGormStorage_PauseJob_CannotPauseCompleted(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	_ = store.Enqueue(ctx, job)

	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)
	_ = store.Complete(ctx, job.ID, "worker-1")

	err := store.PauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrCannotPauseStatus)
}

func TestGormStorage_UnpauseJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	_ = store.Enqueue(ctx, job)
	_ = store.PauseJob(ctx, job.ID)

	err := store.UnpauseJob(ctx, job.ID)
	require.NoError(t, err)

	updated, _ := store.GetJob(ctx, job.ID)
	assert.Equal(t, jobs.StatusPending, updated.Status)
}

func TestGormStorage_UnpauseJob_NotPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	_ = store.Enqueue(ctx, job)

	err := store.UnpauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrJobNotPaused)
}

func TestGormStorage_GetPausedJobs(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job1 := &jobs.Job{Type: "job1", Queue: "emails"}
	job2 := &jobs.Job{Type: "job2", Queue: "emails"}
	job3 := &jobs.Job{Type: "job3", Queue: "other"}
	_ = store.Enqueue(ctx, job1)
	_ = store.Enqueue(ctx, job2)
	_ = store.Enqueue(ctx, job3)

	_ = store.PauseJob(ctx, job1.ID)
	_ = store.PauseJob(ctx, job3.ID)

	paused, err := store.GetPausedJobs(ctx, "emails")
	require.NoError(t, err)
	assert.Len(t, paused, 1)
	assert.Equal(t, job1.ID, paused[0].ID)
}

func TestGormStorage_IsJobPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	_ = store.Enqueue(ctx, job)

	paused, _ := store.IsJobPaused(ctx, job.ID)
	assert.False(t, paused)

	_ = store.PauseJob(ctx, job.ID)

	paused, _ = store.IsJobPaused(ctx, job.ID)
	assert.True(t, paused)
}

func TestGormStorage_PauseQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	err := store.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	paused, err := store.IsQueuePaused(ctx, "emails")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestGormStorage_PauseQueue_AlreadyPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	_ = store.PauseQueue(ctx, "emails")

	err := store.PauseQueue(ctx, "emails")
	assert.ErrorIs(t, err, jobs.ErrQueueAlreadyPaused)
}

func TestGormStorage_UnpauseQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	_ = store.PauseQueue(ctx, "emails")

	err := store.UnpauseQueue(ctx, "emails")
	require.NoError(t, err)

	paused, _ := store.IsQueuePaused(ctx, "emails")
	assert.False(t, paused)
}

func TestGormStorage_UnpauseQueue_NotPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	err := store.UnpauseQueue(ctx, "not-paused")
	assert.ErrorIs(t, err, jobs.ErrQueueNotPaused)
}

func TestGormStorage_GetPausedQueues(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	_ = store.PauseQueue(ctx, "emails")
	_ = store.PauseQueue(ctx, "notifications")

	queues, err := store.GetPausedQueues(ctx)
	require.NoError(t, err)
	assert.Len(t, queues, 2)
	assert.Contains(t, queues, "emails")
	assert.Contains(t, queues, "notifications")
}

func TestGormStorage_RefreshQueueStates(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	_ = store.PauseQueue(ctx, "emails")

	states, err := store.RefreshQueueStates(ctx)
	require.NoError(t, err)

	assert.True(t, states["emails"])
	assert.False(t, states["other"])
}

func TestGormStorage_Dequeue_SkipsPausedQueues(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create jobs in two queues
	job1 := &jobs.Job{Type: "job1", Queue: "emails"}
	job2 := &jobs.Job{Type: "job2", Queue: "other"}
	_ = store.Enqueue(ctx, job1)
	_ = store.Enqueue(ctx, job2)

	// Pause emails queue
	_ = store.PauseQueue(ctx, "emails")

	// Dequeue from both queues - should only get the "other" job
	dequeued, err := store.Dequeue(ctx, []string{"emails", "other"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, "other", dequeued.Queue)
}
