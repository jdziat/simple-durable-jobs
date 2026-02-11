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
	store.Enqueue(ctx, job)
	store.PauseJob(ctx, job.ID)

	err := store.PauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrJobAlreadyPaused)
}

func TestGormStorage_PauseJob_CannotPauseCompleted(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	store.Enqueue(ctx, job)

	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)
	store.Complete(ctx, job.ID, "worker-1")

	err := store.PauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrCannotPauseStatus)
}

func TestGormStorage_UnpauseJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	store.Enqueue(ctx, job)
	store.PauseJob(ctx, job.ID)

	err := store.UnpauseJob(ctx, job.ID)
	require.NoError(t, err)

	updated, _ := store.GetJob(ctx, job.ID)
	assert.Equal(t, jobs.StatusPending, updated.Status)
}

func TestGormStorage_UnpauseJob_NotPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	store.Enqueue(ctx, job)

	err := store.UnpauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrJobNotPaused)
}

func TestGormStorage_GetPausedJobs(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job1 := &jobs.Job{Type: "job1", Queue: "emails"}
	job2 := &jobs.Job{Type: "job2", Queue: "emails"}
	job3 := &jobs.Job{Type: "job3", Queue: "other"}
	store.Enqueue(ctx, job1)
	store.Enqueue(ctx, job2)
	store.Enqueue(ctx, job3)

	store.PauseJob(ctx, job1.ID)
	store.PauseJob(ctx, job3.ID)

	paused, err := store.GetPausedJobs(ctx, "emails")
	require.NoError(t, err)
	assert.Len(t, paused, 1)
	assert.Equal(t, job1.ID, paused[0].ID)
}

func TestGormStorage_IsJobPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	store.Enqueue(ctx, job)

	paused, _ := store.IsJobPaused(ctx, job.ID)
	assert.False(t, paused)

	store.PauseJob(ctx, job.ID)

	paused, _ = store.IsJobPaused(ctx, job.ID)
	assert.True(t, paused)
}
