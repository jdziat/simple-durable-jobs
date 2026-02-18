package jobs_test

import (
	"context"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPauseJob_Standalone(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test", Queue: "default"}
	_ = store.Enqueue(ctx, job)

	err := jobs.PauseJob(ctx, store, job.ID)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, job.ID)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestResumeJob_Standalone(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test", Queue: "default"}
	_ = store.Enqueue(ctx, job)
	_ = jobs.PauseJob(ctx, store, job.ID)

	err := jobs.ResumeJob(ctx, store, job.ID)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, job.ID)
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestPauseQueue_Standalone(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	err := jobs.PauseQueue(ctx, store, "emails")
	require.NoError(t, err)

	paused, err := jobs.IsQueuePaused(ctx, store, "emails")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestResumeQueue_Standalone(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	_ = jobs.PauseQueue(ctx, store, "emails")

	err := jobs.ResumeQueue(ctx, store, "emails")
	require.NoError(t, err)

	paused, err := jobs.IsQueuePaused(ctx, store, "emails")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestGetPausedJobs_Standalone(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test", Queue: "default"}
	_ = store.Enqueue(ctx, job)
	_ = jobs.PauseJob(ctx, store, job.ID)

	paused, err := jobs.GetPausedJobs(ctx, store, "default")
	require.NoError(t, err)
	assert.Len(t, paused, 1)
}

func TestGetPausedQueues_Standalone(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	_ = jobs.PauseQueue(ctx, store, "emails")
	_ = jobs.PauseQueue(ctx, store, "notifications")

	queues, err := jobs.GetPausedQueues(ctx, store)
	require.NoError(t, err)
	assert.Len(t, queues, 2)
}
