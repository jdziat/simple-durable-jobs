package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupStorageTest(t *testing.T) jobs.Storage {
	t.Helper()
	return openIntegrationStorage(t)
}

func TestGormStorage_Migrate(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&mode=memory"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	// Running migrate again should be idempotent
	err = store.Migrate(context.Background())
	require.NoError(t, err)
}

func TestGormStorage_Enqueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Args:  []byte(`{"key":"value"}`),
		Queue: "default",
	}

	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	assert.NotEmpty(t, job.ID)
	assert.Equal(t, jobs.StatusPending, job.Status)
}

func TestGormStorage_Enqueue_DefaultValues(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type: "test-job",
	}

	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	assert.NotEmpty(t, job.ID)
	assert.Equal(t, jobs.StatusPending, job.Status)
	assert.Equal(t, "default", job.Queue)
}

func TestGormStorage_Enqueue_PreserveID(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		ID:   "custom-id-123",
		Type: "test-job",
	}

	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	assert.Equal(t, "custom-id-123", job.ID)
}

func TestGormStorage_Dequeue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Enqueue a job
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue it
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	assert.Equal(t, job.ID, dequeued.ID)
	assert.Equal(t, jobs.StatusRunning, dequeued.Status)
	assert.Equal(t, "worker-1", dequeued.LockedBy)
	assert.NotNil(t, dequeued.LockedUntil)
	assert.NotNil(t, dequeued.StartedAt)
	assert.Equal(t, 1, dequeued.Attempt)
}

func TestGormStorage_Dequeue_EmptyQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, dequeued)
}

func TestGormStorage_Dequeue_RespectsRunAt(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Enqueue a job scheduled for the future
	future := time.Now().Add(1 * time.Hour)
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
		RunAt: &future,
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Should not dequeue it
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, dequeued)
}

func TestGormStorage_Dequeue_RespectsQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Enqueue to "other" queue
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "other",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue from "default" queue - should be empty
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, dequeued)

	// Dequeue from "other" queue - should get the job
	dequeued, err = store.Dequeue(ctx, []string{"other"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, job.ID, dequeued.ID)
}

func TestGormStorage_Dequeue_PriorityOrder(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Enqueue low priority first
	lowJob := &jobs.Job{
		Type:     "low-job",
		Queue:    "default",
		Priority: 1,
	}
	err := store.Enqueue(ctx, lowJob)
	require.NoError(t, err)

	// Enqueue high priority second
	highJob := &jobs.Job{
		Type:     "high-job",
		Queue:    "default",
		Priority: 100,
	}
	err = store.Enqueue(ctx, highJob)
	require.NoError(t, err)

	// Should dequeue high priority first
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, "high-job", dequeued.Type)
}

func TestGormStorage_Complete(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue (locks the job to worker-1)
	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)

	// Complete (must use same workerID)
	err = store.Complete(ctx, job.ID, "worker-1")
	require.NoError(t, err)

	// Verify
	completed, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusCompleted, completed.Status)
	assert.NotNil(t, completed.CompletedAt)
	assert.Empty(t, completed.LockedBy)
	assert.Nil(t, completed.LockedUntil)
}

func TestGormStorage_Fail_NoRetry(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue to lock the job
	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)

	// Fail without retry
	err = store.Fail(ctx, job.ID, "worker-1", "something went wrong", nil)
	require.NoError(t, err)

	// Verify
	failed, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusFailed, failed.Status)
	assert.Equal(t, "something went wrong", failed.LastError)
	assert.NotNil(t, failed.CompletedAt)
}

func TestGormStorage_Fail_WithRetry(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue to lock the job
	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)

	// Fail with retry
	retryAt := time.Now().Add(5 * time.Minute)
	err = store.Fail(ctx, job.ID, "worker-1", "temporary error", &retryAt)
	require.NoError(t, err)

	// Verify
	retrying, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusPending, retrying.Status)
	assert.Equal(t, "temporary error", retrying.LastError)
	assert.NotNil(t, retrying.RunAt)
}

func TestGormStorage_SaveCheckpoint(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create a job first
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Save checkpoint
	cp := &jobs.Checkpoint{
		JobID:     job.ID,
		CallIndex: 0,
		CallType:  "nested-call",
		Result:    []byte(`{"value":42}`),
	}
	err = store.SaveCheckpoint(ctx, cp)
	require.NoError(t, err)

	assert.NotEmpty(t, cp.ID)
}

func TestGormStorage_GetCheckpoints(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create a job
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Save multiple checkpoints
	for i := 0; i < 3; i++ {
		cp := &jobs.Checkpoint{
			JobID:     job.ID,
			CallIndex: i,
			CallType:  "nested-call",
			Result:    []byte(`{}`),
		}
		err = store.SaveCheckpoint(ctx, cp)
		require.NoError(t, err)
	}

	// Get checkpoints
	checkpoints, err := store.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	assert.Len(t, checkpoints, 3)

	// Verify order
	for i, cp := range checkpoints {
		assert.Equal(t, i, cp.CallIndex)
	}
}

func TestGormStorage_DeleteCheckpoints(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create a job
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Save checkpoints
	for i := 0; i < 3; i++ {
		cp := &jobs.Checkpoint{
			JobID:     job.ID,
			CallIndex: i,
			CallType:  "nested-call",
		}
		err = store.SaveCheckpoint(ctx, cp)
		require.NoError(t, err)
	}

	// Delete checkpoints
	err = store.DeleteCheckpoints(ctx, job.ID)
	require.NoError(t, err)

	// Verify deleted
	checkpoints, err := store.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	assert.Empty(t, checkpoints)
}

func TestGormStorage_GetDueJobs(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create jobs with different run_at times
	past := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(1 * time.Hour)

	pastJob := &jobs.Job{
		Type:  "past-job",
		Queue: "default",
		RunAt: &past,
	}
	err := store.Enqueue(ctx, pastJob)
	require.NoError(t, err)

	futureJob := &jobs.Job{
		Type:  "future-job",
		Queue: "default",
		RunAt: &future,
	}
	err = store.Enqueue(ctx, futureJob)
	require.NoError(t, err)

	nowJob := &jobs.Job{
		Type:  "now-job",
		Queue: "default",
	}
	err = store.Enqueue(ctx, nowJob)
	require.NoError(t, err)

	// Get due jobs
	dueJobs, err := store.GetDueJobs(ctx, []string{"default"}, 10)
	require.NoError(t, err)

	// Should get past and now jobs, not future
	assert.Len(t, dueJobs, 2)
	types := make([]string, len(dueJobs))
	for i, j := range dueJobs {
		types[i] = j.Type
	}
	assert.Contains(t, types, "past-job")
	assert.Contains(t, types, "now-job")
	assert.NotContains(t, types, "future-job")
}

func TestGormStorage_GetDueJobs_Limit(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create 5 jobs
	for i := 0; i < 5; i++ {
		job := &jobs.Job{
			Type:  "test-job",
			Queue: "default",
		}
		err := store.Enqueue(ctx, job)
		require.NoError(t, err)
	}

	// Get with limit of 2
	dueJobs, err := store.GetDueJobs(ctx, []string{"default"}, 2)
	require.NoError(t, err)
	assert.Len(t, dueJobs, 2)
}

func TestGormStorage_Heartbeat(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue to lock
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	require.NotNil(t, dequeued.LockedUntil)
	originalLockUntil := *dequeued.LockedUntil

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Heartbeat
	err = store.Heartbeat(ctx, job.ID, "worker-1")
	require.NoError(t, err)

	// Verify lock extended
	updated, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, updated)
	require.NotNil(t, updated.LockedUntil)
	assert.True(t, updated.LockedUntil.After(originalLockUntil))
}

func TestGormStorage_ReleaseStaleLocks(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create and dequeue a job
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)

	// Release stale locks with a very long duration (nothing should be released)
	count, err := store.ReleaseStaleLocks(ctx, 10*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// The job should still be running
	updated, _ := store.GetJob(ctx, job.ID)
	assert.Equal(t, jobs.StatusRunning, updated.Status)
}

func TestGormStorage_GetJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:     "test-job",
		Queue:    "default",
		Priority: 10,
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Get by ID
	retrieved, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, job.ID, retrieved.ID)
	assert.Equal(t, "test-job", retrieved.Type)
	assert.Equal(t, 10, retrieved.Priority)
}

func TestGormStorage_GetJob_NotFound(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job, err := store.GetJob(ctx, "non-existent-id")
	require.NoError(t, err)
	assert.Nil(t, job)
}

func TestGormStorage_GetJobsByStatus(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create jobs with different statuses
	pendingJob := &jobs.Job{
		Type:  "pending-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, pendingJob)
	require.NoError(t, err)

	// Dequeue to make it running
	runningJob := &jobs.Job{
		Type:  "running-job",
		Queue: "default",
	}
	err = store.Enqueue(ctx, runningJob)
	require.NoError(t, err)
	_, _ = store.Dequeue(ctx, []string{"default"}, "worker-1")

	// Get pending jobs
	pendingJobs, err := store.GetJobsByStatus(ctx, jobs.StatusPending, 10)
	require.NoError(t, err)
	assert.Len(t, pendingJobs, 1)

	// Get running jobs
	runningJobs, err := store.GetJobsByStatus(ctx, jobs.StatusRunning, 10)
	require.NoError(t, err)
	assert.Len(t, runningJobs, 1)
}

func TestGormStorage_GetJobsByStatus_Limit(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create 5 pending jobs
	for i := 0; i < 5; i++ {
		job := &jobs.Job{
			Type:  "test-job",
			Queue: "default",
		}
		err := store.Enqueue(ctx, job)
		require.NoError(t, err)
	}

	// Get with limit of 2
	pendingJobs, err := store.GetJobsByStatus(ctx, jobs.StatusPending, 2)
	require.NoError(t, err)
	assert.Len(t, pendingJobs, 2)
}
