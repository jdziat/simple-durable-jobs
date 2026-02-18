package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// newTestStorage creates a fresh in-memory SQLite storage instance for each test.
// The database is fully migrated and ready for use.
func newTestStorage(t *testing.T) *GormStorage {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open in-memory sqlite")

	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()), "migrate schema")
	return s
}

// newTestJob builds a minimal valid Job for insertion in tests.
func newTestJob(queue, jobType string) *core.Job {
	return &core.Job{
		Type:  jobType,
		Queue: queue,
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Constructor / detection
// ──────────────────────────────────────────────────────────────────────────────

func TestNewGormStorage_IsSQLite(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := NewGormStorage(db)
	assert.True(t, s.IsSQLite(), "should detect SQLite dialect")
}

func TestNewGormStorage_DB(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := NewGormStorage(db)
	assert.Same(t, db, s.DB(), "DB() should return the same *gorm.DB passed in")
}

func TestNewGormStorage_NilDB(t *testing.T) {
	s := NewGormStorage(nil)
	assert.False(t, s.IsSQLite(), "nil db should not claim SQLite")
}

// ──────────────────────────────────────────────────────────────────────────────
// Enqueue
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueue_CreatesJobWithCorrectFields(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{
		Type:     "email.send",
		Queue:    "notifications",
		Priority: 5,
		Args:     []byte(`{"to":"user@example.com"}`),
	}

	require.NoError(t, s.Enqueue(ctx, job))

	assert.NotEmpty(t, job.ID, "ID should be auto-generated")
	assert.Equal(t, core.StatusPending, job.Status)
	assert.Equal(t, "notifications", job.Queue)
	assert.Equal(t, 5, job.Priority)
}

func TestEnqueue_DefaultsQueueToPending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{Type: "task.run"}
	require.NoError(t, s.Enqueue(ctx, job))

	assert.Equal(t, core.StatusPending, job.Status)
	assert.Equal(t, "default", job.Queue)
}

func TestEnqueue_PreservesExistingID(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{
		ID:   "my-custom-id",
		Type: "task.run",
	}
	require.NoError(t, s.Enqueue(ctx, job))
	assert.Equal(t, "my-custom-id", job.ID)
}

func TestEnqueue_PreservesExistingStatus(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{
		Type:   "task.run",
		Status: core.StatusWaiting,
	}
	require.NoError(t, s.Enqueue(ctx, job))
	assert.Equal(t, core.StatusWaiting, job.Status)
}

// ──────────────────────────────────────────────────────────────────────────────
// Dequeue
// ──────────────────────────────────────────────────────────────────────────────

func TestDequeue_ReturnsPendingJobAndSetsRunning(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got, "should return a job")

	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, core.StatusRunning, got.Status)
	assert.Equal(t, "worker-1", got.LockedBy)
	assert.NotNil(t, got.LockedUntil, "LockedUntil should be set")
	assert.NotNil(t, got.StartedAt, "StartedAt should be set")
	assert.Equal(t, 1, got.Attempt, "Attempt should be incremented to 1")
}

func TestDequeue_RespectsQueueFilter(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("emails", "email.send")))
	require.NoError(t, s.Enqueue(ctx, newTestJob("reports", "report.gen")))

	// Only dequeue from "emails" queue
	got, err := s.Dequeue(ctx, []string{"emails"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "emails", got.Queue)
}

func TestDequeue_ReturnsNilWhenNoJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, got, "empty queue should return nil")
}

func TestDequeue_ReturnsNilWhenAllQueuesPaused(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))
	require.NoError(t, s.PauseQueue(ctx, "default"))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, got, "should not dequeue from paused queue")
}

func TestDequeue_SkipsFutureScheduledJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	future := time.Now().Add(1 * time.Hour)
	job := &core.Job{
		Type:  "task.run",
		Queue: "default",
		RunAt: &future,
	}
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, got, "future scheduled job should not be dequeued yet")
}

func TestDequeue_PrioritisesHigherPriorityFirst(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	low := &core.Job{Type: "low", Queue: "default", Priority: 0}
	high := &core.Job{Type: "high", Queue: "default", Priority: 10}
	require.NoError(t, s.Enqueue(ctx, low))
	require.NoError(t, s.Enqueue(ctx, high))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "high", got.Type, "higher priority job should come first")
}

func TestDequeue_SkipsPausedJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.PauseJob(ctx, job.ID))

	// Enqueue another job that should be picked up
	job2 := newTestJob("default", "task.other")
	require.NoError(t, s.Enqueue(ctx, job2))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, job2.ID, got.ID, "should skip paused job and pick the other one")
}

// ──────────────────────────────────────────────────────────────────────────────
// Complete
// ──────────────────────────────────────────────────────────────────────────────

func TestComplete_SetsStatusToCompleted(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NoError(t, s.Complete(ctx, got.ID, "worker-1"))

	completed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, core.StatusCompleted, completed.Status)
	assert.Empty(t, completed.LockedBy)
	assert.Nil(t, completed.LockedUntil)
	assert.NotNil(t, completed.CompletedAt)
}

func TestComplete_FailsWhenWorkerDoesNotOwnJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	err = s.Complete(ctx, got.ID, "worker-2") // wrong worker
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrJobNotOwned))
}

// ──────────────────────────────────────────────────────────────────────────────
// Fail
// ──────────────────────────────────────────────────────────────────────────────

func TestFail_SetsStatusToFailed(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NoError(t, s.Fail(ctx, got.ID, "worker-1", "something broke", nil))

	failed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, core.StatusFailed, failed.Status)
	assert.Equal(t, "something broke", failed.LastError)
	assert.Empty(t, failed.LockedBy)
	assert.NotNil(t, failed.CompletedAt)
}

func TestFail_SchedulesRetryWhenRetryAtProvided(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	retryAt := time.Now().Add(5 * time.Minute)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-1", "transient error", &retryAt))

	retrying, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, retrying)
	// When retryAt is set, status goes back to pending (scheduled for retry)
	assert.Equal(t, core.StatusPending, retrying.Status)
	assert.NotNil(t, retrying.RunAt)
}

func TestFail_FailsWhenWorkerDoesNotOwnJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	err = s.Fail(ctx, got.ID, "wrong-worker", "error", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrJobNotOwned))
}

// ──────────────────────────────────────────────────────────────────────────────
// GetJob
// ──────────────────────────────────────────────────────────────────────────────

func TestGetJob_RetrievesById(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{
		ID:    "test-job-id",
		Type:  "email.send",
		Queue: "default",
	}
	require.NoError(t, s.Enqueue(ctx, job))

	found, err := s.GetJob(ctx, "test-job-id")
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, "test-job-id", found.ID)
	assert.Equal(t, "email.send", found.Type)
}

func TestGetJob_ReturnsNilForMissingJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	found, err := s.GetJob(ctx, "does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, found)
}

// ──────────────────────────────────────────────────────────────────────────────
// GetJobsByStatus
// ──────────────────────────────────────────────────────────────────────────────

func TestGetJobsByStatus_FiltersCorrectly(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Enqueue three jobs; dequeue and complete one; fail one
	j1 := newTestJob("default", "task.a")
	j2 := newTestJob("default", "task.b")
	j3 := newTestJob("default", "task.c")
	require.NoError(t, s.Enqueue(ctx, j1))
	require.NoError(t, s.Enqueue(ctx, j2))
	require.NoError(t, s.Enqueue(ctx, j3))

	d1, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, d1)
	require.NoError(t, s.Complete(ctx, d1.ID, "worker-1"))

	d2, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, d2)
	require.NoError(t, s.Fail(ctx, d2.ID, "worker-1", "boom", nil))

	// Two remaining pending jobs is j3 only (j1 completed, j2 failed)
	pending, err := s.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Len(t, pending, 1)

	completed, err := s.GetJobsByStatus(ctx, core.StatusCompleted, 100)
	require.NoError(t, err)
	assert.Len(t, completed, 1)

	failed, err := s.GetJobsByStatus(ctx, core.StatusFailed, 100)
	require.NoError(t, err)
	assert.Len(t, failed, 1)
}

func TestGetJobsByStatus_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	for i := 0; i < 5; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))
	}

	jobs, err := s.GetJobsByStatus(ctx, core.StatusPending, 3)
	require.NoError(t, err)
	assert.Len(t, jobs, 3)
}

// ──────────────────────────────────────────────────────────────────────────────
// Locking: Heartbeat + ReleaseStaleLocks
// ──────────────────────────────────────────────────────────────────────────────

func TestDequeue_SetsLockedByAndLockedUntil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	before := time.Now()
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-42")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, "worker-42", got.LockedBy)
	require.NotNil(t, got.LockedUntil)
	assert.True(t, got.LockedUntil.After(before), "LockedUntil should be in the future")
}

func TestHeartbeat_ExtendsLockedUntil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	originalLock := *got.LockedUntil

	// Small sleep to ensure the new lock is strictly after the original
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, s.Heartbeat(ctx, got.ID, "worker-1"))

	refreshed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	require.NotNil(t, refreshed.LockedUntil)
	assert.True(t, refreshed.LockedUntil.After(originalLock), "heartbeat should extend lock")
	assert.NotNil(t, refreshed.LastHeartbeatAt)
}

func TestHeartbeat_FailsWhenWorkerDoesNotOwnJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	err = s.Heartbeat(ctx, got.ID, "wrong-worker")
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrJobNotOwned))
}

func TestReleaseStaleLocks_ResetsExpiredRunningJobsToPending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	// Manually push locked_until into the past so the reaper can find it
	past := time.Now().Add(-2 * time.Hour)
	err = s.db.Model(&core.Job{}).
		Where("id = ?", got.ID).
		Update("locked_until", past).Error
	require.NoError(t, err)

	count, err := s.ReleaseStaleLocks(ctx, 1*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	released, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, released)
	assert.Equal(t, core.StatusPending, released.Status)
	assert.Empty(t, released.LockedBy)
	assert.Nil(t, released.LockedUntil)
}

func TestReleaseStaleLocks_DoesNotTouchFreshLocks(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	// staleDuration is 2 hours – fresh lock of 45 min should not be affected
	count, err := s.ReleaseStaleLocks(ctx, 2*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	still, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusRunning, still.Status)
}

// ──────────────────────────────────────────────────────────────────────────────
// Checkpoints
// ──────────────────────────────────────────────────────────────────────────────

func TestSaveCheckpoint_AndGetCheckpoints_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	cp1 := &core.Checkpoint{
		JobID:     job.ID,
		CallIndex: 0,
		CallType:  "http.get",
		Result:    []byte(`{"status":200}`),
	}
	cp2 := &core.Checkpoint{
		JobID:     job.ID,
		CallIndex: 1,
		CallType:  "db.query",
		Result:    []byte(`[1,2,3]`),
	}

	require.NoError(t, s.SaveCheckpoint(ctx, cp1))
	require.NoError(t, s.SaveCheckpoint(ctx, cp2))

	assert.NotEmpty(t, cp1.ID)
	assert.NotEmpty(t, cp2.ID)

	checkpoints, err := s.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 2)

	// Results should be ordered by call_index ASC
	assert.Equal(t, 0, checkpoints[0].CallIndex)
	assert.Equal(t, "http.get", checkpoints[0].CallType)
	assert.Equal(t, 1, checkpoints[1].CallIndex)
	assert.Equal(t, "db.query", checkpoints[1].CallType)
}

func TestSaveCheckpoint_PreservesExistingID(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	cp := &core.Checkpoint{
		ID:        "custom-cp-id",
		JobID:     job.ID,
		CallIndex: 0,
		CallType:  "http.get",
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp))
	assert.Equal(t, "custom-cp-id", cp.ID)
}

func TestGetCheckpoints_ReturnsEmptySliceForUnknownJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	checkpoints, err := s.GetCheckpoints(ctx, "no-such-job")
	require.NoError(t, err)
	assert.Empty(t, checkpoints)
}

func TestDeleteCheckpoints_RemovesAllForJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	for i := 0; i < 3; i++ {
		require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
			JobID:     job.ID,
			CallIndex: i,
			CallType:  "step",
		}))
	}

	require.NoError(t, s.DeleteCheckpoints(ctx, job.ID))

	checkpoints, err := s.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	assert.Empty(t, checkpoints)
}

func TestDeleteCheckpoints_OnlyDeletesForTargetJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job1 := newTestJob("default", "task.a")
	job2 := newTestJob("default", "task.b")
	require.NoError(t, s.Enqueue(ctx, job1))
	require.NoError(t, s.Enqueue(ctx, job2))

	require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
		JobID: job1.ID, CallIndex: 0, CallType: "step",
	}))
	require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
		JobID: job2.ID, CallIndex: 0, CallType: "step",
	}))

	require.NoError(t, s.DeleteCheckpoints(ctx, job1.ID))

	cp1, err := s.GetCheckpoints(ctx, job1.ID)
	require.NoError(t, err)
	assert.Empty(t, cp1)

	cp2, err := s.GetCheckpoints(ctx, job2.ID)
	require.NoError(t, err)
	assert.Len(t, cp2, 1, "job2 checkpoints should be untouched")
}

// ──────────────────────────────────────────────────────────────────────────────
// Pause / Unpause – Jobs
// ──────────────────────────────────────────────────────────────────────────────

func TestPauseJob_AndIsJobPaused(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	paused, err := s.IsJobPaused(ctx, job.ID)
	require.NoError(t, err)
	assert.False(t, paused)

	require.NoError(t, s.PauseJob(ctx, job.ID))

	paused, err = s.IsJobPaused(ctx, job.ID)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestPauseJob_StoresPreviousStatus(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.PauseJob(ctx, job.ID))

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, core.StatusPending, got.PreviousStatus, "previous status should be saved")
}

func TestPauseJob_AlreadyPausedReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.PauseJob(ctx, job.ID))

	err := s.PauseJob(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrJobAlreadyPaused))
}

func TestPauseJob_CannotPauseRunningJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	_, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)

	err = s.PauseJob(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrCannotPauseStatus))
}

func TestPauseJob_NotFoundReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	err := s.PauseJob(ctx, "ghost-job-id")
	require.Error(t, err)
}

func TestUnpauseJob_ClearsPause(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.PauseJob(ctx, job.ID))

	require.NoError(t, s.UnpauseJob(ctx, job.ID))

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, core.StatusPending, got.Status)
	assert.Empty(t, got.PreviousStatus)
}

func TestUnpauseJob_NotPausedReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	err := s.UnpauseJob(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrJobNotPaused))
}

func TestUnpauseJob_NotFoundReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	err := s.UnpauseJob(ctx, "ghost-job-id")
	require.Error(t, err)
}

func TestGetPausedJobs_ReturnsPausedJobIDs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	j1 := newTestJob("work", "task.a")
	j2 := newTestJob("work", "task.b")
	j3 := newTestJob("other", "task.c")
	require.NoError(t, s.Enqueue(ctx, j1))
	require.NoError(t, s.Enqueue(ctx, j2))
	require.NoError(t, s.Enqueue(ctx, j3))

	require.NoError(t, s.PauseJob(ctx, j1.ID))
	require.NoError(t, s.PauseJob(ctx, j2.ID))

	jobs, err := s.GetPausedJobs(ctx, "work")
	require.NoError(t, err)
	assert.Len(t, jobs, 2)

	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	assert.Contains(t, ids, j1.ID)
	assert.Contains(t, ids, j2.ID)
}

func TestIsJobPaused_ReturnsFalseForUnknownJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	paused, err := s.IsJobPaused(ctx, "no-such-job")
	require.NoError(t, err)
	assert.False(t, paused)
}

// ──────────────────────────────────────────────────────────────────────────────
// Pause / Unpause – Queues
// ──────────────────────────────────────────────────────────────────────────────

func TestPauseQueue_AndIsQueuePaused(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	paused, err := s.IsQueuePaused(ctx, "work")
	require.NoError(t, err)
	assert.False(t, paused)

	require.NoError(t, s.PauseQueue(ctx, "work"))

	paused, err = s.IsQueuePaused(ctx, "work")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestPauseQueue_AlreadyPausedReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.PauseQueue(ctx, "work"))

	err := s.PauseQueue(ctx, "work")
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrQueueAlreadyPaused))
}

func TestUnpauseQueue_ClearsPause(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.PauseQueue(ctx, "work"))
	require.NoError(t, s.UnpauseQueue(ctx, "work"))

	paused, err := s.IsQueuePaused(ctx, "work")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestUnpauseQueue_NotPausedReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	err := s.UnpauseQueue(ctx, "never-paused")
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrQueueNotPaused))
}

func TestGetPausedQueues_ReturnsPausedQueueNames(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.PauseQueue(ctx, "q1"))
	require.NoError(t, s.PauseQueue(ctx, "q2"))

	queues, err := s.GetPausedQueues(ctx)
	require.NoError(t, err)
	assert.Len(t, queues, 2)
	assert.Contains(t, queues, "q1")
	assert.Contains(t, queues, "q2")
}

func TestGetPausedQueues_EmptyWhenNonePaused(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	queues, err := s.GetPausedQueues(ctx)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

func TestDequeue_SkipsJobsInPausedQueues(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("paused-q", "task.run")))
	require.NoError(t, s.PauseQueue(ctx, "paused-q"))

	got, err := s.Dequeue(ctx, []string{"paused-q"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, got, "should not dequeue from paused queue")
}

func TestIsQueuePaused_ReturnsFalseForUnknownQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	paused, err := s.IsQueuePaused(ctx, "unknown-queue")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestRefreshQueueStates_ReturnsCorrectMap(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.PauseQueue(ctx, "q-paused"))
	// Create a queue state entry for "q-active" so it appears in the map
	require.NoError(t, s.PauseQueue(ctx, "q-active"))
	require.NoError(t, s.UnpauseQueue(ctx, "q-active"))

	states, err := s.RefreshQueueStates(ctx)
	require.NoError(t, err)
	assert.True(t, states["q-paused"])
	assert.False(t, states["q-active"])
}

// ──────────────────────────────────────────────────────────────────────────────
// Fan-Out
// ──────────────────────────────────────────────────────────────────────────────

func TestCreateFanOut_AndGetFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{
		ParentJobID: parent.ID,
		TotalCount:  5,
		Strategy:    core.StrategyCollectAll,
	}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))
	assert.NotEmpty(t, fanOut.ID)

	got, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, fanOut.ID, got.ID)
	assert.Equal(t, parent.ID, got.ParentJobID)
	assert.Equal(t, 5, got.TotalCount)
	assert.Equal(t, core.FanOutPending, got.Status)
}

func TestGetFanOut_ReturnsNilForMissing(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	got, err := s.GetFanOut(ctx, "no-such-fanout")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCreateFanOut_PreservesExistingID(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{
		ID:          "my-fanout-id",
		ParentJobID: parent.ID,
		TotalCount:  2,
	}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))
	assert.Equal(t, "my-fanout-id", fanOut.ID)
}

func TestIncrementFanOutCompleted(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	updated, err := s.IncrementFanOutCompleted(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.CompletedCount)

	updated, err = s.IncrementFanOutCompleted(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, updated.CompletedCount)
}

func TestIncrementFanOutFailed(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	updated, err := s.IncrementFanOutFailed(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.FailedCount)
}

func TestIncrementFanOutCancelled(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	updated, err := s.IncrementFanOutCancelled(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.CancelledCount)
}

func TestUpdateFanOutStatus_UpdatesFromPending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 1}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	updated, err := s.UpdateFanOutStatus(ctx, fanOut.ID, core.FanOutCompleted)
	require.NoError(t, err)
	assert.True(t, updated, "should have updated status from pending")

	got, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Equal(t, core.FanOutCompleted, got.Status)
}

func TestUpdateFanOutStatus_IdempotentWhenAlreadyCompleted(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 1}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	_, err := s.UpdateFanOutStatus(ctx, fanOut.ID, core.FanOutCompleted)
	require.NoError(t, err)

	// Second attempt: already completed, not pending, should return false
	updated, err := s.UpdateFanOutStatus(ctx, fanOut.ID, core.FanOutCompleted)
	require.NoError(t, err)
	assert.False(t, updated, "should not update a non-pending fan-out")
}

func TestGetFanOutsByParent(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fo1 := &core.FanOut{ParentJobID: parent.ID, TotalCount: 2}
	fo2 := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fo1))
	require.NoError(t, s.CreateFanOut(ctx, fo2))

	results, err := s.GetFanOutsByParent(ctx, parent.ID)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestGetSubJobs_ReturnsChildJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	sub1 := &core.Job{
		Type: "subtask.a", Queue: "default",
		FanOutID: &fanOut.ID, FanOutIndex: 0,
	}
	sub2 := &core.Job{
		Type: "subtask.b", Queue: "default",
		FanOutID: &fanOut.ID, FanOutIndex: 1,
	}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{sub1, sub2}))

	subs, err := s.GetSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	require.Len(t, subs, 2)
	assert.Equal(t, 0, subs[0].FanOutIndex)
	assert.Equal(t, 1, subs[1].FanOutIndex)
}

func TestGetSubJobResults_ReturnsCompletedAndFailed(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Use a dedicated queue so the parent job cannot accidentally be dequeued
	// when we call Dequeue for the sub-jobs.
	const subQueue = "sub-results-queue"

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	sub1 := &core.Job{Type: "sub", Queue: subQueue, FanOutID: &fanOut.ID, FanOutIndex: 0}
	sub2 := &core.Job{Type: "sub", Queue: subQueue, FanOutID: &fanOut.ID, FanOutIndex: 1}
	sub3 := &core.Job{Type: "sub", Queue: subQueue, FanOutID: &fanOut.ID, FanOutIndex: 2}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{sub1, sub2, sub3}))

	// Dequeue and complete sub1
	d1, err := s.Dequeue(ctx, []string{subQueue}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, d1)
	require.NoError(t, s.Complete(ctx, d1.ID, "worker-1"))

	// Dequeue and fail sub2
	d2, err := s.Dequeue(ctx, []string{subQueue}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, d2)
	require.NoError(t, s.Fail(ctx, d2.ID, "worker-1", "kaboom", nil))

	results, err := s.GetSubJobResults(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Len(t, results, 2, "completed + failed = 2 results")

	// sub3 is still pending – should not appear
	for _, r := range results {
		assert.NotEqual(t, core.StatusPending, r.Status)
	}
}

func TestSaveJobResult_AndRetrieveViaGetJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	result := []byte(`{"value":42}`)
	require.NoError(t, s.SaveJobResult(ctx, got.ID, "worker-1", result))

	refreshed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	assert.Equal(t, result, refreshed.Result)
}

// ──────────────────────────────────────────────────────────────────────────────
// EnqueueBatch
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueueBatch_InsertsMultipleJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	jobs := []*core.Job{
		{Type: "task.a", Queue: "default"},
		{Type: "task.b", Queue: "default"},
		{Type: "task.c", Queue: "default"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, jobs))

	for _, j := range jobs {
		assert.NotEmpty(t, j.ID)
		assert.Equal(t, core.StatusPending, j.Status)
	}

	all, err := s.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestEnqueueBatch_EmptySliceIsNoOp(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{}))

	all, err := s.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Empty(t, all)
}

// ──────────────────────────────────────────────────────────────────────────────
// Scheduling: GetDueJobs
// ──────────────────────────────────────────────────────────────────────────────

func TestGetDueJobs_ReturnsJobsWhereRunAtLteNow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	past := time.Now().Add(-1 * time.Minute)
	dueJob := &core.Job{
		Type:  "scheduled.task",
		Queue: "default",
		RunAt: &past,
	}
	require.NoError(t, s.Enqueue(ctx, dueJob))

	jobs, err := s.GetDueJobs(ctx, []string{"default"}, 100)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, dueJob.ID, jobs[0].ID)
}

func TestGetDueJobs_SkipsFutureJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	future := time.Now().Add(1 * time.Hour)
	futureJob := &core.Job{
		Type:  "future.task",
		Queue: "default",
		RunAt: &future,
	}
	require.NoError(t, s.Enqueue(ctx, futureJob))

	jobs, err := s.GetDueJobs(ctx, []string{"default"}, 100)
	require.NoError(t, err)
	assert.Empty(t, jobs, "future jobs should not be returned")
}

func TestGetDueJobs_ReturnsJobsWithNilRunAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// nil run_at means "run immediately"
	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "immediate.task")))

	jobs, err := s.GetDueJobs(ctx, []string{"default"}, 100)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

func TestGetDueJobs_SkipsPausedQueues(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("paused-q", "task.run")))
	require.NoError(t, s.PauseQueue(ctx, "paused-q"))

	jobs, err := s.GetDueJobs(ctx, []string{"paused-q"}, 100)
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestGetDueJobs_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	for i := 0; i < 10; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))
	}

	jobs, err := s.GetDueJobs(ctx, []string{"default"}, 3)
	require.NoError(t, err)
	assert.Len(t, jobs, 3)
}

// ──────────────────────────────────────────────────────────────────────────────
// Unique Jobs
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueueUnique_CreatesJobOnFirstCall(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{Type: "email.send", Queue: "default"}
	require.NoError(t, s.EnqueueUnique(ctx, job, "unique:email:user@example.com"))
	assert.NotEmpty(t, job.ID)
}

func TestEnqueueUnique_ReturnsErrDuplicateJobForSameKey(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job1 := &core.Job{Type: "email.send", Queue: "default"}
	require.NoError(t, s.EnqueueUnique(ctx, job1, "unique:email:user@example.com"))

	job2 := &core.Job{Type: "email.send", Queue: "default"}
	err := s.EnqueueUnique(ctx, job2, "unique:email:user@example.com")
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrDuplicateJob))
}

func TestEnqueueUnique_AllowsNewJobAfterCompletion(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const key = "unique:email:user@example.com"

	job1 := &core.Job{Type: "email.send", Queue: "default"}
	require.NoError(t, s.EnqueueUnique(ctx, job1, key))

	// Complete the job so the unique key slot opens up
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.Complete(ctx, got.ID, "worker-1"))

	// A new job with the same key should now succeed
	job2 := &core.Job{Type: "email.send", Queue: "default"}
	require.NoError(t, s.EnqueueUnique(ctx, job2, key))
	assert.NotEmpty(t, job2.ID)
	assert.NotEqual(t, job1.ID, job2.ID)
}

func TestEnqueueUnique_DifferentKeysCreateDifferentJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job1 := &core.Job{Type: "email.send", Queue: "default"}
	job2 := &core.Job{Type: "email.send", Queue: "default"}
	require.NoError(t, s.EnqueueUnique(ctx, job1, "key:a"))
	require.NoError(t, s.EnqueueUnique(ctx, job2, "key:b"))
	assert.NotEqual(t, job1.ID, job2.ID)
}

// ──────────────────────────────────────────────────────────────────────────────
// Suspend / Resume
// ──────────────────────────────────────────────────────────────────────────────

func TestSuspendJob_AndResumeJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NoError(t, s.SuspendJob(ctx, got.ID, "worker-1"))

	suspended, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusWaiting, suspended.Status)
	assert.Empty(t, suspended.LockedBy)

	resumed, err := s.ResumeJob(ctx, got.ID)
	require.NoError(t, err)
	assert.True(t, resumed)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, after.Status)
}

func TestSuspendJob_FailsWhenWorkerDoesNotOwnJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	err = s.SuspendJob(ctx, got.ID, "wrong-worker")
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrJobNotOwned))
}

func TestResumeJob_ReturnsFalseWhenNotWaiting(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	resumed, err := s.ResumeJob(ctx, job.ID)
	require.NoError(t, err)
	assert.False(t, resumed, "pending job should not resume (already pending)")
}

func TestResumeJob_DecrementsAttemptCounter(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 1, got.Attempt)

	require.NoError(t, s.SuspendJob(ctx, got.ID, "worker-1"))
	_, err = s.ResumeJob(ctx, got.ID)
	require.NoError(t, err)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	// Attempt should be decremented back to 0 so the next dequeue doesn't
	// consume a retry slot.
	assert.Equal(t, 0, after.Attempt)
}

// ──────────────────────────────────────────────────────────────────────────────
// CancelSubJobs / CancelSubJob
// ──────────────────────────────────────────────────────────────────────────────

func TestCancelSubJobs_CancelsPendingSubJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	subs := []*core.Job{
		{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 0},
		{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 1},
		{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 2},
	}
	require.NoError(t, s.EnqueueBatch(ctx, subs))

	cancelled, err := s.CancelSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), cancelled)

	for _, sub := range subs {
		got, err := s.GetJob(ctx, sub.ID)
		require.NoError(t, err)
		assert.Equal(t, core.StatusCancelled, got.Status)
	}
}

func TestCancelSubJob_CancelsSingleSubJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 2}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	sub := &core.Job{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 0}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{sub}))

	fo, err := s.CancelSubJob(ctx, sub.ID)
	require.NoError(t, err)
	require.NotNil(t, fo)
	assert.Equal(t, 1, fo.CancelledCount)

	got, err := s.GetJob(ctx, sub.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCancelled, got.Status)
}

func TestCancelSubJob_NonSubJobReturnsNil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Regular job with no FanOutID
	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	fo, err := s.CancelSubJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Nil(t, fo, "non-sub-job should return nil FanOut")
}

// ──────────────────────────────────────────────────────────────────────────────
// GetWaitingJobsToResume
// ──────────────────────────────────────────────────────────────────────────────

func TestGetWaitingJobsToResume_ReturnsJobsWithCompletedFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	// Dequeue and suspend the parent to put it in waiting state
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.SuspendJob(ctx, got.ID, "worker-1"))

	// Create a completed fan-out pointing at the parent
	fanOut := &core.FanOut{
		ParentJobID: parent.ID,
		TotalCount:  1,
		Status:      core.FanOutCompleted,
	}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	waiting, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)
	require.Len(t, waiting, 1)
	assert.Equal(t, parent.ID, waiting[0].ID)
}

func TestGetWaitingJobsToResume_DoesNotReturnPendingFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.SuspendJob(ctx, got.ID, "worker-1"))

	// Fan-out still pending – parent should NOT be in resume list
	fanOut := &core.FanOut{
		ParentJobID: parent.ID,
		TotalCount:  3,
		Status:      core.FanOutPending,
	}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	waiting, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)
	assert.Empty(t, waiting)
}
