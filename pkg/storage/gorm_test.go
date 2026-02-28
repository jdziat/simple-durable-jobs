package storage

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// newTestStorage creates a fresh storage instance for each test.
// When TEST_DATABASE_URL is set it connects to PostgreSQL; otherwise
// it opens an in-memory SQLite database.  The database is fully
// migrated and ready for use.
func newTestStorage(t *testing.T) *GormStorage {
	t.Helper()
	db := openTestDB(t)

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

func TestSaveCheckpoint_UpsertsOnSameJobCallIndexCallType(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	// Save initial checkpoint.
	cp1 := &core.Checkpoint{
		JobID:     job.ID,
		CallIndex: -1,
		CallType:  "ocr_tiles",
		Result:    []byte(`{"results":{"0":{"text":"tile 0"}}}`),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp1))
	assert.NotEmpty(t, cp1.ID)

	// Save again with same (job_id, call_index, call_type) — should upsert, not insert.
	cp2 := &core.Checkpoint{
		JobID:     job.ID,
		CallIndex: -1,
		CallType:  "ocr_tiles",
		Result:    []byte(`{"results":{"0":{"text":"tile 0"},"1":{"text":"tile 1"}}}`),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp2))

	// Should be exactly 1 checkpoint, not 2.
	checkpoints, err := s.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	assert.Len(t, checkpoints, 1, "expected upsert to produce 1 row, not 2")

	// The result should be the updated value.
	assert.Equal(t, `{"results":{"0":{"text":"tile 0"},"1":{"text":"tile 1"}}}`, string(checkpoints[0].Result))
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

// ──────────────────────────────────────────────────────────────────────────────
// EnqueueBatch — additional edge cases
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueueBatch_SetsDefaultsForEachJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Jobs with no ID, Status, or Queue should get defaults filled in.
	jobs := []*core.Job{
		{Type: "task.a"},
		{Type: "task.b", Queue: "custom"},
		{Type: "task.c", Status: core.StatusWaiting},
	}
	require.NoError(t, s.EnqueueBatch(ctx, jobs))

	assert.NotEmpty(t, jobs[0].ID)
	assert.Equal(t, core.StatusPending, jobs[0].Status)
	assert.Equal(t, "default", jobs[0].Queue)

	assert.NotEmpty(t, jobs[1].ID)
	assert.Equal(t, core.StatusPending, jobs[1].Status)
	assert.Equal(t, "custom", jobs[1].Queue)

	// Pre-set status should be preserved.
	assert.Equal(t, core.StatusWaiting, jobs[2].Status)
}

func TestEnqueueBatch_PreservesExistingIDs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	customID := "my-batch-job-id"
	jobs := []*core.Job{
		{ID: customID, Type: "task.a"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, jobs))

	assert.Equal(t, customID, jobs[0].ID)

	got, err := s.GetJob(ctx, customID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, customID, got.ID)
}

// ──────────────────────────────────────────────────────────────────────────────
// PauseQueue — additional paths
// ──────────────────────────────────────────────────────────────────────────────

// TestPauseQueue_UnpauseThenRepause covers the "existing, paused=false → update"
// branch in PauseQueue where a QueueState record already exists but is not paused.
func TestPauseQueue_UnpauseThenRepause(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Pause then unpause to create a QueueState row with paused=false.
	require.NoError(t, s.PauseQueue(ctx, "repause-q"))
	require.NoError(t, s.UnpauseQueue(ctx, "repause-q"))

	// PauseQueue should now find the existing (unpaused) row and update it.
	require.NoError(t, s.PauseQueue(ctx, "repause-q"))

	paused, err := s.IsQueuePaused(ctx, "repause-q")
	require.NoError(t, err)
	assert.True(t, paused)
}

// ──────────────────────────────────────────────────────────────────────────────
// Complete — ErrJobNotOwned path with explicit job ID
// ──────────────────────────────────────────────────────────────────────────────

func TestComplete_ErrJobNotOwned_CorrectError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-A")
	require.NoError(t, err)
	require.NotNil(t, got)

	// Different worker tries to complete — must get ErrJobNotOwned.
	err = s.Complete(ctx, got.ID, "worker-B")
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrJobNotOwned)

	// The job should still be running (not completed).
	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusRunning, after.Status)
}

// ──────────────────────────────────────────────────────────────────────────────
// Fail — retry path sets run_at
// ──────────────────────────────────────────────────────────────────────────────

func TestFail_RetryAtSetsStatusToPendingAndRunAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	retryAt := time.Now().Add(10 * time.Minute)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-1", "transient", &retryAt))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusPending, after.Status, "job should be re-queued for retry")
	assert.NotNil(t, after.RunAt, "run_at should be set for scheduled retry")
	assert.Empty(t, after.LockedBy, "LockedBy should be cleared on retry")
}

// ──────────────────────────────────────────────────────────────────────────────
// Dequeue — all queues paused returns nil (distinct queue list path)
// ──────────────────────────────────────────────────────────────────────────────

func TestDequeue_MultipleQueuesBothPausedReturnsNil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("q1", "task.a")))
	require.NoError(t, s.Enqueue(ctx, newTestJob("q2", "task.b")))
	require.NoError(t, s.PauseQueue(ctx, "q1"))
	require.NoError(t, s.PauseQueue(ctx, "q2"))

	got, err := s.Dequeue(ctx, []string{"q1", "q2"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, got, "no job should be returned when all queues are paused")
}

// ──────────────────────────────────────────────────────────────────────────────
// GetDueJobs — all queues paused returns nil slice
// ──────────────────────────────────────────────────────────────────────────────

func TestGetDueJobs_AllQueuesPausedReturnsNil(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("only-q", "task.a")))
	require.NoError(t, s.PauseQueue(ctx, "only-q"))

	jobs, err := s.GetDueJobs(ctx, []string{"only-q"}, 10)
	require.NoError(t, err)
	assert.Nil(t, jobs, "all paused queues should return nil job slice from GetDueJobs")
}

// ──────────────────────────────────────────────────────────────────────────────
// CancelSubJobs — zero rows affected (no sub-jobs to cancel)
// ──────────────────────────────────────────────────────────────────────────────

func TestCancelSubJobs_NoSubJobsReturnsZero(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 2}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	// No sub-jobs were actually created for this fan-out.
	cancelled, err := s.CancelSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), cancelled)
}

// ──────────────────────────────────────────────────────────────────────────────
// CancelSubJob — job already completed is a no-op
// ──────────────────────────────────────────────────────────────────────────────

func TestCancelSubJob_AlreadyCompletedJobIsNoop(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 1}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	sub := &core.Job{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 0}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{sub}))

	// Dequeue and complete the sub-job first.
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.Complete(ctx, got.ID, "worker-1"))

	// Attempting to cancel a completed job should be a no-op (returns nil FanOut).
	fo, err := s.CancelSubJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Nil(t, fo, "cancelling an already-completed sub-job should return nil FanOut")
}

// ──────────────────────────────────────────────────────────────────────────────
// ResumeJob — decrement clamp: attempt of 0 stays 0
// ──────────────────────────────────────────────────────────────────────────────

func TestResumeJob_AttemptZeroDoesNotGoNegative(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Manually insert a waiting job with attempt=0 to exercise the CASE guard.
	job := &core.Job{
		Type:   "task.run",
		Queue:  "default",
		Status: core.StatusWaiting,
	}
	require.NoError(t, s.Enqueue(ctx, job))
	// Force status to waiting directly (Enqueue preserves the pre-set status).

	resumed, err := s.ResumeJob(ctx, job.ID)
	require.NoError(t, err)
	assert.True(t, resumed)

	after, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 0, after.Attempt, "attempt should not go below 0")
}

// ──────────────────────────────────────────────────────────────────────────────
// Dequeue — SQLite race-condition branches
// ──────────────────────────────────────────────────────────────────────────────

func TestDequeueSQLite_QueuePausedMidDequeue(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		t.Skip("SQLite-specific test — skipping on PostgreSQL")
	}
	// Tests the race-condition guard: queue paused between the initial check
	// and the inner transaction re-check (lines 218-224 in dequeueSQLite).
	ctx := context.Background()
	s := newTestStorage(t)

	// Enqueue a job, then pause its queue.
	job := newTestJob("emails", "send")
	require.NoError(t, s.Enqueue(ctx, job))

	// Pause the queue — this sets QueueState.Paused = true.
	require.NoError(t, s.PauseQueue(ctx, "emails"))

	// Dequeue with "emails" explicitly in the active list (bypassing the outer
	// filter by calling dequeueSQLite directly).
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute)
	got, err := s.dequeueSQLite(ctx, []string{"emails"}, "w1", now, lockUntil)
	require.NoError(t, err)
	assert.Nil(t, got, "should return nil because inner re-check finds queue paused")
}

func TestDequeueSQLite_NoPendingJobs(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		t.Skip("SQLite-specific test — skipping on PostgreSQL")
	}
	ctx := context.Background()
	s := newTestStorage(t)

	// No jobs enqueued at all.
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute)
	got, err := s.dequeueSQLite(ctx, []string{"default"}, "w1", now, lockUntil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDequeueSQLite_LockedJobSkipped(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		t.Skip("SQLite-specific test — skipping on PostgreSQL")
	}
	// A job that is already locked (locked_until in the future) should not be dequeued.
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task")
	require.NoError(t, s.Enqueue(ctx, job))

	// Dequeue it once to lock it.
	j1, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, j1)

	// Try to dequeue again — should find nothing (job is locked and running).
	j2, err := s.Dequeue(ctx, []string{"default"}, "w2")
	require.NoError(t, err)
	assert.Nil(t, j2, "locked job should not be dequeued by another worker")
}

// ──────────────────────────────────────────────────────────────────────────────
// Dequeue — paused queue filtering
// ──────────────────────────────────────────────────────────────────────────────

func TestDequeue_PartialQueuePause(t *testing.T) {
	// When one queue is paused and another isn't, only the active queue's jobs
	// should be dequeued.
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("paused-q", "task")))
	require.NoError(t, s.Enqueue(ctx, newTestJob("active-q", "task")))
	require.NoError(t, s.PauseQueue(ctx, "paused-q"))

	got, err := s.Dequeue(ctx, []string{"paused-q", "active-q"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "active-q", got.Queue)
}

// ──────────────────────────────────────────────────────────────────────────────
// UnpauseJob — default previous_status branch
// ──────────────────────────────────────────────────────────────────────────────

func TestUnpauseJob_DefaultsToPendingWhenNoPreviousStatus(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task")
	require.NoError(t, s.Enqueue(ctx, job))

	// Manually set status to paused WITHOUT setting previous_status, to exercise
	// the "default to pending" branch in UnpauseJob (line 792).
	s.db.Model(&core.Job{}).Where("id = ?", job.ID).Updates(map[string]any{
		"status":          core.StatusPaused,
		"previous_status": "",
	})

	require.NoError(t, s.UnpauseJob(ctx, job.ID))

	after, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, after.Status)
}

func TestUnpauseJob_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	err := s.UnpauseJob(ctx, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job not found")
}

// ──────────────────────────────────────────────────────────────────────────────
// SuspendJob — ownership and error paths
// ──────────────────────────────────────────────────────────────────────────────

func TestSuspendJob_Success(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "workflow")
	require.NoError(t, s.Enqueue(ctx, job))

	// Dequeue to lock it with a worker ID.
	got, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NoError(t, s.SuspendJob(ctx, got.ID, "w1"))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusWaiting, after.Status)
	assert.Empty(t, after.LockedBy)
}

// ──────────────────────────────────────────────────────────────────────────────
// CancelSubJob — cancellation of a pending sub-job with fan-out
// ──────────────────────────────────────────────────────────────────────────────

func TestCancelSubJob_CancelsPendingSubJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Create a fan-out.
	fo := &core.FanOut{
		ID:          "fo-cancel",
		ParentJobID: "parent-1",
		TotalCount:  2,
		Strategy:    core.StrategyCollectAll,
		Status:      core.FanOutPending,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo))

	// Create a pending sub-job linked to the fan-out.
	fanOutID := "fo-cancel"
	subJob := &core.Job{
		Type:     "step",
		Queue:    "default",
		Status:   core.StatusPending,
		FanOutID: &fanOutID,
	}
	require.NoError(t, s.Enqueue(ctx, subJob))

	result, err := s.CancelSubJob(ctx, subJob.ID)
	require.NoError(t, err)
	require.NotNil(t, result, "should return updated fan-out")
	assert.Equal(t, 1, result.CancelledCount)

	// Verify the job is now cancelled.
	after, err := s.GetJob(ctx, subJob.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCancelled, after.Status)
}

// ──────────────────────────────────────────────────────────────────────────────
// CancelSubJobs — with actual pending sub-jobs
// ──────────────────────────────────────────────────────────────────────────────

func TestCancelSubJobs_CancelsMultiplePending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	fo := &core.FanOut{
		ID:          "fo-multi",
		ParentJobID: "parent-1",
		TotalCount:  3,
		Strategy:    core.StrategyCollectAll,
		Status:      core.FanOutPending,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo))

	fanOutID := "fo-multi"
	for i := range 3 {
		j := &core.Job{
			Type:        "step",
			Queue:       "default",
			Status:      core.StatusPending,
			FanOutID:    &fanOutID,
			FanOutIndex: i,
		}
		require.NoError(t, s.Enqueue(ctx, j))
	}

	cancelled, err := s.CancelSubJobs(ctx, "fo-multi")
	require.NoError(t, err)
	assert.Equal(t, int64(3), cancelled)

	// Verify fan-out cancelled_count was updated.
	updated, err := s.GetFanOut(ctx, "fo-multi")
	require.NoError(t, err)
	assert.Equal(t, 3, updated.CancelledCount)
}

// ──────────────────────────────────────────────────────────────────────────────
// IncrementFanOut* — verify counters and returned fan-out
// ──────────────────────────────────────────────────────────────────────────────

func TestIncrementFanOutCancelled_ReturnsUpdatedFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	fo := &core.FanOut{
		ID:          "fo-inc-cancel",
		ParentJobID: "p1",
		TotalCount:  5,
		Strategy:    core.StrategyCollectAll,
		Status:      core.FanOutPending,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo))

	result, err := s.IncrementFanOutCancelled(ctx, "fo-inc-cancel")
	require.NoError(t, err)
	assert.Equal(t, 1, result.CancelledCount)

	result2, err := s.IncrementFanOutCancelled(ctx, "fo-inc-cancel")
	require.NoError(t, err)
	assert.Equal(t, 2, result2.CancelledCount)
}

// ──────────────────────────────────────────────────────────────────────────────
// UpdateFanOutStatus — already-completed idempotency
// ──────────────────────────────────────────────────────────────────────────────

func TestUpdateFanOutStatus_CompleteThenFail(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	fo := &core.FanOut{
		ID:          "fo-status",
		ParentJobID: "p1",
		TotalCount:  2,
		Strategy:    core.StrategyFailFast,
		Status:      core.FanOutPending,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo))

	// First update succeeds.
	ok, err := s.UpdateFanOutStatus(ctx, "fo-status", core.FanOutCompleted)
	require.NoError(t, err)
	assert.True(t, ok)

	// Second update should return false (already completed, not pending).
	ok2, err := s.UpdateFanOutStatus(ctx, "fo-status", core.FanOutFailed)
	require.NoError(t, err)
	assert.False(t, ok2)
}

// ──────────────────────────────────────────────────────────────────────────────
// Heartbeat — success path
// ──────────────────────────────────────────────────────────────────────────────

func TestHeartbeat_Success(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "long-task")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, got)

	beforeLock := got.LockedUntil

	// Small sleep so the new lock time is measurably different.
	time.Sleep(5 * time.Millisecond)

	require.NoError(t, s.Heartbeat(ctx, got.ID, "w1"))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after.LockedUntil)
	assert.True(t, after.LockedUntil.After(*beforeLock), "heartbeat should extend lock")
	assert.NotNil(t, after.LastHeartbeatAt)
}

// ──────────────────────────────────────────────────────────────────────────────
// GetDueJobs — with paused queue filtering
// ──────────────────────────────────────────────────────────────────────────────

func TestGetDueJobs_MixedPausedAndActive(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	past := time.Now().Add(-time.Hour)
	j1 := &core.Job{Type: "t", Queue: "paused-q", Status: core.StatusPending, RunAt: &past}
	j2 := &core.Job{Type: "t", Queue: "active-q", Status: core.StatusPending, RunAt: &past}
	require.NoError(t, s.Enqueue(ctx, j1))
	require.NoError(t, s.Enqueue(ctx, j2))
	require.NoError(t, s.PauseQueue(ctx, "paused-q"))

	jobs, err := s.GetDueJobs(ctx, []string{"paused-q", "active-q"}, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, "active-q", jobs[0].Queue)
}

// ──────────────────────────────────────────────────────────────────────────────
// IsJobPaused — not-found returns false
// ──────────────────────────────────────────────────────────────────────────────

func TestIsJobPaused_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	paused, err := s.IsJobPaused(ctx, "nonexistent")
	require.NoError(t, err)
	assert.False(t, paused)
}

// ──────────────────────────────────────────────────────────────────────────────
// IsQueuePaused — not-found returns false
// ──────────────────────────────────────────────────────────────────────────────

func TestIsQueuePaused_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	paused, err := s.IsQueuePaused(ctx, "never-paused")
	require.NoError(t, err)
	assert.False(t, paused)
}

// ──────────────────────────────────────────────────────────────────────────────
// UnpauseQueue — not paused returns error
// ──────────────────────────────────────────────────────────────────────────────

func TestUnpauseQueue_NeverPausedReturnsError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	err := s.UnpauseQueue(ctx, "never-paused")
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrQueueNotPaused))
}

// ──────────────────────────────────────────────────────────────────────────────
// RefreshQueueStates — with data
// ──────────────────────────────────────────────────────────────────────────────

func TestRefreshQueueStates_ReturnsAllStates(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.PauseQueue(ctx, "q1"))
	require.NoError(t, s.PauseQueue(ctx, "q2"))
	require.NoError(t, s.UnpauseQueue(ctx, "q2"))

	states, err := s.RefreshQueueStates(ctx)
	require.NoError(t, err)
	assert.True(t, states["q1"])
	assert.False(t, states["q2"])
}

// ──────────────────────────────────────────────────────────────────────────────
// GetPausedQueues — empty when none paused
// ──────────────────────────────────────────────────────────────────────────────

func TestGetPausedQueues_ReturnsEmptySlice(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	queues, err := s.GetPausedQueues(ctx)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

// ──────────────────────────────────────────────────────────────────────────────
// EnqueueUnique — defaults applied
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueueUnique_DefaultsApplied(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Enqueue with empty ID, status, queue — should get defaults.
	job := &core.Job{Type: "task"}
	require.NoError(t, s.EnqueueUnique(ctx, job, "unique-1"))

	assert.NotEmpty(t, job.ID)
	assert.Equal(t, core.StatusPending, job.Status)
	assert.Equal(t, "default", job.Queue)
	assert.Equal(t, "unique-1", job.UniqueKey)
}

// ──────────────────────────────────────────────────────────────────────────────
// PauseJob — pausing a waiting job
// ──────────────────────────────────────────────────────────────────────────────

func TestPauseJob_WaitingJobCanBePaused(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{Type: "wf", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.Enqueue(ctx, job))

	require.NoError(t, s.PauseJob(ctx, job.ID))

	after, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPaused, after.Status)
	assert.Equal(t, core.StatusWaiting, after.PreviousStatus)
}

// ──────────────────────────────────────────────────────────────────────────────
// Complete and Fail — success paths with correct field clearing
// ──────────────────────────────────────────────────────────────────────────────

func TestComplete_ClearsLockFields(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.NotEmpty(t, got.LockedBy)

	require.NoError(t, s.Complete(ctx, got.ID, "w1"))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCompleted, after.Status)
	assert.Empty(t, after.LockedBy)
	assert.Nil(t, after.LockedUntil)
	assert.NotNil(t, after.CompletedAt)
}

func TestFail_WithoutRetryAt_SetsCompletedAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)

	require.NoError(t, s.Fail(ctx, got.ID, "w1", "something broke", nil))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusFailed, after.Status)
	assert.NotNil(t, after.CompletedAt)
	assert.Empty(t, after.LockedBy)
}

// ──────────────────────────────────────────────────────────────────────────────
// GetFanOut — not found returns nil
// ──────────────────────────────────────────────────────────────────────────────

func TestGetFanOut_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	fo, err := s.GetFanOut(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, fo)
}

// ──────────────────────────────────────────────────────────────────────────────
// Error paths — use a closed database to trigger DB errors
// ──────────────────────────────────────────────────────────────────────────────

// closedStorage returns a GormStorage whose underlying SQL connection is closed.
// All DB operations will return errors, exercising the error-handling branches.
func closedStorage(t *testing.T) *GormStorage {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))

	sqlDB, err := db.DB()
	require.NoError(t, err)
	_ = sqlDB.Close()
	return s
}

func TestDequeue_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.Dequeue(context.Background(), []string{"default"}, "w1")
	assert.Error(t, err)
}

func TestComplete_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.Complete(context.Background(), "j1", "w1")
	assert.Error(t, err)
}

func TestFail_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.Fail(context.Background(), "j1", "w1", "oops", nil)
	assert.Error(t, err)
}

func TestHeartbeat_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.Heartbeat(context.Background(), "j1", "w1")
	assert.Error(t, err)
}

func TestReleaseStaleLocks_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.ReleaseStaleLocks(context.Background(), time.Hour)
	assert.Error(t, err)
}

func TestSaveCheckpoint_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.SaveCheckpoint(context.Background(), &core.Checkpoint{JobID: "j1", CallIndex: 0})
	assert.Error(t, err)
}

func TestGetCheckpoints_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetCheckpoints(context.Background(), "j1")
	assert.Error(t, err)
}

func TestGetJob_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetJob(context.Background(), "j1")
	assert.Error(t, err)
}

func TestGetJobsByStatus_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetJobsByStatus(context.Background(), core.StatusPending, 10)
	assert.Error(t, err)
}

func TestGetDueJobs_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetDueJobs(context.Background(), []string{"default"}, 10)
	assert.Error(t, err)
}

func TestIncrementFanOutCompleted_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.IncrementFanOutCompleted(context.Background(), "fo1")
	assert.Error(t, err)
}

func TestIncrementFanOutFailed_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.IncrementFanOutFailed(context.Background(), "fo1")
	assert.Error(t, err)
}

func TestIncrementFanOutCancelled_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.IncrementFanOutCancelled(context.Background(), "fo1")
	assert.Error(t, err)
}

func TestUpdateFanOutStatus_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.UpdateFanOutStatus(context.Background(), "fo1", core.FanOutCompleted)
	assert.Error(t, err)
}

func TestCreateFanOut_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.CreateFanOut(context.Background(), &core.FanOut{ID: "fo1", ParentJobID: "p1"})
	assert.Error(t, err)
}

func TestEnqueueBatch_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.EnqueueBatch(context.Background(), []*core.Job{{Type: "t"}})
	assert.Error(t, err)
}

func TestCancelSubJobs_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.CancelSubJobs(context.Background(), "fo1")
	assert.Error(t, err)
}

func TestCancelSubJob_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.CancelSubJob(context.Background(), "j1")
	assert.Error(t, err)
}

func TestSuspendJob_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.SuspendJob(context.Background(), "j1", "w1")
	assert.Error(t, err)
}

func TestResumeJob_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.ResumeJob(context.Background(), "j1")
	assert.Error(t, err)
}

func TestPauseJob_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.PauseJob(context.Background(), "j1")
	assert.Error(t, err)
}

func TestUnpauseJob_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.UnpauseJob(context.Background(), "j1")
	assert.Error(t, err)
}

func TestGetPausedJobs_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetPausedJobs(context.Background(), "default")
	assert.Error(t, err)
}

func TestIsJobPaused_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.IsJobPaused(context.Background(), "j1")
	assert.Error(t, err)
}

func TestPauseQueue_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.PauseQueue(context.Background(), "default")
	assert.Error(t, err)
}

func TestUnpauseQueue_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.UnpauseQueue(context.Background(), "default")
	assert.Error(t, err)
}

func TestGetPausedQueues_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetPausedQueues(context.Background())
	assert.Error(t, err)
}

func TestIsQueuePaused_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.IsQueuePaused(context.Background(), "default")
	assert.Error(t, err)
}

func TestRefreshQueueStates_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.RefreshQueueStates(context.Background())
	assert.Error(t, err)
}

func TestEnqueue_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.Enqueue(context.Background(), &core.Job{Type: "t"})
	assert.Error(t, err)
}

func TestEnqueueUnique_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.EnqueueUnique(context.Background(), &core.Job{Type: "t"}, "uk1")
	assert.Error(t, err)
}

func TestSaveJobResult_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.SaveJobResult(context.Background(), "j1", "w1", []byte("{}"))
	assert.Error(t, err)
}

func TestGetWaitingJobsToResume_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetWaitingJobsToResume(context.Background())
	assert.Error(t, err)
}

func TestGetFanOutsByParent_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetFanOutsByParent(context.Background(), "p1")
	assert.Error(t, err)
}

func TestGetSubJobs_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetSubJobs(context.Background(), "fo1")
	assert.Error(t, err)
}

func TestGetSubJobResults_DBError(t *testing.T) {
	s := closedStorage(t)
	_, err := s.GetSubJobResults(context.Background(), "fo1")
	assert.Error(t, err)
}

func TestDeleteCheckpoints_DBError(t *testing.T) {
	s := closedStorage(t)
	err := s.DeleteCheckpoints(context.Background(), "j1")
	assert.Error(t, err)
}

func TestMigrate_DBError(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	s := NewGormStorage(db)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	_ = sqlDB.Close()

	err = s.Migrate(context.Background())
	assert.Error(t, err)
}

// Note: ConfigurePool and NewGormStorageWithPool error paths require db.DB()
// to fail, which doesn't happen with GORM's SQLite driver (it caches the
// *sql.DB pointer). Those error branches are unreachable in SQLite tests.
