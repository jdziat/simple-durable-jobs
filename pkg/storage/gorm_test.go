package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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

// newConcurrentTestStorage returns a storage whose underlying DB can be
// accessed from multiple goroutines at once. Plain openTestDB on SQLite
// uses ":memory:" which gives each pooled connection its own isolated
// database — useless for concurrency tests. When TEST_DATABASE_URL /
// TEST_MYSQL_URL are set we fall through to the standard helper because
// those backends already share state across connections.
//
// On default SQLite we use a per-test temp file in WAL journal mode so
// multiple connections can perform reads and writes concurrently without
// tripping the file-level writer lock.
func newConcurrentTestStorage(t *testing.T) *GormStorage {
	t.Helper()

	if os.Getenv("TEST_DATABASE_URL") != "" || os.Getenv("TEST_MYSQL_URL") != "" {
		return newTestStorage(t)
	}

	dbFile := t.TempDir() + "/concurrent.db"
	dsn := "file:" + dbFile + "?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open wal-mode sqlite")

	// Apply pragmas explicitly too — some builds of the SQLite driver do
	// not respect all DSN parameters, and without a long busy_timeout
	// concurrent writers trip SQLITE_BUSY the moment the writer lock is
	// held by another transaction.
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("PRAGMA busy_timeout=10000").Error)
	require.NoError(t, db.Exec("PRAGMA synchronous=NORMAL").Error)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	// Multiple connections against the WAL DB can read concurrently; writes
	// serialize at the file level but are fast enough that the TOCTOU window
	// between the dedup SELECT and Create in EnqueueBatch is still
	// observable when the fix is missing.
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = sqlDB.Close() })

	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	return s
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

func TestMigrate_DequeueIndexExists(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	s := NewGormStorage(db)

	require.NoError(t, s.Migrate(ctx))
	require.NoError(t, s.Migrate(ctx), "migrate should be idempotent")

	var indexName string
	switch {
	case s.IsSQLite():
		require.NoError(t, s.DB().Raw(
			"SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
			"idx_jobs_dequeue",
		).Scan(&indexName).Error)
	case strings.EqualFold(s.DB().Name(), "postgres"):
		require.NoError(t, s.DB().Raw(
			"SELECT indexname FROM pg_indexes WHERE tablename = ? AND indexname = ?",
			"jobs", "idx_jobs_dequeue",
		).Scan(&indexName).Error)
	case strings.EqualFold(s.DB().Name(), "mysql"):
		require.NoError(t, s.DB().Raw(
			"SELECT index_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? LIMIT 1",
			"jobs", "idx_jobs_dequeue",
		).Scan(&indexName).Error)
	default:
		t.Fatalf("unsupported test dialect %q", s.DB().Name())
	}
	assert.Equal(t, "idx_jobs_dequeue", indexName)
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

func TestJobTimeoutDeterminism_RoundTripZero(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	zero := &core.Job{
		Type:        "task.zero",
		Timeout:     0,
		Determinism: 0,
	}
	require.NoError(t, s.Enqueue(ctx, zero))

	zeroReloaded, err := s.GetJob(ctx, zero.ID)
	require.NoError(t, err)
	require.NotNil(t, zeroReloaded)
	assert.Equal(t, time.Duration(0), zeroReloaded.Timeout)
	assert.Equal(t, 0, zeroReloaded.Determinism)

	nonZero := &core.Job{
		Type:        "task.nonzero",
		Timeout:     30 * time.Second,
		Determinism: 2,
	}
	require.NoError(t, s.Enqueue(ctx, nonZero))

	nonZeroReloaded, err := s.GetJob(ctx, nonZero.ID)
	require.NoError(t, err)
	require.NotNil(t, nonZeroReloaded)
	assert.Equal(t, 30*time.Second, nonZeroReloaded.Timeout)
	assert.Equal(t, 2, nonZeroReloaded.Determinism)
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

func TestDequeue_PreservesPayloadColumns(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	args := []byte(`{"large":"args"}`)
	result := []byte(`{"previous":"result"}`)
	traceContext := []byte(`trace-context`)
	job := &core.Job{
		Type:         "task.run",
		Queue:        "default",
		Args:         args,
		Result:       result,
		TraceContext: traceContext,
	}
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, args, got.Args)
	assert.Equal(t, result, got.Result)
	assert.Equal(t, traceContext, got.TraceContext)

	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, args, row.Args)
	assert.Equal(t, result, row.Result)
	assert.Equal(t, traceContext, row.TraceContext)
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

func TestHeartbeat_FailsWhenJobIsNoLongerRunning(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	before := *got.LockedUntil
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", got.ID).
		Update("status", core.StatusCompleted).Error)

	err = s.Heartbeat(ctx, got.ID, "worker-1")
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.NotNil(t, after.LockedUntil)
	assert.WithinDuration(t, before, *after.LockedUntil, time.Millisecond)
	assert.Nil(t, after.LastHeartbeatAt)
}

func TestRelease_ReturnsRunningJobToPending(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, 1, got.Attempt)

	require.NoError(t, s.Release(ctx, got.ID, "worker-1"))

	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusPending, row.Status)
	assert.Empty(t, row.LockedBy)
	assert.Nil(t, row.LockedUntil)
	assert.Nil(t, row.StartedAt)
	assert.Equal(t, 0, row.Attempt)
}

func TestRelease_NotOwned_ReturnsErrJobNotOwned(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	beforeLock := got.LockedUntil
	beforeStarted := got.StartedAt

	err = s.Release(ctx, got.ID, "worker-2")
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusRunning, row.Status)
	assert.Equal(t, "worker-1", row.LockedBy)
	require.NotNil(t, row.LockedUntil)
	require.NotNil(t, row.StartedAt)
	assert.WithinDuration(t, *beforeLock, *row.LockedUntil, time.Millisecond)
	assert.WithinDuration(t, *beforeStarted, *row.StartedAt, time.Millisecond)
	assert.Equal(t, 1, row.Attempt)

	require.NoError(t, s.Complete(ctx, got.ID, "worker-1"))
	err = s.Release(ctx, got.ID, "worker-1")
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	completed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, core.StatusCompleted, completed.Status)
	assert.Equal(t, 1, completed.Attempt)
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

	released, err := s.ReleaseStaleLocks(ctx, 1*time.Hour)
	require.NoError(t, err)
	require.Len(t, released, 1, "should report the released job's ID")
	assert.Equal(t, got.ID, released[0])

	row, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusPending, row.Status)
	assert.Empty(t, row.LockedBy)
	assert.Nil(t, row.LockedUntil)
}

func TestReleaseStaleLocks_ReapedJobIsOrphanedAndDequeuable(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	past := time.Now().Add(-2 * time.Hour)
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", got.ID).
		Update("locked_until", past).Error)

	released, err := s.ReleaseStaleLocks(ctx, time.Hour)
	require.NoError(t, err)
	require.Equal(t, []string{got.ID}, released)

	orphaned, err := s.FindOrphanedJobs(ctx, []string{got.ID}, "worker-1")
	require.NoError(t, err)
	require.Equal(t, []string{got.ID}, orphaned)

	reacquired, err := s.Dequeue(ctx, []string{"default"}, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, reacquired)
	assert.Equal(t, got.ID, reacquired.ID)
	assert.Equal(t, core.StatusRunning, reacquired.Status)
	assert.Equal(t, "worker-2", reacquired.LockedBy)
}

func TestReleaseStaleLocks_DoesNotTouchFreshLocks(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	// staleDuration is 2 hours – fresh lock of 45 min should not be affected
	released, err := s.ReleaseStaleLocks(ctx, 2*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, released, "fresh locks should not be released")

	still, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusRunning, still.Status)
}

func TestReleaseStaleLocks_LimitsBatch(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	past := time.Now().Add(-2 * time.Hour)
	for i := 0; i < maxResumeBatch+25; i++ {
		job := &core.Job{
			Type:        "task.stale",
			Queue:       "default",
			Status:      core.StatusRunning,
			LockedBy:    "worker-stale",
			LockedUntil: &past,
		}
		require.NoError(t, s.Enqueue(ctx, job))
	}

	released, err := s.ReleaseStaleLocks(ctx, time.Hour)
	require.NoError(t, err)
	assert.Len(t, released, maxResumeBatch)
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

func TestSaveCheckpoint_PreservesCreatedAtOnResave(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	cp1 := &core.Checkpoint{
		JobID:           job.ID,
		CallIndex:       7,
		CallType:        "step",
		Result:          []byte(`{"value":"first"}`),
		Error:           "first error",
		ErrorKind:       "transient",
		ErrorDelayNanos: int64(time.Second),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp1))

	checkpoints, err := s.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	createdAt := checkpoints[0].CreatedAt

	time.Sleep(time.Millisecond)

	cp2 := &core.Checkpoint{
		JobID:           job.ID,
		CallIndex:       7,
		CallType:        "step",
		Result:          []byte(`{"value":"second"}`),
		Error:           "second error",
		ErrorKind:       "permanent",
		ErrorDelayNanos: int64(2 * time.Second),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp2))

	checkpoints, err = s.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	assert.True(t, checkpoints[0].CreatedAt.Equal(createdAt), "created_at should remain the first creation time")
	assert.Equal(t, `{"value":"second"}`, string(checkpoints[0].Result))
	assert.Equal(t, "second error", checkpoints[0].Error)
	assert.Equal(t, "permanent", checkpoints[0].ErrorKind)
	assert.Equal(t, int64(2*time.Second), checkpoints[0].ErrorDelayNanos)
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

func TestPauseJob_CancelsRunningJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	_, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)

	// Running jobs should be cancelled (not paused)
	err = s.PauseJob(ctx, job.ID)
	require.NoError(t, err)

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCancelled, got.Status)
	assert.Equal(t, "cancelled by user", got.LastError)
	assert.NotNil(t, got.CompletedAt)
	assert.Empty(t, got.LockedBy)
	assert.Nil(t, got.LockedUntil)
}

func TestComplete_DoesNotOverwriteCancelledJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	_, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NoError(t, s.PauseJob(ctx, job.ID))

	err = s.Complete(ctx, job.ID, "worker-1")
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, core.StatusCancelled, got.Status)
}

func TestFail_DoesNotOverwriteCancelledJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	_, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NoError(t, s.PauseJob(ctx, job.ID))

	err = s.Fail(ctx, job.ID, "worker-1", "context canceled", nil)
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, core.StatusCancelled, got.Status)
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

func p2bID(t *testing.T, suffix string) string {
	t.Helper()
	// Must fit the varchar(36) id columns (Job.ID, FanOut.ID/ParentJobID). The
	// previous t.Name() prefix overflowed on Postgres/MySQL ("value too long for
	// type character varying(36)", SQLSTATE 22001); SQLite does not enforce
	// varchar length so it masked the bug. suffix ("fo"/"parent"/"job") + UnixNano
	// is ~26 chars, unique across shared-DB runs, and distinct within a call.
	return fmt.Sprintf("%s-%d", suffix, time.Now().UnixNano())
}

func createRunningP2BJob(t *testing.T, ctx context.Context, s *GormStorage, fanOutID *string, workerID string) *core.Job {
	t.Helper()
	lockUntil := time.Now().Add(time.Hour)
	job := &core.Job{
		ID:          p2bID(t, "job"),
		Type:        "p2b.sub",
		Queue:       "p2b",
		Status:      core.StatusRunning,
		LockedBy:    workerID,
		LockedUntil: &lockUntil,
		FanOutID:    fanOutID,
	}
	require.NoError(t, s.Enqueue(ctx, job))
	return job
}

func createP2BFanOut(t *testing.T, ctx context.Context, s *GormStorage, status core.FanOutStatus) *core.FanOut {
	t.Helper()
	fo := &core.FanOut{
		ID:          p2bID(t, "fo"),
		ParentJobID: p2bID(t, "parent"),
		TotalCount:  2,
		Strategy:    core.StrategyCollectAll,
		Status:      status,
	}
	require.NoError(t, s.CreateFanOut(ctx, fo))
	return fo
}

func TestCompleteWithResult_AtomicIncrementOnce(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")
	result := []byte(`{"ok":true}`)

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-1", result)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.CompletedCount)
	assert.Equal(t, 0, updated.FailedCount)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusCompleted, row.Status)
	assert.Equal(t, result, row.Result)
	assert.Empty(t, row.LockedBy)
	assert.Nil(t, row.LockedUntil)
	assert.NotNil(t, row.CompletedAt)

	updated, err = s.CompleteWithResult(ctx, job.ID, "worker-1", result)
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Nil(t, updated)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 1, after.CompletedCount)
}

func TestCompleteWithResult_ConcurrentWithReaper(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")
	result := []byte(`{"ok":true}`)

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-1", result)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.CompletedCount)

	released, err := s.ReleaseStaleLocks(ctx, 0)
	require.NoError(t, err)
	assert.Empty(t, released)

	updated, err = s.CompleteWithResult(ctx, job.ID, "worker-1", []byte(`{"ok":true}`))
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Nil(t, updated)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 1, after.CompletedCount)
	assert.Equal(t, core.FanOutPending, after.Status)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusCompleted, row.Status)
	assert.Equal(t, result, row.Result)
}

func TestCompleteWithResult_NotOwned_NoIncrement(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-2", []byte(`{"wrong":true}`))
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Nil(t, updated)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusRunning, row.Status)
	assert.Equal(t, "worker-1", row.LockedBy)
	assert.Empty(t, row.Result)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 0, after.CompletedCount)
}

func TestCompleteWithResult_NonFanout(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	job := createRunningP2BJob(t, ctx, s, nil, "worker-1")
	result := []byte(`{"standalone":true}`)

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-1", result)
	require.NoError(t, err)
	assert.Nil(t, updated)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusCompleted, row.Status)
	assert.Equal(t, result, row.Result)
}

func TestCompleteWithResult_LivenessGuard_NoCountOnTerminalFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutFailed)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")

	updated, err := s.CompleteWithResult(ctx, job.ID, "worker-1", []byte(`{"late":true}`))
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, core.FanOutFailed, updated.Status)
	assert.Equal(t, 0, updated.CompletedCount)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusCompleted, row.Status)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 0, after.CompletedCount)
}

func TestFailTerminalWithResult_AtomicIncrementOnce(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")

	updated, err := s.FailTerminalWithResult(ctx, job.ID, "worker-1", "something broke")
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.FailedCount)
	assert.Equal(t, 0, updated.CompletedCount)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusFailed, row.Status)
	assert.Equal(t, "something broke", row.LastError)
	assert.Empty(t, row.LockedBy)
	assert.Nil(t, row.LockedUntil)
	assert.NotNil(t, row.CompletedAt)

	updated, err = s.FailTerminalWithResult(ctx, job.ID, "worker-1", "again")
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Nil(t, updated)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 1, after.FailedCount)
}

func TestFailTerminalWithResult_NotOwned_NoIncrement(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")

	updated, err := s.FailTerminalWithResult(ctx, job.ID, "worker-2", "wrong worker")
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.Nil(t, updated)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusRunning, row.Status)
	assert.Equal(t, "worker-1", row.LockedBy)
	assert.Empty(t, row.LastError)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 0, after.FailedCount)
}

func TestFailTerminalWithResult_NonFanout(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	job := createRunningP2BJob(t, ctx, s, nil, "worker-1")

	updated, err := s.FailTerminalWithResult(ctx, job.ID, "worker-1", "standalone failed")
	require.NoError(t, err)
	assert.Nil(t, updated)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusFailed, row.Status)
	assert.Equal(t, "standalone failed", row.LastError)
}

func TestFailTerminalWithResult_LivenessGuard_NoCountOnTerminalFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	fo := createP2BFanOut(t, ctx, s, core.FanOutFailed)
	job := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")

	updated, err := s.FailTerminalWithResult(ctx, job.ID, "worker-1", "late fail")
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, core.FanOutFailed, updated.Status)
	assert.Equal(t, 0, updated.FailedCount)

	row, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, core.StatusFailed, row.Status)

	after, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, 0, after.FailedCount)
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

func TestSaveJobResult_DoesNotOverwriteCompletedJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.run")
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	original := []byte(`{"value":"original"}`)
	require.NoError(t, s.SaveJobResult(ctx, got.ID, "worker-1", original))
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", got.ID).
		Update("status", core.StatusCompleted).Error)

	err = s.SaveJobResult(ctx, got.ID, "worker-1", []byte(`{"value":"overwrite"}`))
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	refreshed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	assert.Equal(t, original, refreshed.Result)
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

func TestEnqueueUnique_Concurrent_NoDuplicates(t *testing.T) {
	ctx := context.Background()
	s := newConcurrentTestStorage(t)

	const concurrency = 20
	const key = "unique:concurrent:sqlite"

	start := make(chan struct{})
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job := &core.Job{Type: "email.send", Queue: "default"}
			<-start
			errs <- s.EnqueueUnique(ctx, job, key)
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	successes := 0
	duplicates := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, core.ErrDuplicateJob):
			duplicates++
		default:
			require.NoError(t, err)
		}
	}

	assert.Equal(t, 1, successes, "exactly one concurrent enqueue should succeed")
	assert.Equal(t, concurrency-1, duplicates, "all remaining enqueues should report duplicates")

	var count int64
	require.NoError(t, s.DB().Model(&core.Job{}).
		Where("unique_key = ?", key).
		Count(&count).Error)
	assert.EqualValues(t, 1, count, "concurrent EnqueueUnique produced duplicate rows")
}

func TestClaimScheduledFire_ConcurrentExactlyOnceAndTimeOrdering(t *testing.T) {
	ctx := context.Background()
	s := newConcurrentTestStorage(t)

	const concurrency = 20
	const name = "daily-report"
	fireTime := time.Now().UTC().Truncate(time.Millisecond)

	start := make(chan struct{})
	results := make(chan bool, concurrency)
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed, err := s.ClaimScheduledFire(ctx, name, fireTime)
			if err != nil {
				errs <- err
				return
			}
			results <- claimed
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	claims := 0
	for claimed := range results {
		if claimed {
			claims++
		}
	}
	assert.Equal(t, 1, claims, "exactly one caller should claim a schedule boundary")

	claimed, err := s.ClaimScheduledFire(ctx, name, fireTime)
	require.NoError(t, err)
	assert.False(t, claimed, "equal fire time should not claim again")

	claimed, err = s.ClaimScheduledFire(ctx, name, fireTime.Add(-time.Second))
	require.NoError(t, err)
	assert.False(t, claimed, "earlier fire time should not claim")

	later := fireTime.Add(time.Second)
	claimed, err = s.ClaimScheduledFire(ctx, name, later)
	require.NoError(t, err)
	assert.True(t, claimed, "strictly later fire time should claim")

	claimed, err = s.ClaimScheduledFire(ctx, name, later)
	require.NoError(t, err)
	assert.False(t, claimed, "later boundary should also be claimed only once")
}

func TestGetScheduledFireTime(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Use schedule names unique to this test. The external PG/MySQL test DBs are
	// shared across runs and scheduled_fires is never truncated (the integration
	// cleanup omits it), so reusing a name shared with another test
	// (e.g. "daily-report" from TestClaimScheduledFire_*) leaves a row whose
	// monotonic last_fire_at would refuse this test's claim. Scope by name and
	// clear any residue first so the assertions are deterministic on a shared DB.
	const missingName = "getfire-missing-schedule"
	const claimName = "getfire-daily-report"
	require.NoError(t, s.DB().Where("name IN ?", []string{missingName, claimName}).
		Delete(&core.ScheduledFire{}).Error)

	got, found, err := s.GetScheduledFireTime(ctx, missingName)
	require.NoError(t, err)
	assert.False(t, found)
	assert.True(t, got.IsZero())

	t1 := time.Now().UTC().Truncate(time.Millisecond)
	claimed, err := s.ClaimScheduledFire(ctx, claimName, t1)
	require.NoError(t, err)
	require.True(t, claimed)

	got, found, err = s.GetScheduledFireTime(ctx, claimName)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, got.Equal(t1), "got %v, want %v", got, t1)
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

	cancelledIDs, err := s.CancelSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Len(t, cancelledIDs, 3, "should return all three sub-job IDs")
	gotIDs := map[string]bool{}
	for _, id := range cancelledIDs {
		gotIDs[id] = true
	}
	for _, sub := range subs {
		assert.True(t, gotIDs[sub.ID], "cancelledIDs missing sub %s", sub.ID)
	}

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

func TestCancelSubJob_NoDoubleCountWhenAlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 1}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	sub := &core.Job{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 0}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{sub}))

	first, err := s.CancelSubJob(ctx, sub.ID)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, 1, first.CancelledCount)

	second, err := s.CancelSubJob(ctx, sub.ID)
	require.NoError(t, err)
	assert.Nil(t, second, "already-terminal sub-job cancellation is a no-op")

	got, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 1, got.CancelledCount)
	assert.LessOrEqual(t, got.CompletedCount+got.FailedCount+got.CancelledCount, got.TotalCount)
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

func TestCancelSubJobs_SetsCompletedAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	fanOut := &core.FanOut{ParentJobID: parent.ID, TotalCount: 2}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	subs := []*core.Job{
		{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 0},
		{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 1},
	}
	require.NoError(t, s.EnqueueBatch(ctx, subs))

	cancelledIDs, err := s.CancelSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	require.Len(t, cancelledIDs, 2)

	for _, sub := range subs {
		got, err := s.GetJob(ctx, sub.ID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, core.StatusCancelled, got.Status)
		assert.NotNil(t, got.CompletedAt)
	}
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

// TestGetWaitingJobsToResume_SkipsParentsWithPendingFanOuts is the regression
// guard for the sequential-fan-out bug. A workflow that dispatches phase 1,
// suspends, resumes, dispatches phase 2, suspends again has two rows in
// fan_outs: #1 completed, #2 pending. The polling fallback must NOT resume
// the parent — phase 2 is still running. Before the fix, the INNER JOIN
// matched fan_out #1 alone and woke the parent every poll tick, causing the
// workflow to spin and re-enter the phase loop forever while phase 2's child
// quietly tried to make progress underneath. After the fix, the NOT EXISTS
// guard requires every fan-out for the parent to have terminated.
func TestGetWaitingJobsToResume_SkipsParentsWithPendingFanOuts(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.SuspendJob(ctx, got.ID, "worker-1"))

	// Sequential fan-outs: phase 1 done, phase 2 still running.
	fanOut1 := &core.FanOut{ParentJobID: parent.ID, TotalCount: 1, Status: core.FanOutCompleted}
	require.NoError(t, s.CreateFanOut(ctx, fanOut1))
	fanOut2 := &core.FanOut{ParentJobID: parent.ID, TotalCount: 1, Status: core.FanOutPending}
	require.NoError(t, s.CreateFanOut(ctx, fanOut2))

	waiting, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)
	assert.Empty(t, waiting, "parent must not be eligible to resume while a later fan-out is still pending")
}

// TestGetWaitingJobsToResume_DeduplicatesParentsWithMultipleTerminatedFanOuts
// guards against the parent appearing multiple times in the result when it
// has more than one completed/failed fan-out (e.g. a workflow that ran phase
// 1 and phase 2 successfully and is now waiting on phase 3). DISTINCT in the
// query keeps the row count honest.
func TestGetWaitingJobsToResume_DeduplicatesParentsWithMultipleTerminatedFanOuts(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := newTestJob("default", "workflow.run")
	require.NoError(t, s.Enqueue(ctx, parent))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.SuspendJob(ctx, got.ID, "worker-1"))

	// Three terminated fan-outs, no pending — parent should appear once.
	for _, status := range []core.FanOutStatus{core.FanOutCompleted, core.FanOutCompleted, core.FanOutFailed} {
		require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
			ParentJobID: parent.ID,
			TotalCount:  1,
			Status:      status,
		}))
	}

	waiting, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)
	require.Len(t, waiting, 1)
	assert.Equal(t, parent.ID, waiting[0].ID)
}

func TestGetWaitingJobsToResume_LimitsResumeBatch(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	for i := 0; i < maxResumeBatch+25; i++ {
		parent := &core.Job{Type: "workflow.bulk", Queue: "default", Status: core.StatusWaiting}
		require.NoError(t, s.Enqueue(ctx, parent))
		require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
			ParentJobID: parent.ID,
			TotalCount:  1,
			Status:      core.FanOutCompleted,
		}))
	}

	waiting, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)
	assert.Len(t, waiting, maxResumeBatch)
}

func TestGetStalledFanOutParents(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	old := time.Now().Add(-10 * time.Minute)
	recent := time.Now()
	cutoff := time.Now().Add(-2 * time.Minute)

	oldIncompleteParent := &core.Job{Type: "workflow.old-incomplete", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.Enqueue(ctx, oldIncompleteParent))
	oldIncompleteFanOut := &core.FanOut{
		ParentJobID: oldIncompleteParent.ID,
		TotalCount:  3,
		Status:      core.FanOutPending,
		CreatedAt:   old,
	}
	require.NoError(t, s.CreateFanOut(ctx, oldIncompleteFanOut))

	recentParent := &core.Job{Type: "workflow.recent", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.Enqueue(ctx, recentParent))
	recentFanOut := &core.FanOut{
		ParentJobID: recentParent.ID,
		TotalCount:  3,
		Status:      core.FanOutPending,
		CreatedAt:   recent,
	}
	require.NoError(t, s.CreateFanOut(ctx, recentFanOut))

	completeParent := &core.Job{Type: "workflow.complete", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.Enqueue(ctx, completeParent))
	completeFanOut := &core.FanOut{
		ParentJobID: completeParent.ID,
		TotalCount:  2,
		Status:      core.FanOutPending,
		CreatedAt:   old,
	}
	require.NoError(t, s.CreateFanOut(ctx, completeFanOut))
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{
		{Type: "sub", Queue: "default", FanOutID: &completeFanOut.ID, FanOutIndex: 0},
		{Type: "sub", Queue: "default", FanOutID: &completeFanOut.ID, FanOutIndex: 1},
	}))

	terminalParent := &core.Job{Type: "workflow.terminal", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.Enqueue(ctx, terminalParent))
	terminalFanOut := &core.FanOut{
		ParentJobID: terminalParent.ID,
		TotalCount:  3,
		Status:      core.FanOutCompleted,
		CreatedAt:   old,
	}
	require.NoError(t, s.CreateFanOut(ctx, terminalFanOut))

	stalled, err := s.GetStalledFanOutParents(ctx, cutoff)
	require.NoError(t, err)
	require.Len(t, stalled, 1)
	assert.Equal(t, oldIncompleteParent.ID, stalled[0].ID)
}

func TestGetStalledFanOutParents_LimitsBatch(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	old := time.Now().Add(-10 * time.Minute)
	cutoff := time.Now().Add(-2 * time.Minute)
	for i := 0; i < maxResumeBatch+25; i++ {
		parent := &core.Job{Type: "workflow.stalled", Queue: "default", Status: core.StatusWaiting}
		require.NoError(t, s.Enqueue(ctx, parent))
		require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
			ParentJobID: parent.ID,
			TotalCount:  2,
			Status:      core.FanOutPending,
			CreatedAt:   old,
		}))
	}

	stalled, err := s.GetStalledFanOutParents(ctx, cutoff)
	require.NoError(t, err)
	assert.Len(t, stalled, maxResumeBatch)
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
	assert.Empty(t, cancelled)
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
	assert.Len(t, cancelled, 3)

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
	fo, err := s.IncrementFanOutCompleted(context.Background(), "fo1")
	assert.Error(t, err)
	assert.Nil(t, fo)
}

func TestIncrementFanOutFailed_DBError(t *testing.T) {
	s := closedStorage(t)
	fo, err := s.IncrementFanOutFailed(context.Background(), "fo1")
	assert.Error(t, err)
	assert.Nil(t, fo)
}

func TestIncrementFanOutCancelled_DBError(t *testing.T) {
	s := closedStorage(t)
	fo, err := s.IncrementFanOutCancelled(context.Background(), "fo1")
	assert.Error(t, err)
	assert.Nil(t, fo)
}

func TestIncrementFanOut_DBError_ReturnsNilFanOut(t *testing.T) {
	ctx := context.Background()

	completed, err := closedStorage(t).IncrementFanOutCompleted(ctx, "fo1")
	assert.Error(t, err)
	assert.Nil(t, completed)

	failed, err := closedStorage(t).IncrementFanOutFailed(ctx, "fo1")
	assert.Error(t, err)
	assert.Nil(t, failed)

	cancelled, err := closedStorage(t).IncrementFanOutCancelled(ctx, "fo1")
	assert.Error(t, err)
	assert.Nil(t, cancelled)
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

// ──────────────────────────────────────────────────────────────────────────────
// WithStorageLockDuration / SetLockDuration
// ──────────────────────────────────────────────────────────────────────────────

func TestNewGormStorage_DefaultLockDuration(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := NewGormStorage(db)
	assert.Equal(t, 45*time.Minute, s.lockDuration, "default lock duration should be 45 minutes")
}

func TestWithStorageLockDuration_SetsCustomDuration(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := NewGormStorage(db, WithStorageLockDuration(2*time.Hour))
	assert.Equal(t, 2*time.Hour, s.lockDuration)
}

func TestSetLockDuration_OverridesValue(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := NewGormStorage(db)
	s.SetLockDuration(90 * time.Minute)
	assert.Equal(t, 90*time.Minute, s.lockDuration)
}

func TestDequeue_CustomLockDurationIsApplied(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetLockDuration(10 * time.Minute)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	before := time.Now()
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.LockedUntil)

	// The lock should expire in ~10 minutes, not 45.
	// Allow a few seconds of clock skew.
	expectedMin := before.Add(9 * time.Minute)
	expectedMax := before.Add(11 * time.Minute)
	assert.True(t, got.LockedUntil.After(expectedMin),
		"LockedUntil %v should be after %v", got.LockedUntil, expectedMin)
	assert.True(t, got.LockedUntil.Before(expectedMax),
		"LockedUntil %v should be before %v", got.LockedUntil, expectedMax)
}

func TestHeartbeat_CustomLockDurationIsApplied(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	s.SetLockDuration(10 * time.Minute)

	require.NoError(t, s.Enqueue(ctx, newTestJob("default", "task.run")))

	got, err := s.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	time.Sleep(10 * time.Millisecond)

	before := time.Now()
	require.NoError(t, s.Heartbeat(ctx, got.ID, "worker-1"))

	refreshed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	require.NotNil(t, refreshed.LockedUntil)

	// The extended lock should still be ~10 minutes from now, not 45.
	expectedMin := before.Add(9 * time.Minute)
	expectedMax := before.Add(11 * time.Minute)
	assert.True(t, refreshed.LockedUntil.After(expectedMin),
		"LockedUntil %v should be after %v", refreshed.LockedUntil, expectedMin)
	assert.True(t, refreshed.LockedUntil.Before(expectedMax),
		"LockedUntil %v should be before %v", refreshed.LockedUntil, expectedMax)
}

// ──────────────────────────────────────────────────────────────────────────────
// EnqueueBatch — UniqueKey idempotency (fan-out replay safety)
// ──────────────────────────────────────────────────────────────────────────────

// TestEnqueueBatch_SkipsDuplicatesByUniqueKey verifies that a second call to
// EnqueueBatch with the same UniqueKey values is a no-op, so replaying a
// parent workflow after a crash does not create duplicate sub-jobs.
func TestEnqueueBatch_SkipsDuplicatesByUniqueKey(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	first := []*core.Job{
		{Type: "sub.a", Queue: "default", UniqueKey: "fanout-abc-0"},
		{Type: "sub.b", Queue: "default", UniqueKey: "fanout-abc-1"},
		{Type: "sub.c", Queue: "default", UniqueKey: "fanout-abc-2"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, first))

	// Simulate a replay: different Job IDs, same UniqueKeys.
	replay := []*core.Job{
		{Type: "sub.a", Queue: "default", UniqueKey: "fanout-abc-0"},
		{Type: "sub.b", Queue: "default", UniqueKey: "fanout-abc-1"},
		{Type: "sub.c", Queue: "default", UniqueKey: "fanout-abc-2"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, replay))

	all, err := s.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Len(t, all, 3, "replay must not create duplicate sub-jobs")

	// The surviving rows are the originals.
	originalIDs := map[string]bool{first[0].ID: true, first[1].ID: true, first[2].ID: true}
	for _, j := range all {
		assert.Truef(t, originalIDs[j.ID], "unexpected duplicate job %q", j.ID)
	}
}

// TestEnqueueBatch_MixedReplayInsertsOnlyNewJobs covers the realistic
// partial-replay case where some sub-jobs were persisted before the parent
// crashed and some were not. EnqueueBatch must insert only the missing ones.
func TestEnqueueBatch_MixedReplayInsertsOnlyNewJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// First pass: imagine only the first two sub-jobs made it to storage
	// before a crash.
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{
		{Type: "sub.a", Queue: "default", UniqueKey: "fanout-xyz-0"},
		{Type: "sub.b", Queue: "default", UniqueKey: "fanout-xyz-1"},
	}))

	// Replay: parent workflow re-runs and attempts to enqueue all three
	// sub-jobs. The first two should be skipped; only index 2 is new.
	replay := []*core.Job{
		{Type: "sub.a", Queue: "default", UniqueKey: "fanout-xyz-0"},
		{Type: "sub.b", Queue: "default", UniqueKey: "fanout-xyz-1"},
		{Type: "sub.c", Queue: "default", UniqueKey: "fanout-xyz-2"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, replay))

	all, err := s.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Len(t, all, 3, "replay must add exactly one new sub-job")

	byKey := map[string]int{}
	for _, j := range all {
		byKey[j.UniqueKey]++
	}
	assert.Equal(t, 1, byKey["fanout-xyz-0"])
	assert.Equal(t, 1, byKey["fanout-xyz-1"])
	assert.Equal(t, 1, byKey["fanout-xyz-2"])
}

// TestEnqueueBatch_SkipsDuplicatesAcrossTerminalStates verifies that the
// idempotency filter considers pending, running, and completed jobs — not
// just pending — so a replay does not re-create a sub-job that has already
// executed.
func TestEnqueueBatch_SkipsDuplicatesAcrossTerminalStates(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Seed two sub-jobs: drive one to completed, leave one pending.
	seed := []*core.Job{
		{Type: "sub.a", Queue: "default", UniqueKey: "fanout-states-0"},
		{Type: "sub.b", Queue: "default", UniqueKey: "fanout-states-1"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, seed))

	dequeued, err := s.Dequeue(ctx, []string{"default"}, "worker-X")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	require.NoError(t, s.Complete(ctx, dequeued.ID, "worker-X"))

	// Replay with the same UniqueKeys should skip both.
	replay := []*core.Job{
		{Type: "sub.a", Queue: "default", UniqueKey: "fanout-states-0"},
		{Type: "sub.b", Queue: "default", UniqueKey: "fanout-states-1"},
	}
	require.NoError(t, s.EnqueueBatch(ctx, replay))

	// Count unique-keyed jobs regardless of status.
	var total int64
	require.NoError(t, s.DB().Model(&core.Job{}).
		Where("unique_key LIKE ?", "fanout-states-%").
		Count(&total).Error)
	assert.EqualValues(t, 2, total, "completed + pending siblings must not be duplicated on replay")
}

// TestEnqueueBatch_ConcurrentUniqueKey_NoDuplicates probes the TOCTOU window
// between the Count and Create statements in EnqueueBatch. Multiple goroutines
// race to insert the same set of UniqueKeys simultaneously; afterwards the
// database must contain exactly one row per key. Failure of this test means
// two concurrent parent replays could produce duplicate sub-jobs.
//
// Uses a WAL-mode SQLite file (or the external TEST_DATABASE_URL) so pooled
// connections see the same database — plain ":memory:" gives each
// connection its own isolated DB, which would mask the race entirely.
func TestEnqueueBatch_ConcurrentUniqueKey_NoDuplicates(t *testing.T) {
	ctx := context.Background()
	s := newConcurrentTestStorage(t)

	const workers = 8
	keys := []string{
		"fanout-race-0",
		"fanout-race-1",
		"fanout-race-2",
	}

	// Gate so all goroutines hit EnqueueBatch as close to simultaneously
	// as possible, maximizing the chance of exposing the race.
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]*core.Job, len(keys))
			for i, k := range keys {
				batch[i] = &core.Job{
					Type:      "sub.race",
					Queue:     "default",
					UniqueKey: k,
				}
			}
			<-start
			if err := s.EnqueueBatch(ctx, batch); err != nil {
				errs <- err
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		// A unique-constraint violation surfacing here would also be an
		// acceptable signal that duplicates were prevented; any other error
		// is a real failure.
		require.NoError(t, err)
	}

	byKey := map[string]int64{}
	for _, k := range keys {
		var count int64
		require.NoError(t, s.DB().Model(&core.Job{}).
			Where("unique_key = ?", k).Count(&count).Error)
		byKey[k] = count
	}

	for _, k := range keys {
		assert.EqualValuesf(t, 1, byKey[k],
			"UniqueKey %q has %d rows; concurrent EnqueueBatch produced duplicates", k, byKey[k])
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// FindOrphanedJobs
// ──────────────────────────────────────────────────────────────────────────────

// TestFindOrphanedJobs_FlagsReclaimedAndCancelled is the storage-level
// contract test for the cross-worker cancellation feature. The audit
// query must return IDs of jobs whose DB state indicates the caller no
// longer owns them:
//   - locked_by changed (reclaimed by another worker)
//   - locked_by IS NULL (stale-lock reaper released)
//   - status terminal (cancelled by a fan-out or completed by a replay)
//
// and must NOT return jobs still legitimately owned by the caller.
func TestFindOrphanedJobs_FlagsReclaimedAndCancelled(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Seed four jobs in different states. We'll claim the first two from
	// worker-A's perspective and check the audit.
	for i := 0; i < 4; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("default", "t")))
	}

	// worker-A dequeues 2 jobs.
	jobA, err := s.Dequeue(ctx, []string{"default"}, "worker-A")
	require.NoError(t, err)
	require.NotNil(t, jobA)
	jobB, err := s.Dequeue(ctx, []string{"default"}, "worker-A")
	require.NoError(t, err)
	require.NotNil(t, jobB)
	jobC, err := s.Dequeue(ctx, []string{"default"}, "worker-A")
	require.NoError(t, err)
	require.NotNil(t, jobC)
	jobD, err := s.Dequeue(ctx, []string{"default"}, "worker-A")
	require.NoError(t, err)
	require.NotNil(t, jobD)

	// jobA: still owned by worker-A → not orphaned.
	// jobB: stolen by worker-B.
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", jobB.ID).
		Update("locked_by", "worker-B").Error)
	// jobC: lock released to nil (stale-lock reaper).
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", jobC.ID).
		Updates(map[string]any{"locked_by": nil, "status": core.StatusPending}).Error)
	// jobD: cancelled by a fan-out.
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id = ?", jobD.ID).
		Update("status", core.StatusCancelled).Error)

	orphaned, err := s.FindOrphanedJobs(ctx, []string{jobA.ID, jobB.ID, jobC.ID, jobD.ID}, "worker-A")
	require.NoError(t, err)

	// Convert to set for order-independent assertion.
	got := map[string]bool{}
	for _, id := range orphaned {
		got[id] = true
	}
	assert.False(t, got[jobA.ID], "jobA is still owned, should not be orphaned")
	assert.True(t, got[jobB.ID], "jobB was stolen, should be orphaned")
	assert.True(t, got[jobC.ID], "jobC's lock was released, should be orphaned")
	assert.True(t, got[jobD.ID], "jobD was cancelled, should be orphaned")
}

func TestFindOrphanedJobs_EmptyInputReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	orphaned, err := s.FindOrphanedJobs(ctx, nil, "worker-A")
	require.NoError(t, err)
	assert.Empty(t, orphaned)

	orphaned, err = s.FindOrphanedJobs(ctx, []string{}, "worker-A")
	require.NoError(t, err)
	assert.Empty(t, orphaned)
}

func TestFindOrphanedJobs_UnknownIDsAreNotReturned(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Asking about IDs that don't exist must not cause errors and must
	// not return those IDs (they're not in the DB, so not technically
	// orphaned — they're just non-existent).
	orphaned, err := s.FindOrphanedJobs(ctx, []string{"does-not-exist-1", "does-not-exist-2"}, "worker-A")
	require.NoError(t, err)
	assert.Empty(t, orphaned)
}

// TestIsSerializationFailure_RetryableErrors locks in the set of transient
// driver errors that withSerializationRetry must retry. The SQLite BUSY/LOCKED
// cases are a regression guard: the P1 local-handler-cancel can wake sibling
// sub-job handlers into concurrent writes, and if a contended CancelSubJob /
// Increment* surfaced SQLITE_BUSY instead of being retried, the sub-job would
// go unaccounted and a CollectAll/Threshold parent would wedge in 'waiting'.
func TestIsSerializationFailure_RetryableErrors(t *testing.T) {
	retryable := []string{
		// MySQL
		"Error 1213: Deadlock found when trying to get lock",
		"Deadlock found when trying to get lock; try restarting transaction",
		"Error 1205: Lock wait timeout exceeded",
		"Lock wait timeout exceeded; try restarting transaction",
		// PostgreSQL
		"ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)",
		"ERROR: deadlock detected (SQLSTATE 40P01)",
		"could not serialize access due to read/write dependencies",
		"deadlock detected",
		// SQLite (regression guard)
		"database is locked",
		"database table is locked",
		"SQLITE_BUSY: database is locked",
		"SQLITE_LOCKED: database table is locked",
	}
	for _, msg := range retryable {
		if !isSerializationFailure(errors.New(msg)) {
			t.Errorf("expected %q to be treated as a retryable serialization failure", msg)
		}
	}

	nonRetryable := []string{
		"",
		"record not found",
		"UNIQUE constraint failed: jobs.unique_key",
		"no such table: jobs",
		"context canceled",
		"jobs: job not owned by this worker",
	}
	for _, msg := range nonRetryable {
		var err error
		if msg != "" {
			err = errors.New(msg)
		}
		if isSerializationFailure(err) {
			t.Errorf("expected %q to NOT be treated as a serialization failure", msg)
		}
	}
}
