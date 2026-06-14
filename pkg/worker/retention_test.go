package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

func TestWorkerRetentionLoopPrunesOldTerminalJobs(t *testing.T) {
	db, store := newWorkerRetentionStore(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	recent := time.Now().Add(-10 * time.Minute).UTC()

	seedWorkerRetentionJob(t, db, "completed-old", core.StatusCompleted, old)
	seedWorkerRetentionJob(t, db, "failed-old", core.StatusFailed, old)
	seedWorkerRetentionJob(t, db, "cancelled-old", core.StatusCancelled, old)
	seedWorkerRetentionJob(t, db, "completed-new", core.StatusCompleted, recent)
	seedWorkerRetentionJob(t, db, "pending-old", core.StatusPending, old)
	seedWorkerRetentionJob(t, db, "running-old", core.StatusRunning, old)
	seedWorkerRetentionJob(t, db, "waiting-old", core.StatusWaiting, old)
	seedWorkerRetentionJob(t, db, "paused-old", core.StatusPaused, old)

	q := queue.New(store)
	w := NewWorker(q,
		WorkerQueue("retention-empty", Concurrency(1)),
		WithRetention(
			RetentionCompletedAfter(time.Hour),
			RetentionFailedAfter(time.Hour),
			RetentionInterval(10*time.Millisecond),
			RetentionBatchSize(1),
		),
		WithOwnershipAuditInterval(0),
	)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Start(runCtx) }()
	require.Eventually(t, func() bool {
		return !workerRetentionExists(t, db, "completed-old") &&
			!workerRetentionExists(t, db, "failed-old") &&
			!workerRetentionExists(t, db, "cancelled-old")
	}, 5*time.Second, 50*time.Millisecond)

	assert.True(t, workerRetentionExists(t, db, "completed-new"))
	assert.True(t, workerRetentionExists(t, db, "pending-old"))
	assert.True(t, workerRetentionExists(t, db, "running-old"))
	assert.True(t, workerRetentionExists(t, db, "waiting-old"))
	assert.True(t, workerRetentionExists(t, db, "paused-old"))

	cancel()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return assert.ErrorIs(t, err, context.Canceled)
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
}

func TestWorkerRetentionLoopPrunesConsumedSignals(t *testing.T) {
	db, store := newWorkerRetentionStore(t)
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour).UTC()
	recent := time.Now().Add(-10 * time.Minute).UTC()

	seedWorkerRetentionSignal(t, db, "consumed-old", "job-a", "ctx", &old)
	seedWorkerRetentionSignal(t, db, "consumed-new", "job-a", "ctx", &recent)
	seedWorkerRetentionSignal(t, db, "pending-old", "job-a", "ctx", nil)

	q := queue.New(store)
	w := NewWorker(q,
		WorkerQueue("retention-empty", Concurrency(1)),
		WithRetention(
			RetentionConsumedSignalsAfter(time.Hour),
			RetentionInterval(10*time.Millisecond),
			RetentionBatchSize(1),
		),
		WithOwnershipAuditInterval(0),
	)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Start(runCtx) }()
	require.Eventually(t, func() bool {
		return !workerRetentionSignalExists(t, db, "consumed-old")
	}, 5*time.Second, 50*time.Millisecond)

	assert.True(t, workerRetentionSignalExists(t, db, "consumed-new"))
	assert.True(t, workerRetentionSignalExists(t, db, "pending-old"))

	cancel()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return assert.ErrorIs(t, err, context.Canceled)
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
}

func TestWorkerRetentionUnsupportedStorageLogsOnceAndRuns(t *testing.T) {
	var buf lockedLogBuffer
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithRetention(
			RetentionCompletedAfter(time.Hour),
			RetentionInterval(10*time.Millisecond),
		),
		WithOwnershipAuditInterval(0),
	)
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "storage backend does not support retention GC")
	}, 2*time.Second, 20*time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return assert.ErrorIs(t, err, context.Canceled)
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, 1, strings.Count(buf.String(), "storage backend does not support retention GC"))
}

func TestWorkerRetentionDefaultOnPrunesSafeWindows(t *testing.T) {
	db, store := newWorkerRetentionStore(t)
	ctx := context.Background()
	oldCompleted := time.Now().Add(-(31 * 24 * time.Hour)).UTC()
	oldFailed := time.Now().Add(-(91 * 24 * time.Hour)).UTC()
	oldSignal := time.Now().Add(-(8 * 24 * time.Hour)).UTC()
	recentCompleted := time.Now().Add(-(29 * 24 * time.Hour)).UTC()
	recentFailed := time.Now().Add(-(89 * 24 * time.Hour)).UTC()
	recentSignal := time.Now().Add(-(6 * 24 * time.Hour)).UTC()

	seedWorkerRetentionJob(t, db, "default-completed-old", core.StatusCompleted, oldCompleted)
	seedWorkerRetentionJob(t, db, "default-completed-new", core.StatusCompleted, recentCompleted)
	seedWorkerRetentionJob(t, db, "default-failed-old", core.StatusFailed, oldFailed)
	seedWorkerRetentionJob(t, db, "default-failed-new", core.StatusFailed, recentFailed)
	seedWorkerRetentionJob(t, db, "default-cancelled-old", core.StatusCancelled, oldFailed)
	seedWorkerRetentionSignal(t, db, "default-signal-old", "default-completed-new", "ctx", &oldSignal)
	seedWorkerRetentionSignal(t, db, "default-signal-new", "default-completed-new", "ctx", &recentSignal)

	q := queue.New(store)
	w := NewWorker(q,
		WorkerQueue("retention-empty", Concurrency(1)),
		WithOwnershipAuditInterval(0),
	)
	require.Equal(t, defaultRetentionCompletedAfter, w.config.Retention.CompletedAfter)
	require.Equal(t, defaultRetentionFailedAfter, w.config.Retention.FailedAfter)
	require.Equal(t, defaultRetentionConsumedSignalsAfter, w.config.Retention.ConsumedSignalsAfter)
	w.config.Retention.Interval = 10 * time.Millisecond
	w.config.Retention.BatchSize = 1

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Start(runCtx) }()
	require.Eventually(t, func() bool {
		return !workerRetentionExists(t, db, "default-completed-old") &&
			!workerRetentionExists(t, db, "default-failed-old") &&
			!workerRetentionExists(t, db, "default-cancelled-old") &&
			!workerRetentionSignalExists(t, db, "default-signal-old")
	}, 5*time.Second, 50*time.Millisecond)

	assert.True(t, workerRetentionExists(t, db, "default-completed-new"))
	assert.True(t, workerRetentionExists(t, db, "default-failed-new"))
	assert.True(t, workerRetentionSignalExists(t, db, "default-signal-new"))

	cancel()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return assert.ErrorIs(t, err, context.Canceled)
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
}

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestStart_WarnsWhenRetentionDisabled(t *testing.T) {
	var buf bytes.Buffer
	q := queue.New(&mockStorage{})
	w := NewWorker(q, RetentionDisabled(), WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	require.False(t, w.config.Retention.enabled())

	// CompareAndSwap dedup: two direct calls log exactly once.
	w.warnIfRetentionUnconfigured()
	w.warnIfRetentionUnconfigured()

	out := buf.String()
	assert.Contains(t, out, "retention is disabled")
	assert.Equal(t, 1, strings.Count(out, "retention is disabled"))
}

func TestStart_LogsActiveRetentionWindowsOnce(t *testing.T) {
	var buf bytes.Buffer
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	require.True(t, w.config.Retention.enabled())

	w.logRetentionConfigured()
	w.logRetentionConfigured()

	out := buf.String()
	assert.Contains(t, out, "retention GC enabled")
	assert.Contains(t, out, "completed_after=720h0m0s")
	assert.Contains(t, out, "failed_after=2160h0m0s")
	assert.Contains(t, out, "consumed_signals_after=168h0m0s")
	assert.Contains(t, out, "disable with jobs.RetentionDisabled()")
	assert.Equal(t, 1, strings.Count(out, "retention GC enabled"))
}

func TestDefaultRetention_EnablesConservativeWindows(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, DefaultRetention(), WithOwnershipAuditInterval(0))

	cfg := w.config.Retention
	assert.True(t, cfg.enabled())
	assert.Equal(t, 7*24*time.Hour, cfg.CompletedAfter)
	assert.Equal(t, 30*24*time.Hour, cfg.FailedAfter)
	assert.Equal(t, 7*24*time.Hour, cfg.ConsumedSignalsAfter)
}

func TestRetentionDisabledTurnsSweepOff(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, RetentionDisabled(), WithOwnershipAuditInterval(0))

	assert.True(t, w.config.Retention.Disabled)
	assert.False(t, w.config.Retention.enabled())
	assert.Zero(t, w.config.Retention.CompletedAfter)
	assert.Zero(t, w.config.Retention.FailedAfter)
	assert.Zero(t, w.config.Retention.ConsumedSignalsAfter)
}

func TestWithRetentionOverridesDefaultWindows(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithRetention(
			RetentionCompletedAfter(2*time.Hour),
			RetentionFailedAfter(3*time.Hour),
			RetentionConsumedSignalsAfter(4*time.Hour),
		),
		WithOwnershipAuditInterval(0),
	)

	assert.True(t, w.config.Retention.enabled())
	assert.Equal(t, 2*time.Hour, w.config.Retention.CompletedAfter)
	assert.Equal(t, 3*time.Hour, w.config.Retention.FailedAfter)
	assert.Equal(t, 4*time.Hour, w.config.Retention.ConsumedSignalsAfter)
}

func TestRetentionDeleteCheckpointsOnComplete_SetsFlag(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithRetention(RetentionDeleteCheckpointsOnComplete()),
		WithOwnershipAuditInterval(0),
	)
	assert.True(t, w.config.Retention.DeleteCheckpointsOnComplete)
	// The opt-in alone does not start a retention sweep.
	assert.False(t, w.config.Retention.enabled())
}

// TestRetentionDeleteCheckpointsOnComplete_WiredToStorage proves NewWorker
// propagates the opt-in to a GormStorage backend (via SetDeleteCheckpointsOnComplete),
// so a real success write GCs that job's checkpoints. Without the opt-in the
// same backend keeps them (default-preserving for the dashboard).
func TestRetentionDeleteCheckpointsOnComplete_WiredToStorage(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, optIn bool, wantCheckpoints int) {
		_, store := newWorkerRetentionStore(t)
		q := queue.New(store)
		opts := []WorkerOption{WithOwnershipAuditInterval(0)}
		if optIn {
			opts = append(opts, WithRetention(RetentionDeleteCheckpointsOnComplete()))
		}
		// NewWorker is where the wiring happens.
		_ = NewWorker(q, opts...)

		job := &core.Job{ID: workerTestUUID("wired-" + t.Name()), Type: "gc", Queue: "default", Status: core.StatusPending}
		require.NoError(t, store.Enqueue(ctx, job))
		got, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NoError(t, store.SaveCheckpoint(ctx, &core.Checkpoint{JobID: got.ID, CallIndex: 0, CallType: "call", Result: []byte(`"x"`)}))

		_, err = store.CompleteWithResult(ctx, got.ID, "worker-1", []byte(`"done"`))
		require.NoError(t, err)

		cps, err := store.GetCheckpoints(ctx, got.ID)
		require.NoError(t, err)
		assert.Len(t, cps, wantCheckpoints)
	}

	t.Run("opt-in deletes", func(t *testing.T) { run(t, true, 0) })
	t.Run("default keeps", func(t *testing.T) { run(t, false, 1) })
}

func TestWithRetentionZeroConfigDisabled(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithRetention(), WithRetention(RetentionInterval(10*time.Millisecond)))
	assert.False(t, w.config.Retention.enabled())
}

func TestWithRetentionConsumedSignalsEnablesRetention(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithRetention(RetentionConsumedSignalsAfter(time.Hour)))
	assert.True(t, w.config.Retention.enabled())
	assert.Equal(t, time.Hour, w.config.Retention.ConsumedSignalsAfter)
}

func newWorkerRetentionStore(t *testing.T) (*gorm.DB, *storage.GormStorage) {
	t.Helper()
	dbFile := t.TempDir() + "/worker-retention.db"
	dsn := "file:" + dbFile + "?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("PRAGMA busy_timeout=10000").Error)
	require.NoError(t, db.Exec("PRAGMA synchronous=NORMAL").Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = sqlDB.Close() })

	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return db, store
}

func seedWorkerRetentionJob(t *testing.T, db *gorm.DB, id string, status core.JobStatus, completedAt time.Time) {
	t.Helper()
	require.NoError(t, db.Create(&core.Job{
		ID:          workerTestUUID(id),
		Type:        "retention.worker",
		Queue:       "default",
		Status:      status,
		CompletedAt: &completedAt,
	}).Error)
}

func seedWorkerRetentionSignal(t *testing.T, db *gorm.DB, id, jobID, name string, consumedAt *time.Time) {
	t.Helper()
	require.NoError(t, db.Create(&core.Signal{
		ID:         workerTestUUID(id),
		JobID:      workerTestUUID(jobID),
		Name:       name,
		Payload:    []byte(`"payload"`),
		ConsumedAt: consumedAt,
	}).Error)
}

func workerRetentionExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&core.Job{}).Where("id = ?", workerTestUUID(id)).Count(&count).Error)
	return count == 1
}

func workerRetentionSignalExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&core.Signal{}).Where("id = ?", workerTestUUID(id)).Count(&count).Error)
	return count == 1
}
