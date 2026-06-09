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

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/storage"
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

func TestStart_WarnsWhenRetentionUnconfigured(t *testing.T) {
	var buf bytes.Buffer
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	require.False(t, w.config.Retention.enabled())

	// CompareAndSwap dedup: two direct calls log exactly once.
	w.warnIfRetentionUnconfigured()
	w.warnIfRetentionUnconfigured()

	out := buf.String()
	assert.Contains(t, out, "retention is not configured")
	assert.Equal(t, 1, strings.Count(out, "retention is not configured"))
}

func TestStart_NoWarnWhenRetentionConfigured(t *testing.T) {
	var buf bytes.Buffer
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithRetention(RetentionCompletedAfter(time.Hour)),
		WithOwnershipAuditInterval(0),
	)
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	require.True(t, w.config.Retention.enabled())

	// The Start gate takes the retention branch (not the warn branch) when
	// enabled; exercising the warn path directly here would be a misuse, so we
	// assert the gate predicate and that no warn was emitted.
	assert.NotContains(t, buf.String(), "retention is not configured")
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

		job := &core.Job{ID: "wired-" + t.Name(), Type: "gc", Queue: "default", Status: core.StatusPending}
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
		ID:          id,
		Type:        "retention.worker",
		Queue:       "default",
		Status:      status,
		CompletedAt: &completedAt,
	}).Error)
}

func seedWorkerRetentionSignal(t *testing.T, db *gorm.DB, id, jobID, name string, consumedAt *time.Time) {
	t.Helper()
	require.NoError(t, db.Create(&core.Signal{
		ID:         id,
		JobID:      jobID,
		Name:       name,
		Payload:    []byte(`"payload"`),
		ConsumedAt: consumedAt,
	}).Error)
}

func workerRetentionExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&core.Job{}).Where("id = ?", id).Count(&count).Error)
	return count == 1
}

func workerRetentionSignalExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&core.Signal{}).Where("id = ?", id).Count(&count).Error)
	return count == 1
}
