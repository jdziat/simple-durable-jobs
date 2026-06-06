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

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
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

func TestWithRetentionZeroConfigDisabled(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithRetention(), WithRetention(RetentionInterval(10*time.Millisecond)))
	assert.False(t, w.config.Retention.enabled())
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

func workerRetentionExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&core.Job{}).Where("id = ?", id).Count(&count).Error)
	return count == 1
}
