package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestQueue(t *testing.T) (*jobs.Queue, *gorm.DB) {
	// Use shared memory database with WAL mode for concurrent access
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&mode=memory"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	// Enable WAL mode for better concurrency
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA busy_timeout=5000;")

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)
	return queue, db
}

func TestQueue_Register(t *testing.T) {
	queue, _ := setupTestQueue(t)

	type EmailParams struct {
		To      string
		Subject string
	}

	queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
		return nil
	})

	assert.True(t, queue.HasHandler("send-email"))
	assert.False(t, queue.HasHandler("unknown"))
}

func TestQueue_Enqueue(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	type EmailParams struct {
		To      string
		Subject string
	}

	queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "send-email", EmailParams{
		To:      "test@example.com",
		Subject: "Hello",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, jobID)

	// Verify job in database
	var job jobs.Job
	err = db.First(&job, "id = ?", jobID).Error
	require.NoError(t, err)
	assert.Equal(t, "send-email", job.Type)
	assert.Equal(t, "default", job.Queue)
}

func TestQueue_EnqueueWithOptions(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("task", func(ctx context.Context, args string) error {
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "task", "data",
		jobs.QueueOpt("critical"),
		jobs.Priority(100),
		jobs.Retries(5),
	)

	require.NoError(t, err)

	var job jobs.Job
	db.First(&job, "id = ?", jobID)

	assert.Equal(t, "critical", job.Queue)
	assert.Equal(t, 100, job.Priority)
	assert.Equal(t, 5, job.MaxRetries)
}

func TestQueue_Schedule(t *testing.T) {
	queue, _ := setupTestQueue(t)
	ctx := context.Background()

	var runCount atomic.Int32
	queue.Register("scheduled-task", func(ctx context.Context, _ struct{}) error {
		runCount.Add(1)
		return nil
	})

	queue.Schedule("scheduled-task", jobs.Every(200*time.Millisecond))

	worker := queue.NewWorker(jobs.WithScheduler(true))
	workerCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	go worker.Start(workerCtx)

	time.Sleep(800 * time.Millisecond)

	// Should have run at least once (relaxed for race detector)
	assert.GreaterOrEqual(t, runCount.Load(), int32(1))
}
