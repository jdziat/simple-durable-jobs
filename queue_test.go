package jobs_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestQueue(t *testing.T) (*jobs.Queue, *gorm.DB) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

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
