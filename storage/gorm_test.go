package storage_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	return db
}

func TestGormStorage_EnqueueAndDequeue(t *testing.T) {
	db := setupTestDB(t)
	store := storage.NewGormStorage(db)
	ctx := context.Background()

	err := store.Migrate(ctx)
	require.NoError(t, err)

	job := &jobs.Job{
		ID:     "job-123",
		Type:   "send-email",
		Args:   []byte(`{"to":"test@example.com"}`),
		Queue:  "default",
		Status: jobs.StatusPending,
	}

	err = store.Enqueue(ctx, job)
	require.NoError(t, err)

	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	assert.Equal(t, "job-123", dequeued.ID)
	assert.Equal(t, jobs.StatusRunning, dequeued.Status)
	assert.Equal(t, "worker-1", dequeued.LockedBy)
}

func TestGormStorage_DequeueRespectsQueue(t *testing.T) {
	db := setupTestDB(t)
	store := storage.NewGormStorage(db)
	ctx := context.Background()

	err := store.Migrate(ctx)
	require.NoError(t, err)

	job := &jobs.Job{
		ID:    "job-123",
		Type:  "report",
		Queue: "reports",
	}
	err = store.Enqueue(ctx, job)
	require.NoError(t, err)

	dequeued, err := store.Dequeue(ctx, []string{"emails"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, dequeued)

	dequeued, err = store.Dequeue(ctx, []string{"reports"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, "job-123", dequeued.ID)
}

func TestGormStorage_DequeueRespectsPriority(t *testing.T) {
	db := setupTestDB(t)
	store := storage.NewGormStorage(db)
	ctx := context.Background()

	err := store.Migrate(ctx)
	require.NoError(t, err)

	lowPri := &jobs.Job{ID: "low", Type: "task", Queue: "default", Priority: 1}
	highPri := &jobs.Job{ID: "high", Type: "task", Queue: "default", Priority: 100}

	store.Enqueue(ctx, lowPri)
	store.Enqueue(ctx, highPri)

	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	assert.Equal(t, "high", dequeued.ID)
}
