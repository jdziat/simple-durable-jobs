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

func TestCall_ExecutesNestedJob(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var callCount atomic.Int32
	queue.Register("nested", func(ctx context.Context, n int) (int, error) {
		callCount.Add(1)
		return n * 2, nil
	})

	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[int](ctx, "nested", 5)
		if err != nil {
			return err
		}
		assert.Equal(t, 10, result)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "parent", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(1), callCount.Load())
}

func TestCall_ReturnsCheckpointedResult(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var callCount atomic.Int32
	queue.Register("nested", func(ctx context.Context, n int) (int, error) {
		callCount.Add(1)
		return n * 2, nil
	})

	// Simulate pre-existing checkpoint
	jobID := "test-job"
	checkpoint := &jobs.Checkpoint{
		ID:        "cp-1",
		JobID:     jobID,
		CallIndex: 0,
		CallType:  "nested",
		Result:    []byte(`100`),
	}
	store.SaveCheckpoint(context.Background(), checkpoint)

	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[int](ctx, "nested", 5)
		require.NoError(t, err)
		assert.Equal(t, 100, result) // Should get checkpointed result
		return nil
	})

	// Create job with pre-existing checkpoints
	job := &jobs.Job{
		ID:     jobID,
		Type:   "parent",
		Args:   []byte(`{}`),
		Queue:  "default",
		Status: jobs.StatusPending,
	}
	store.Enqueue(context.Background(), job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	// Nested should not have been called (checkpoint existed)
	assert.Equal(t, int32(0), callCount.Load())
}
