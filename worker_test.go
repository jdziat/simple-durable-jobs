package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestWorker_ProcessesJob(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var processed atomic.Bool
	queue.Register("test-job", func(ctx context.Context, msg string) error {
		processed.Store(true)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test-job", "hello")

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	assert.True(t, processed.Load())
}

func TestWorker_MarksJobComplete(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	queue.Register("test-job", func(ctx context.Context, msg string) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, _ := queue.Enqueue(ctx, "test-job", "hello")

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	job, _ := store.GetJob(context.Background(), jobID)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}
