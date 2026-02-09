package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestHooks_OnJobStart(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var started atomic.Bool
	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		started.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, started.Load())
}

func TestHooks_OnJobComplete(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var completed atomic.Bool
	queue.OnJobComplete(func(ctx context.Context, j *jobs.Job) {
		completed.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, completed.Load())
}

func TestHooks_OnJobFail(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var failed atomic.Bool
	queue.OnJobFail(func(ctx context.Context, j *jobs.Job, err error) {
		failed.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return jobs.NoRetry(errors.New("fail"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, failed.Load())
}
