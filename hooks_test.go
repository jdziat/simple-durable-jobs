package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
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

var hooksTestCounter int

func setupHooksTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	hooksTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_hooks_test_%d_%d.db", os.Getpid(), hooksTestCounter)
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)
	return queue, store
}

func TestHooks_OnJobStart(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var started atomic.Bool
	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		started.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, started.Load())
}

func TestHooks_OnJobComplete(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var completed atomic.Bool
	queue.OnJobComplete(func(ctx context.Context, j *jobs.Job) {
		completed.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, completed.Load())
}

func TestHooks_OnJobFail(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var failed atomic.Bool
	queue.OnJobFail(func(ctx context.Context, j *jobs.Job, err error) {
		failed.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return jobs.NoRetry(errors.New("fail"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, failed.Load())
}

func TestHooks_OnRetry(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var retried atomic.Bool
	var retryAttempt int
	queue.OnRetry(func(ctx context.Context, j *jobs.Job, attempt int, err error) {
		retried.Store(true)
		retryAttempt = attempt
	})

	var attempts atomic.Int32
	queue.Register("retry-test", func(ctx context.Context, _ struct{}) error {
		if attempts.Add(1) < 2 {
			return errors.New("retry me")
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "retry-test", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for retry
	for i := 0; i < 50; i++ {
		if retried.Load() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, retried.Load())
	assert.Equal(t, 1, retryAttempt)
}

func TestHooks_MultipleHooks(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var startCount, completeCount atomic.Int32

	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		startCount.Add(1)
	})
	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		startCount.Add(1)
	})

	queue.OnJobComplete(func(ctx context.Context, j *jobs.Job) {
		completeCount.Add(1)
	})
	queue.OnJobComplete(func(ctx context.Context, j *jobs.Job) {
		completeCount.Add(1)
	})

	queue.Register("multi-hook-test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "multi-hook-test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, int32(2), startCount.Load())
	assert.Equal(t, int32(2), completeCount.Load())
}

func TestHooks_JobDataInHook(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var capturedType string
	var capturedQueue string

	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		capturedType = j.Type
		capturedQueue = j.Queue
	})

	queue.Register("data-hook-test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "data-hook-test", struct{}{}, jobs.QueueOpt("special"))
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WorkerQueue("special", jobs.Concurrency(1)))
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, "data-hook-test", capturedType)
	assert.Equal(t, "special", capturedQueue)
}
