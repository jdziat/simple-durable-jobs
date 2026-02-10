package jobs_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var fanoutTestCounter int

func setupFanOutTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	fanoutTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_fanout_test_%d_%d.db", os.Getpid(), fanoutTestCounter)
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

func TestFanOut_Basic(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedCount atomic.Int32
	resultChan := make(chan int, 5)

	// Register sub-job handler that multiplies by 2
	queue.Register("multiply", func(ctx context.Context, n int) (int, error) {
		processedCount.Add(1)
		result := n * 2
		resultChan <- result
		return result, nil
	})

	// Register parent job that fans out
	queue.Register("batchMultiply", func(ctx context.Context, items []int) ([]int, error) {
		subJobs := make([]jobs.SubJob, len(items))
		for i, item := range items {
			subJobs[i] = jobs.Sub("multiply", item)
		}

		results, err := jobs.FanOut[int](ctx, subJobs, jobs.FailFast())
		if err != nil {
			// If we get a suspend error, that's expected during first run
			if jobs.IsSuspendError(err) {
				return nil, err
			}
			return nil, err
		}

		return jobs.Values(results), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "batchMultiply", []int{1, 2, 3, 4, 5})
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait for sub-jobs to complete
	completed := 0
	timeout := time.After(5 * time.Second)
waitLoop:
	for completed < 5 {
		select {
		case <-resultChan:
			completed++
		case <-timeout:
			break waitLoop
		}
	}

	assert.Equal(t, int32(5), processedCount.Load(), "all 5 sub-jobs should be processed")
}

func TestFanOut_Empty(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var called atomic.Bool

	queue.Register("emptyFanout", func(ctx context.Context, _ struct{}) error {
		results, err := jobs.FanOut[int](ctx, nil, jobs.FailFast())
		called.Store(true)
		assert.NoError(t, err)
		assert.Empty(t, results)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "emptyFanout", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait a bit for job to run
	time.Sleep(500 * time.Millisecond)
	assert.True(t, called.Load())
}

func TestSub_DefaultValues(t *testing.T) {
	subJob := fanout.Sub("testJob", map[string]int{"key": 42})

	assert.Equal(t, "testJob", subJob.Type)
	assert.Equal(t, map[string]int{"key": 42}, subJob.Args)
	// Default retries comes from queue.DefaultJobRetries which is 2
	assert.Equal(t, 2, subJob.Retries)
	assert.Empty(t, subJob.Queue) // Empty means inherit from parent
}

func TestSub_WithOptions(t *testing.T) {
	subJob := fanout.Sub("testJob", "args",
		jobs.QueueOpt("high-priority"),
		jobs.Priority(10),
		jobs.Retries(5),
	)

	assert.Equal(t, "testJob", subJob.Type)
	assert.Equal(t, "high-priority", subJob.Queue)
	assert.Equal(t, 10, subJob.Priority)
	assert.Equal(t, 5, subJob.Retries)
}

func TestFanOutError_Message(t *testing.T) {
	err := &fanout.Error{
		FanOutID:    "test-id",
		TotalCount:  10,
		FailedCount: 3,
		Failures:    []fanout.SubJobFailure{},
	}

	assert.Equal(t, "fan-out failed: 3/10 sub-jobs failed", err.Error())
}

func TestSuspendError(t *testing.T) {
	err := &fanout.SuspendError{FanOutID: "test-123"}
	assert.Contains(t, err.Error(), "test-123")
	assert.True(t, fanout.IsSuspendError(err))
	assert.False(t, fanout.IsSuspendError(fmt.Errorf("regular error")))
	assert.False(t, fanout.IsSuspendError(nil))
}
