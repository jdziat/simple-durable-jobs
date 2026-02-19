package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupHooksTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	return openIntegrationQueue(t)
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
	go func() { _ = worker.Start(ctx) }()

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
	go func() { _ = worker.Start(ctx) }()

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
	go func() { _ = worker.Start(ctx) }()

	time.Sleep(300 * time.Millisecond)

	assert.True(t, failed.Load())
}

func TestHooks_OnRetry(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	var retryAttempt atomic.Int32
	queue.OnRetry(func(ctx context.Context, j *jobs.Job, attempt int, err error) {
		retryAttempt.Store(int32(attempt))
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
	go func() { _ = worker.Start(ctx) }()

	// Wait for retry
	for i := 0; i < 50; i++ {
		if retryAttempt.Load() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int32(1), retryAttempt.Load())
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
	go func() { _ = worker.Start(ctx) }()

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, int32(2), startCount.Load())
	assert.Equal(t, int32(2), completeCount.Load())
}

func TestHooks_JobDataInHook(t *testing.T) {
	queue, _ := setupHooksTestQueue(t)

	type captured struct {
		jobType  string
		jobQueue string
	}
	var result atomic.Pointer[captured]

	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		result.Store(&captured{jobType: j.Type, jobQueue: j.Queue})
	})

	queue.Register("data-hook-test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "data-hook-test", struct{}{}, jobs.QueueOpt("special"))
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WorkerQueue("special", jobs.Concurrency(1)))
	go func() { _ = worker.Start(ctx) }()

	// Poll for hook to be called
	var cap *captured
	for i := 0; i < 30; i++ {
		cap = result.Load()
		if cap != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NotNil(t, cap)
	assert.Equal(t, "data-hook-test", cap.jobType)
	assert.Equal(t, "special", cap.jobQueue)
}
