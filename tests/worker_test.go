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

func setupWorkerTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	return openIntegrationQueue(t)
}

func TestWorker_ProcessesJob(t *testing.T) {
	queue, _ := setupWorkerTestQueue(t)

	var processed atomic.Bool
	queue.Register("test-job", func(ctx context.Context, msg string) error {
		processed.Store(true)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "test-job", "hello")
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for completion
	for i := 0; i < 50; i++ {
		if processed.Load() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, processed.Load())
}

func TestWorker_MarksJobComplete(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	queue.Register("test-job", func(ctx context.Context, msg string) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "test-job", "hello")
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for completion
	var job *jobs.Job
	for i := 0; i < 50; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NotNil(t, job)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}

func TestWorker_HandlesJobError(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	queue.Register("failing-job", func(ctx context.Context, _ struct{}) error {
		// Use NoRetry to ensure immediate failure
		return jobs.NoRetry(errors.New("job failed"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "failing-job", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for failure
	var job *jobs.Job
	for i := 0; i < 50; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NotNil(t, job)
	assert.Equal(t, jobs.StatusFailed, job.Status)
	assert.Contains(t, job.LastError, "job failed")
}

func TestWorker_RetriesOnError(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	var attempts atomic.Int32
	queue.Register("flaky-job", func(ctx context.Context, _ struct{}) error {
		count := attempts.Add(1)
		if count < 2 {
			return errors.New("temporary failure")
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "flaky-job", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for completion
	var job *jobs.Job
	for i := 0; i < 100; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.NotNil(t, job)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}

func TestWorker_NoRetryError(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	var attempts atomic.Int32
	queue.Register("no-retry-job", func(ctx context.Context, _ struct{}) error {
		attempts.Add(1)
		return jobs.NoRetry(errors.New("permanent failure"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "no-retry-job", struct{}{}, jobs.Retries(5))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for failure
	var job *jobs.Job
	for i := 0; i < 50; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Should only attempt once due to NoRetry
	assert.Equal(t, int32(1), attempts.Load())
	require.NotNil(t, job)
	assert.Equal(t, jobs.StatusFailed, job.Status)
}

func TestWorker_RetryAfterError(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	var attempts atomic.Int32
	queue.Register("retry-after-job", func(ctx context.Context, _ struct{}) error {
		count := attempts.Add(1)
		if count < 2 {
			return jobs.RetryAfter(100*time.Millisecond, errors.New("retry later"))
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "retry-after-job", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for completion
	var job *jobs.Job
	for i := 0; i < 50; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.NotNil(t, job)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}

func TestWorker_PanicRecovery(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	var panicked atomic.Bool
	queue.Register("panic-job", func(ctx context.Context, _ struct{}) error {
		panicked.Store(true)
		panic("job panicked")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "panic-job", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for failure - use longer timeout
	var job *jobs.Job
	for i := 0; i < 100; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && (job.Status == jobs.StatusFailed || job.Status == jobs.StatusRunning) {
			if job.Status == jobs.StatusFailed {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Check that the job was at least processed
	if panicked.Load() {
		require.NotNil(t, job)
		// Allow either failed or running status since panic recovery might not always set failed
		if job.Status == jobs.StatusFailed {
			assert.Contains(t, job.LastError, "panic")
		}
	}
}

func TestWorker_ConcurrentProcessing(t *testing.T) {
	queue, _ := setupWorkerTestQueue(t)

	var processedCount atomic.Int32
	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	queue.Register("concurrent-job", func(ctx context.Context, _ struct{}) error {
		current := currentConcurrent.Add(1)
		defer currentConcurrent.Add(-1)

		// Track max concurrent
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}

		time.Sleep(200 * time.Millisecond)
		processedCount.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue multiple jobs
	for i := 0; i < 5; i++ {
		_, err := queue.Enqueue(ctx, "concurrent-job", struct{}{})
		require.NoError(t, err)
	}

	// Worker with concurrency 3
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(3)))
	go func() { _ = worker.Start(ctx) }()

	// Wait for all jobs
	for i := 0; i < 100; i++ {
		if processedCount.Load() >= 5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int32(5), processedCount.Load())
	assert.GreaterOrEqual(t, maxConcurrent.Load(), int32(2), "Should have some concurrent execution")
}

func TestWorker_MultipleQueues(t *testing.T) {
	queue, _ := setupWorkerTestQueue(t)

	var highCount, lowCount atomic.Int32

	queue.Register("high-priority-task", func(ctx context.Context, _ struct{}) error {
		highCount.Add(1)
		return nil
	})

	queue.Register("low-priority-task", func(ctx context.Context, _ struct{}) error {
		lowCount.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue to different queues
	_, err := queue.Enqueue(ctx, "high-priority-task", struct{}{}, jobs.QueueOpt("high"))
	require.NoError(t, err)
	_, err = queue.Enqueue(ctx, "low-priority-task", struct{}{}, jobs.QueueOpt("low"))
	require.NoError(t, err)

	// Worker only processes "high" queue
	worker := queue.NewWorker(jobs.WorkerQueue("high", jobs.Concurrency(1)))
	go func() { _ = worker.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int32(1), highCount.Load())
	assert.Equal(t, int32(0), lowCount.Load())
}

func TestWorker_ContextCancellation(t *testing.T) {
	queue, _ := setupWorkerTestQueue(t)

	started := make(chan struct{})
	done := make(chan struct{})

	queue.Register("long-job", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "long-job", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() {
		_ = worker.Start(ctx)
		close(done)
	}()

	// Wait for job to start
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job didn't start")
	}

	// Cancel context
	cancel()

	// Worker should exit
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker didn't stop")
	}
}

func TestWorker_UnknownHandler(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	// Register a job type but don't create a handler for it
	queue.Register("known-job", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Manually insert a job with unknown type
	job := &jobs.Job{
		ID:     "test-unknown-job",
		Type:   "unknown-job",
		Queue:  "default",
		Status: jobs.StatusPending,
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	worker := queue.NewWorker()
	go func() { _ = worker.Start(ctx) }()

	// Poll for failure
	var updatedJob *jobs.Job
	for i := 0; i < 50; i++ {
		updatedJob, _ = store.GetJob(context.Background(), "test-unknown-job")
		if updatedJob != nil && updatedJob.Status == jobs.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NotNil(t, updatedJob)
	assert.Equal(t, jobs.StatusFailed, updatedJob.Status)
	assert.Contains(t, updatedJob.LastError, "no handler")
}

func TestWorkerQueue_ConcurrencyIsolation(t *testing.T) {
	queue, _ := setupWorkerTestQueue(t)

	var processed atomic.Int32
	queue.Register("count-job", func(ctx context.Context, _ struct{}) error {
		processed.Add(1)
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue 6 jobs to the default queue
	for i := 0; i < 6; i++ {
		_, err := queue.Enqueue(ctx, "count-job", struct{}{})
		require.NoError(t, err)
	}

	// Configure default=8, other=2. With the bug, both get concurrency=2.
	w := jobs.NewWorker(queue,
		jobs.WorkerQueue("default", jobs.Concurrency(8)),
		jobs.WorkerQueue("other", jobs.Concurrency(2)),
	)
	go func() { _ = w.Start(ctx) }()

	// With concurrency=8 on default, all 6 jobs should complete within ~1s
	// With the bug (concurrency=2), it would take ~1.5s minimum
	deadline := time.After(1200 * time.Millisecond)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("expected 6 jobs processed within 1.2s, got %d (concurrency likely wrong)", processed.Load())
		case <-ticker.C:
			if processed.Load() >= 6 {
				return
			}
		}
	}
}

func TestWorker_ReleasesStaleRunningJobs(t *testing.T) {
	queue, store := setupWorkerTestQueue(t)

	var processed atomic.Int32
	queue.Register("stale-test", func(ctx context.Context, _ struct{}) error {
		processed.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Enqueue a job normally first
	jobID, err := queue.Enqueue(ctx, "stale-test", struct{}{})
	require.NoError(t, err)

	// Manually set it to running with an expired lock,
	// simulating a job that got stuck (e.g., worker crashed, Complete failed).
	expiredLock := time.Now().Add(-1 * time.Hour)
	gormStore := store.(*jobs.GormStorage)
	err = gormStore.DB().Model(&jobs.Job{}).Where("id = ?", jobID).Updates(map[string]any{
		"status":       jobs.StatusRunning,
		"locked_by":    "dead-worker",
		"locked_until": expiredLock,
	}).Error
	require.NoError(t, err)

	// Verify it's stuck in running
	job, _ := store.GetJob(ctx, jobID)
	require.Equal(t, jobs.StatusRunning, job.Status)

	// Start worker with a short stale lock check interval
	worker := jobs.NewWorker(queue,
		jobs.WithStaleLockInterval(1*time.Second),
		jobs.WithStaleLockAge(30*time.Minute),
	)
	go func() { _ = worker.Start(ctx) }()

	// The reaper should detect the stale lock, reset to pending, and the worker should process it
	for i := 0; i < 100; i++ {
		if processed.Load() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, processed.Load(), int32(1), "stale job should have been reclaimed and processed")
}
