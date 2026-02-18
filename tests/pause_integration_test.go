package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPauseIntegration_JobLevelPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var processed atomic.Int32
	q.Register("count-job", func(ctx context.Context, args struct{}) error {
		processed.Add(1)
		return nil
	})

	// Enqueue two jobs
	job1ID, err := q.Enqueue(ctx, "count-job", struct{}{})
	require.NoError(t, err)
	job2ID, err := q.Enqueue(ctx, "count-job", struct{}{})
	require.NoError(t, err)
	_ = job2ID // unused in comparison

	// Pause job1
	err = q.PauseJob(ctx, job1ID)
	require.NoError(t, err)

	// Start worker briefly
	w := jobs.NewWorker(q)
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()
	<-workerCtx.Done()

	// Only job2 should be processed (job1 is paused)
	assert.Equal(t, int32(1), processed.Load())

	// Job1 should still be paused
	paused, err := q.IsJobPaused(ctx, job1ID)
	require.NoError(t, err)
	assert.True(t, paused)

	// Resume job1
	err = q.ResumeJob(ctx, job1ID)
	require.NoError(t, err)

	// Run worker again
	workerCtx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	w2 := jobs.NewWorker(q)
	go func() { _ = w2.Start(workerCtx2) }()
	<-workerCtx2.Done()

	// Now both should be processed
	assert.Equal(t, int32(2), processed.Load())
}

func TestPauseIntegration_QueueLevelPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var emailsProcessed, othersProcessed atomic.Int32

	q.Register("email-job", func(ctx context.Context, args struct{}) error {
		emailsProcessed.Add(1)
		return nil
	})
	q.Register("other-job", func(ctx context.Context, args struct{}) error {
		othersProcessed.Add(1)
		return nil
	})

	// Enqueue jobs to different queues
	_, err := q.Enqueue(ctx, "email-job", struct{}{}, jobs.QueueOpt("emails"))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "other-job", struct{}{}, jobs.QueueOpt("other"))
	require.NoError(t, err)

	// Pause emails queue
	err = q.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	// Start worker for both queues
	w := jobs.NewWorker(q, jobs.WorkerQueue("emails"), jobs.WorkerQueue("other"))
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()
	<-workerCtx.Done()

	// Only other queue should be processed
	assert.Equal(t, int32(0), emailsProcessed.Load())
	assert.Equal(t, int32(1), othersProcessed.Load())
}

func TestPauseIntegration_WorkerGracefulPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var processed atomic.Int32
	started := make(chan struct{})

	q.Register("slow-job", func(ctx context.Context, args struct{}) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	// Enqueue a slow job
	_, err := q.Enqueue(ctx, "slow-job", struct{}{})
	require.NoError(t, err)

	// Start worker
	w := jobs.NewWorker(q)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()

	// Wait for job to start
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	// Graceful pause should let the job complete
	w.Pause(jobs.PauseModeGraceful)
	assert.True(t, w.IsPaused())

	// Wait a bit for job to complete
	time.Sleep(300 * time.Millisecond)

	// Job should have completed
	assert.Equal(t, int32(1), processed.Load())

	cancel()
}

func TestPauseIntegration_WorkerAggressivePause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	started := make(chan struct{})
	cancelled := make(chan struct{})

	q.Register("blocking-job", func(ctx context.Context, args struct{}) error {
		close(started)
		<-ctx.Done() // Wait for cancellation
		close(cancelled)
		return ctx.Err()
	})

	// Enqueue a blocking job
	_, err := q.Enqueue(ctx, "blocking-job", struct{}{})
	require.NoError(t, err)

	// Start worker
	w := jobs.NewWorker(q)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()

	// Wait for job to start
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	// Aggressive pause should cancel the job
	w.Pause(jobs.PauseModeAggressive)
	assert.True(t, w.IsPaused())
	assert.Equal(t, jobs.PauseModeAggressive, w.PauseMode())

	// Job should be cancelled
	select {
	case <-cancelled:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("job was not cancelled")
	}

	cancel()
}

func TestPauseIntegration_WorkerResumeAfterPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var processed atomic.Int32

	q.Register("quick-job", func(ctx context.Context, args struct{}) error {
		processed.Add(1)
		return nil
	})

	// Enqueue jobs
	_, err := q.Enqueue(ctx, "quick-job", struct{}{})
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "quick-job", struct{}{})
	require.NoError(t, err)

	// Create worker
	w := jobs.NewWorker(q)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start paused
	w.Pause(jobs.PauseModeGraceful)

	go func() { _ = w.Start(workerCtx) }()

	// Wait a bit - no jobs should be processed
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(0), processed.Load())

	// Resume
	w.Resume()
	assert.False(t, w.IsPaused())

	// Wait for jobs to be processed
	time.Sleep(300 * time.Millisecond)

	// Both jobs should be processed
	assert.Equal(t, int32(2), processed.Load())

	cancel()
}

func TestPauseIntegration_QueueResume(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var processed atomic.Int32

	q.Register("queue-job", func(ctx context.Context, args struct{}) error {
		processed.Add(1)
		return nil
	})

	// Enqueue a job
	_, err := q.Enqueue(ctx, "queue-job", struct{}{}, jobs.QueueOpt("pausable"))
	require.NoError(t, err)

	// Pause the queue
	err = q.PauseQueue(ctx, "pausable")
	require.NoError(t, err)

	// Start worker for the paused queue
	w := jobs.NewWorker(q, jobs.WorkerQueue("pausable"))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()

	// Wait a bit - no jobs should be processed (queue is paused)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(0), processed.Load())

	// Resume the queue
	err = q.ResumeQueue(ctx, "pausable")
	require.NoError(t, err)

	// Wait for job to be processed
	time.Sleep(300 * time.Millisecond)

	// Job should now be processed
	assert.Equal(t, int32(1), processed.Load())

	cancel()
}

func TestPauseIntegration_MixedPauseStates(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var queueAProcessed, queueBProcessed atomic.Int32

	q.Register("queue-a-job", func(ctx context.Context, args struct{}) error {
		queueAProcessed.Add(1)
		return nil
	})
	q.Register("queue-b-job", func(ctx context.Context, args struct{}) error {
		queueBProcessed.Add(1)
		return nil
	})

	// Enqueue jobs to both queues
	_, err := q.Enqueue(ctx, "queue-a-job", struct{}{}, jobs.QueueOpt("queue-a"))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "queue-b-job", struct{}{}, jobs.QueueOpt("queue-b"))
	require.NoError(t, err)

	// Pause queue-a but not queue-b
	err = q.PauseQueue(ctx, "queue-a")
	require.NoError(t, err)

	// Start worker for both queues
	w := jobs.NewWorker(q, jobs.WorkerQueue("queue-a"), jobs.WorkerQueue("queue-b"))
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()
	<-workerCtx.Done()

	// Only queue-b should be processed
	assert.Equal(t, int32(0), queueAProcessed.Load())
	assert.Equal(t, int32(1), queueBProcessed.Load())
}

func TestPauseIntegration_PauseWorkerStopsNewJobs(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.New(store)
	ctx := context.Background()

	var processed atomic.Int32
	jobProcessing := make(chan struct{})

	q.Register("tracked-job", func(ctx context.Context, args struct{}) error {
		select {
		case jobProcessing <- struct{}{}:
		default:
		}
		processed.Add(1)
		return nil
	})

	// Start worker first
	w := jobs.NewWorker(q)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	// Enqueue first job and wait for it to process
	_, err := q.Enqueue(ctx, "tracked-job", struct{}{})
	require.NoError(t, err)

	select {
	case <-jobProcessing:
	case <-time.After(2 * time.Second):
		t.Fatal("first job did not process")
	}

	// Pause the worker
	w.Pause(jobs.PauseModeGraceful)

	// Enqueue more jobs while paused
	_, err = q.Enqueue(ctx, "tracked-job", struct{}{})
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "tracked-job", struct{}{})
	require.NoError(t, err)

	// Wait a bit - new jobs should not be processed
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(1), processed.Load())

	// Resume worker
	w.Resume()

	// Wait for remaining jobs to process
	for i := 0; i < 20; i++ {
		if processed.Load() >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int32(3), processed.Load())

	cancel()
}
