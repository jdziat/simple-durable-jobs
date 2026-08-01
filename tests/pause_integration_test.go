package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
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

	// Start the worker and WAIT FOR THE OUTCOME rather than for the clock.
	//
	// This used to give the worker a fixed 500ms budget and assert the count the
	// instant it expired. PollInterval defaults to 100ms and time.NewTicker fires
	// FIRST at 100ms, so that budget bought about four dequeue attempts — and the
	// worker performs a stale-lock sweep before its first poll. Measured on this
	// machine, the second assertion failed 3/20 runs here and 1/20 on the previous
	// commit: a margin that thin fails whenever the box is busy, which is exactly
	// when CI runs it. A positive outcome must be POLLED FOR, never timed.
	w := jobs.NewWorker(q)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()

	require.Eventually(t, func() bool { return processed.Load() == 1 }, 30*time.Second, 10*time.Millisecond,
		"the unpaused job should be picked up and run")

	// Job1 should still be paused. This is an ABSENCE, and an absence cannot be
	// polled for — it needs a bounded settle window. Kept generous relative to the
	// 100ms poll so several dequeue rounds go by with the job left alone, and
	// asserted on the paused flag as well as the count so it cannot pass merely
	// because the worker was slow.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), processed.Load(), "the paused job must not run while it is paused")

	paused, err := q.IsJobPaused(ctx, job1ID)
	require.NoError(t, err)
	assert.True(t, paused)

	// Resume job1
	err = q.ResumeJob(ctx, job1ID)
	require.NoError(t, err)

	// The same worker is still running, so no second worker is needed; the resumed
	// job simply becomes claimable on a subsequent poll.
	require.Eventually(t, func() bool { return processed.Load() == 2 }, 30*time.Second, 10*time.Millisecond,
		"the resumed job should be picked up and run")
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

	// Start worker for both queues. Same rule as the job-level test above: poll for
	// the positive outcome, then use a bounded settle for the absence. A fixed
	// 500ms budget is only ~4 poll ticks and fails under load.
	w := jobs.NewWorker(q, jobs.WorkerQueue("emails"), jobs.WorkerQueue("other"))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = w.Start(workerCtx) }()

	require.Eventually(t, func() bool { return othersProcessed.Load() == 1 }, 30*time.Second, 10*time.Millisecond,
		"the unpaused queue should be drained")

	// The paused queue must stay untouched — an absence, so it gets a settle
	// window rather than a poll.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(0), emailsProcessed.Load(), "the paused queue must not be drained")
	assert.Equal(t, int32(1), othersProcessed.Load(), "and the unpaused queue must not run twice")
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
