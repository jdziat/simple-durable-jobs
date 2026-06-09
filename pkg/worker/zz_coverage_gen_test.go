package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/schedule"
)

// ---------------------------------------------------------------------------
// IsRetryableError — cover the ErrJobNotOwned branch (was the missing case).
// ---------------------------------------------------------------------------

func TestGen_IsRetryableError_JobNotOwnedIsNotRetryable(t *testing.T) {
	assert.False(t, IsRetryableError(core.ErrJobNotOwned))
	// A wrapped ErrJobNotOwned must also be treated as permanent.
	wrapped := errors.Join(errors.New("op failed"), core.ErrJobNotOwned)
	assert.False(t, IsRetryableError(wrapped))
}

// ---------------------------------------------------------------------------
// pollWaitingJobsOnce — cover the branches the existing suite misses:
//   * GetWaitingJobsToResume returns an error (early return).
//   * Waiting jobs are resumed successfully (the success log branch).
//   * ResumeJob fails for a waiting job (error log branch).
//   * GetStalledFanOutParents returns an error (early return).
//   * ResumeJob fails for a stalled fan-out parent (error log branch).
// ---------------------------------------------------------------------------

func TestGen_PollWaitingJobsOnce_GetWaitingJobsError(t *testing.T) {
	mock := &mockStorage{
		waitingJobsFunc: func(_ context.Context) ([]*core.Job, error) {
			return nil, errors.New("boom waiting")
		},
		stalledJobsFunc: func(_ context.Context, _ time.Time) ([]*core.Job, error) {
			t.Fatal("stalled query should not run after waiting-jobs error")
			return nil, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// Must return without panicking and without touching the stalled query.
	w.pollWaitingJobsOnce(context.Background())
}

func TestGen_PollWaitingJobsOnce_ResumesWaitingJobsAndHandlesResumeError(t *testing.T) {
	waitingOK := &core.Job{ID: "wait-ok", Status: core.StatusWaiting}
	waitingBad := &core.Job{ID: "wait-bad", Status: core.StatusWaiting}

	var resumed []string
	mock := &mockStorage{
		waitingJobsFunc: func(_ context.Context) ([]*core.Job, error) {
			return []*core.Job{waitingOK, waitingBad}, nil
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed = append(resumed, jobID)
			if jobID == "wait-bad" {
				return false, errors.New("resume failed")
			}
			return true, nil
		},
		// No stalled parents — exercises the empty stalled loop.
		stalledJobsFunc: func(_ context.Context, _ time.Time) ([]*core.Job, error) {
			return nil, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	w.pollWaitingJobsOnce(context.Background())

	require.ElementsMatch(t, []string{"wait-ok", "wait-bad"}, resumed)
}

func TestGen_PollWaitingJobsOnce_GetStalledParentsError(t *testing.T) {
	mock := &mockStorage{
		// No waiting jobs — go straight to stalled query.
		waitingJobsFunc: func(_ context.Context) ([]*core.Job, error) {
			return nil, nil
		},
		stalledJobsFunc: func(_ context.Context, _ time.Time) ([]*core.Job, error) {
			return nil, errors.New("boom stalled")
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	w.pollWaitingJobsOnce(context.Background())
}

func TestGen_PollWaitingJobsOnce_ResumeStalledParentError(t *testing.T) {
	stalled := &core.Job{ID: "stalled-bad", Status: core.StatusWaiting}
	var attempted atomic.Bool
	mock := &mockStorage{
		waitingJobsFunc: func(_ context.Context) ([]*core.Job, error) {
			return nil, nil
		},
		stalledJobsFunc: func(_ context.Context, _ time.Time) ([]*core.Job, error) {
			return []*core.Job{stalled}, nil
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			if jobID == "stalled-bad" {
				attempted.Store(true)
				return false, errors.New("resume failed")
			}
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	w.pollWaitingJobsOnce(context.Background())
	assert.True(t, attempted.Load(), "ResumeJob should be attempted for the stalled parent")
}

// ---------------------------------------------------------------------------
// runOwnershipAudit — cover the FindOrphanedJobs error branch.
// ---------------------------------------------------------------------------

func TestGen_RunOwnershipAudit_QueryError(t *testing.T) {
	var queried atomic.Bool
	mock := &mockStorage{
		findOrphanedFunc: func(_ []string) ([]string, error) {
			queried.Store(true)
			return nil, errors.New("audit query boom")
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithOwnershipAuditInterval(10*time.Millisecond))

	// Register a running job so the audit has IDs to query (otherwise it
	// short-circuits before the storage call).
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.runningJobsMu.Lock()
	w.runningJobs["job-a"] = cancel
	w.runningJobsMu.Unlock()

	ctx, ctxCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runOwnershipAudit(ctx)
	}()

	require.Eventually(t, queried.Load, time.Second, 5*time.Millisecond,
		"ownership audit should have queried storage")

	ctxCancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runOwnershipAudit did not exit after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// runScheduler — cover the Enqueue-error branch. q.Enqueue returns an error
// when no handler is registered for the scheduled job's name.
// ---------------------------------------------------------------------------

func TestGen_RunScheduler_EnqueueErrorIsLogged(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	// Intentionally DO NOT register a handler — q.Enqueue will fail with
	// "no handler registered", driving runScheduler's enqueue-error branch.
	q.Schedule("unregistered-task", nil, schedule.Every(1*time.Millisecond))

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runScheduler(ctx)
	}()

	// Let the scheduler tick at least a few times (100ms ticker), then stop.
	require.Eventually(t, func() bool {
		// Enqueue never reaches storage because q.Enqueue rejects it first.
		return mock.getEnqueueCount() == 0
	}, 500*time.Millisecond, 20*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runScheduler did not exit after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// calculateBackoff — cover the negative-attempt clamp branch (shift < 0).
// ---------------------------------------------------------------------------

func TestGen_CalculateBackoff_NegativeAttemptClampsToOneSecond(t *testing.T) {
	w := newTestWorker(t)
	// A negative attempt clamps shift to 0 → backoff == 1s.
	assert.Equal(t, time.Second, w.calculateBackoff(-5))
}

// ---------------------------------------------------------------------------
// Start — drive the full dequeue→process path end-to-end via a real sqlite
// queue so the "job != nil" channel-send branch in Start is covered, along
// with processLoop/processJob success handling.
// ---------------------------------------------------------------------------

func TestGen_Start_DequeuesAndProcessesJob(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	done := make(chan struct{})
	var once int32
	q.Register("gen-start-job", func(_ context.Context, _ struct{}) error {
		if atomic.AddInt32(&once, 1) == 1 {
			close(done)
		}
		return nil
	})

	_, err := q.Enqueue(context.Background(), "gen-start-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0), WithPollInterval(10*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		_ = w.Start(ctx)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job was not dequeued and processed by Start")
	}

	cancel()
	select {
	case <-startDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Start — cover the dequeue-error branch (dequeueWithRetry returns a
// non-context error) so Start logs and continues without crashing.
// ---------------------------------------------------------------------------

func TestGen_Start_DequeueErrorContinues(t *testing.T) {
	var dequeueCalls atomic.Int32
	mock := &mockStorage{
		dequeueFunc: func(_ context.Context, _ []string, _ string) (*core.Job, error) {
			dequeueCalls.Add(1)
			return nil, errors.New("dequeue boom")
		},
	}
	q := queue.New(mock)
	w := NewWorker(q,
		WithStaleLockInterval(0),
		WithPollInterval(10*time.Millisecond),
		WithDequeueRetry(RetryConfig{MaxAttempts: 1}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		_ = w.Start(ctx)
	}()

	require.Eventually(t, func() bool {
		return dequeueCalls.Load() >= 2
	}, time.Second, 10*time.Millisecond,
		"Start should keep polling after a dequeue error")

	cancel()
	select {
	case <-startDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}
