package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startExecWorker(t *testing.T, q *jobs.Queue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := q.NewWorker(
		jobs.WorkerQueue("default", jobs.Concurrency(1)),
		jobs.WithPollInterval(50*time.Millisecond),
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func waitForExecJob(t *testing.T, store jobs.Storage, jobID jobs.UUID, want jobs.JobStatus) *jobs.Job {
	t.Helper()
	var got *jobs.Job
	require.Eventually(t, func() bool {
		job, err := store.GetJob(context.Background(), jobID)
		if err != nil || job == nil {
			return false
		}
		got = job
		return job.Status == want
	}, 10*time.Second, 50*time.Millisecond)
	return got
}

func TestExecMiddlewareOrderingAndTransforms(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var _ jobs.ExecutionMiddleware = func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		return next(ctx, job)
	}

	var mu sync.Mutex
	var order []string
	record := func(v string) {
		mu.Lock()
		order = append(order, v)
		mu.Unlock()
	}

	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		record("first-before")
		_, err := next(ctx, job)
		record("first-after")
		return []byte(`"first-result"`), fmt.Errorf("first: %w", err)
	})
	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		record("second-before")
		_, err := next(ctx, job)
		record("second-after")
		return []byte(`"second-result"`), fmt.Errorf("second: %w", err)
	})

	sentinel := errors.New("handler error")
	result, err := q.RunExecutionMiddleware(context.Background(), &jobs.Job{ID: "direct", Type: "direct"}, func(context.Context, *jobs.Job) ([]byte, error) {
		record("handler")
		return []byte(`"handler-result"`), sentinel
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, "first: second: handler error", err.Error())
	assert.JSONEq(t, `"first-result"`, string(result))
	assert.Equal(t, []string{"first-before", "second-before", "handler", "second-after", "first-after"}, order)
}

func TestExecMiddlewareShortCircuitCompletesWithoutHandler(t *testing.T) {
	q, store := openIntegrationQueue(t)
	var called atomic.Bool
	q.Register("short-circuit", func(ctx context.Context, _ struct{}) (string, error) {
		called.Store(true)
		return "handler", nil
	})
	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		return []byte(`"short"`), nil
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "short-circuit", struct{}{})
	require.NoError(t, err)
	waitForExecJob(t, store, id, jobs.StatusCompleted)

	assert.False(t, called.Load(), "short-circuit middleware must not invoke the handler")
	got, err := jobs.LoadResult[string](context.Background(), q, id)
	require.NoError(t, err)
	assert.Equal(t, "short", got)
}

func TestExecMiddlewarePanicErrorPassesThroughChain(t *testing.T) {
	q, store := openIntegrationQueue(t)
	seen := make(chan error, 1)
	q.Register("panic-chain", func(ctx context.Context, _ struct{}) error {
		panic("boom")
	})
	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		result, err := next(ctx, job)
		select {
		case seen <- err:
		default:
		}
		return result, err
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "panic-chain", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)
	job := waitForExecJob(t, store, id, jobs.StatusFailed)
	assert.Contains(t, job.LastError, "panic: boom")

	require.Eventually(t, func() bool {
		select {
		case err := <-seen:
			return err != nil && strings.Contains(err.Error(), "panic: boom")
		default:
			return false
		}
	}, 3*time.Second, 50*time.Millisecond)
}

func TestExecMiddlewareIsFailureSwallowsErrorToCompletion(t *testing.T) {
	q, store := openIntegrationQueue(t)
	sentinel := errors.New("domain ignored")
	var completed atomic.Int32
	var handlerCalls atomic.Int32

	q.Register("ignored-error", func(ctx context.Context, _ struct{}) error {
		handlerCalls.Add(1)
		return sentinel
	})
	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		_, err := next(ctx, job)
		return []byte(`"accepted"`), err
	})
	q.SetIsFailure(func(job *jobs.Job, err error) bool {
		return !errors.Is(err, sentinel)
	})
	q.OnJobComplete(func(context.Context, *jobs.Job) {
		completed.Add(1)
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "ignored-error", struct{}{}, jobs.Retries(5))
	require.NoError(t, err)
	waitForExecJob(t, store, id, jobs.StatusCompleted)

	assert.Equal(t, int32(1), handlerCalls.Load())
	assert.Equal(t, int32(1), completed.Load())
	got, err := jobs.LoadResult[string](context.Background(), q, id)
	require.NoError(t, err)
	assert.Equal(t, "accepted", got)
}

func TestExecMiddlewareIsFailureTrueFails(t *testing.T) {
	q, store := openIntegrationQueue(t)
	sentinel := errors.New("real failure")
	noRetry := jobs.NoRetry(sentinel)

	q.Register("real-error", func(ctx context.Context, _ struct{}) error {
		return noRetry
	})
	q.SetIsFailure(func(job *jobs.Job, err error) bool {
		return true
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "real-error", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)
	job := waitForExecJob(t, store, id, jobs.StatusFailed)
	assert.Contains(t, job.LastError, sentinel.Error())
}

func TestExecMiddlewareErrorHandlerObservesFinalErrorOnce(t *testing.T) {
	q, store := openIntegrationQueue(t)
	sentinel := errors.New("terminal observed")
	noRetry := jobs.NoRetry(sentinel)
	var seen atomic.Int32
	var seenJobID atomic.Value
	var seenErr atomic.Value

	q.Register("observed-error", func(ctx context.Context, _ struct{}) error {
		return noRetry
	})
	q.SetErrorHandler(func(ctx context.Context, job *jobs.Job, err error) {
		seen.Add(1)
		seenJobID.Store(job.ID)
		seenErr.Store(err)
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "observed-error", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)
	waitForExecJob(t, store, id, jobs.StatusFailed)

	assert.Equal(t, int32(1), seen.Load())
	assert.Equal(t, id, seenJobID.Load())
	assert.Same(t, noRetry, seenErr.Load())
}

func TestExecMiddlewareWaitingPassthrough(t *testing.T) {
	q, store := openIntegrationQueue(t)
	var middlewareCalls atomic.Int32
	q.Register("wait-with-middleware", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.WaitForSignal[string](ctx, "resume")
		return err
	})
	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		middlewareCalls.Add(1)
		return next(ctx, job)
	})
	q.SetIsFailure(func(job *jobs.Job, err error) bool {
		t.Fatalf("IsFailure must not be called for waiting errors")
		return true
	})
	q.SetErrorHandler(func(ctx context.Context, job *jobs.Job, err error) {
		t.Fatalf("ErrorHandler must not be called for waiting errors")
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "wait-with-middleware", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)
	waitForExecJob(t, store, id, jobs.StatusWaiting)

	job, err := store.GetJob(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, jobs.StatusWaiting, job.Status)
	assert.Empty(t, job.LastError)
	assert.Equal(t, int32(1), middlewareCalls.Load())
}

func TestExecMiddlewareBackCompatNoMiddleware(t *testing.T) {
	q, store := openIntegrationQueue(t)
	var completed atomic.Int32
	var failed atomic.Int32

	q.Register("plain-success", func(ctx context.Context, _ struct{}) error {
		return nil
	})
	q.Register("plain-failure", func(ctx context.Context, _ struct{}) error {
		return errors.New("plain failed")
	})
	q.OnJobComplete(func(context.Context, *jobs.Job) {
		completed.Add(1)
	})
	q.OnJobFail(func(context.Context, *jobs.Job, error) {
		failed.Add(1)
	})
	startExecWorker(t, q)

	successID, err := q.Enqueue(context.Background(), "plain-success", struct{}{})
	require.NoError(t, err)
	failureID, err := q.Enqueue(context.Background(), "plain-failure", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)

	waitForExecJob(t, store, successID, jobs.StatusCompleted)
	failJob := waitForExecJob(t, store, failureID, jobs.StatusFailed)
	assert.Equal(t, "plain failed", failJob.LastError)
	assert.Equal(t, int32(1), completed.Load())
	assert.Equal(t, int32(1), failed.Load())
}

func TestExecMiddlewareWrapsWholeCallWorkflowOnce(t *testing.T) {
	q, store := openIntegrationQueue(t)
	var middlewareCalls atomic.Int32
	var stepCalls atomic.Int32

	q.Register("exec-step-one", func(ctx context.Context, in int) (int, error) {
		stepCalls.Add(1)
		return in + 1, nil
	})
	q.Register("exec-step-two", func(ctx context.Context, in int) (int, error) {
		stepCalls.Add(1)
		return in * 2, nil
	})
	q.Register("exec-workflow", func(ctx context.Context, _ struct{}) (int, error) {
		a, err := jobs.Call[int](ctx, "exec-step-one", 2)
		if err != nil {
			return 0, err
		}
		return jobs.Call[int](ctx, "exec-step-two", a)
	})
	q.UseExecutionMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) ([]byte, error)) ([]byte, error) {
		middlewareCalls.Add(1)
		return next(ctx, job)
	})
	startExecWorker(t, q)

	id, err := q.Enqueue(context.Background(), "exec-workflow", struct{}{})
	require.NoError(t, err)
	waitForExecJob(t, store, id, jobs.StatusCompleted)

	got, err := jobs.LoadResult[int](context.Background(), q, id)
	require.NoError(t, err)
	assert.Equal(t, 6, got)
	assert.Equal(t, int32(2), stepCalls.Load())
	assert.Equal(t, int32(1), middlewareCalls.Load(), "middleware wraps the attempt, not individual Call checkpoints")
}
