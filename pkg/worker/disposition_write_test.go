package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// collectEvents drains everything the subscription buffered. Emit is synchronous
// and the buffer is 100 deep, so by the time processJob returns every event it
// produced is already queued — no polling needed.
func collectEvents(ch <-chan core.Event) []core.Event {
	var out []core.Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func hasEvent[T core.Event](events []core.Event) bool {
	for _, ev := range events {
		if _, ok := ev.(T); ok {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// (a) a non-positive MaxAttempts must never mean "skip the write, report success"
// ---------------------------------------------------------------------------

func TestRetryWithBackoff_NonPositiveMaxAttemptsStillRunsOnce(t *testing.T) {
	for _, attempts := range []int{0, -3} {
		attempts := attempts
		t.Run("", func(t *testing.T) {
			sentinel := errors.New("storage write failed")
			var calls int

			err := retryWithBackoff(context.Background(), RetryConfig{MaxAttempts: attempts}, func() error {
				calls++
				return sentinel
			})

			assert.Equal(t, 1, calls, "operation must run at least once")
			assert.ErrorIs(t, err, sentinel, "must return the real error, not a fabricated nil")
		})
	}
}

func TestNewWorker_ClampsNonPositiveRetryAttempts(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithRetryAttempts(0),
		WithDequeueRetry(RetryConfig{MaxAttempts: -2}),
	)

	require.NotNil(t, w.config.StorageRetry)
	require.NotNil(t, w.config.DequeueRetry)
	assert.Equal(t, 1, w.config.StorageRetry.MaxAttempts)
	assert.Equal(t, 1, w.config.DequeueRetry.MaxAttempts)
}

func TestWorker_ZeroRetryAttemptsDoesNotReportUnwrittenCompletion(t *testing.T) {
	var completeCalls atomic.Int32
	store := &atomicMockStorage{mockStorage: &mockStorage{}}
	store.completeWithResultFunc = func(context.Context, core.UUID, string, []byte) (*core.FanOut, error) {
		completeCalls.Add(1)
		return nil, nil
	}

	q := queue.New(store)
	q.Register("leaf", func(context.Context, struct{}) error { return nil })
	var hooks atomic.Int32
	q.OnJobComplete(func(context.Context, *core.Job) { hooks.Add(1) })

	w := NewWorker(q, WithRetryAttempts(0))
	w.processJob(context.Background(), &core.Job{
		ID: "zero-attempts", Type: "leaf", Queue: "default", Args: []byte(`{}`),
	})

	// THE discriminating assertion: the completion write must actually happen.
	assert.Equal(t, int32(1), completeCalls.Load(),
		"the completion write must be attempted, not skipped")
	assert.Equal(t, int32(1), hooks.Load())
}

// ---------------------------------------------------------------------------
// (b) a failed disposition write must not be reported as a disposition
// ---------------------------------------------------------------------------

func TestWorker_RetryDispositionWriteErrorReleasesWithoutEvent(t *testing.T) {
	writeErr := errors.New("fail write unavailable")
	var failCalls atomic.Int32
	store := &mockStorage{
		failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
			require.NotNil(t, retryAt, "this must be the retry disposition, not the terminal one")
			failCalls.Add(1)
			return writeErr
		},
	}

	q := queue.New(store)
	q.Register("boom", func(context.Context, struct{}) error { return errors.New("handler failed") })
	var retryHooks atomic.Int32
	q.OnRetry(func(context.Context, *core.Job, int, error) { retryHooks.Add(1) })
	events := q.Events()
	defer q.Unsubscribe(events)

	w := NewWorker(q, DisableRetry())
	w.processJob(context.Background(), &core.Job{
		ID: "retry-write-error", Type: "boom", Queue: "default", Args: []byte(`{}`),
		Attempt: 1, MaxRetries: 3,
	})

	assert.Equal(t, int32(1), failCalls.Load(), "the retry write must have been attempted")
	assert.Equal(t, int32(0), retryHooks.Load(),
		"retry hooks must not fire for a retry that was never persisted")
	assert.False(t, hasEvent[*core.JobRetrying](collectEvents(events)),
		"JobRetrying must not be emitted for a retry that was never persisted")
	assert.Equal(t, []core.UUID{core.UUID("retry-write-error")}, store.getReleasedJobIDs(),
		"the job must be released for reclaim instead of stranded 'running'")
}

func TestWorker_LegacyTerminalWriteErrorReleasesWithoutFanOutAccounting(t *testing.T) {
	writeErr := errors.New("fail write unavailable")
	fanOutID := core.UUID("fo-terminal-write-error")
	var failedIncrements atomic.Int32
	store := &mockStorage{
		failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
			require.Nil(t, retryAt, "attempts are exhausted; this must be the terminal disposition")
			return writeErr
		},
		incrementFailedFunc: func(context.Context, core.UUID) (*core.FanOut, error) {
			failedIncrements.Add(1)
			return nil, nil
		},
	}

	q := queue.New(store)
	q.Register("boom", func(context.Context, struct{}) error { return errors.New("handler failed") })
	var failHooks atomic.Int32
	q.OnJobFail(func(context.Context, *core.Job, error) { failHooks.Add(1) })
	events := q.Events()
	defer q.Unsubscribe(events)

	w := NewWorker(q, DisableRetry())
	fanOutIDCopy := fanOutID
	w.processJob(context.Background(), &core.Job{
		ID: "terminal-write-error", Type: "boom", Queue: "default", Args: []byte(`{}`),
		Attempt: 3, MaxRetries: 3, FanOutID: &fanOutIDCopy,
	})

	assert.Equal(t, int32(0), failHooks.Load(),
		"fail hooks must not fire for a terminal failure that was never persisted")
	assert.False(t, hasEvent[*core.JobFailed](collectEvents(events)),
		"JobFailed must not be emitted for a terminal failure that was never persisted")
	assert.Equal(t, int32(0), failedIncrements.Load(),
		"the non-idempotent fan-out failed counter must not advance for an unwritten failure")
	assert.Equal(t, []core.UUID{core.UUID("terminal-write-error")}, store.getReleasedJobIDs(),
		"the job must be released for reclaim instead of stranded 'running'")
}

func TestWorker_NoHandlerWriteErrorReleasesWithoutFanOutAccounting(t *testing.T) {
	writeErr := errors.New("fail write unavailable")
	fanOutID := core.UUID("fo-no-handler-write-error")
	var failedIncrements atomic.Int32
	store := &mockStorage{
		failFunc: func(context.Context, core.UUID, string, string, *time.Time) error {
			return writeErr
		},
		incrementFailedFunc: func(context.Context, core.UUID) (*core.FanOut, error) {
			failedIncrements.Add(1)
			return nil, nil
		},
	}

	q := queue.New(store)
	w := NewWorker(q, DisableRetry())
	fanOutIDCopy := fanOutID
	w.processJob(context.Background(), &core.Job{
		ID: "no-handler-write-error", Type: "missing-handler", Queue: "default",
		Attempt: 1, MaxRetries: 1, FanOutID: &fanOutIDCopy,
	})

	assert.Equal(t, int32(0), failedIncrements.Load(),
		"the fan-out failed counter must not advance for an unwritten no-handler failure")
	assert.Equal(t, []core.UUID{core.UUID("no-handler-write-error")}, store.getReleasedJobIDs(),
		"the job must be released for reclaim instead of stranded 'running'")
}
