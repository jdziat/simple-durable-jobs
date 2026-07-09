package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// TestWorker_PanickingCompleteHook_DoesNotCrashWorker proves the end-to-end
// guarantee behind PKT-02 (finding C2/E2): a panicking user lifecycle hook is
// recovered so the job still reaches its terminal state AND the worker survives
// to process later jobs. OnJobComplete fires only after the job is marked
// completed, so if the hook panic escaped, the processLoop goroutine would die
// and the second job would never complete. We assert both jobs complete.
func TestWorker_PanickingCompleteHook_DoesNotCrashWorker(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	var hookRuns atomic.Int32
	q.Register("panic-complete", func(ctx context.Context, args struct{}) error { return nil })
	q.OnJobComplete(func(_ context.Context, _ *core.Job) {
		hookRuns.Add(1)
		panic("hook boom")
	})

	for range 2 {
		_, err := q.Enqueue(context.Background(), "panic-complete", struct{}{})
		require.NoError(t, err)
	}

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool { return hookRuns.Load() == 2 }, 4*time.Second, 20*time.Millisecond,
		"both jobs must complete despite the OnJobComplete hook panicking on each")
	assert.Equal(t, int32(2), hookRuns.Load())
}

// TestWorker_PanickingFailHook_DoesNotCrashWorker proves the same guarantee on
// the failure path: a panicking OnJobFail hook must not stop the worker from
// terminally failing the job and moving on.
func TestWorker_PanickingFailHook_DoesNotCrashWorker(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	var failHookRuns atomic.Int32
	q.Register("panic-fail", func(ctx context.Context, args struct{}) error {
		return core.NoRetry(assert.AnError)
	})
	q.OnJobFail(func(_ context.Context, _ *core.Job, _ error) {
		failHookRuns.Add(1)
		panic("fail hook boom")
	})

	for range 2 {
		_, err := q.Enqueue(context.Background(), "panic-fail", struct{}{})
		require.NoError(t, err)
	}

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool { return failHookRuns.Load() == 2 }, 4*time.Second, 20*time.Millisecond,
		"both jobs must terminally fail despite the OnJobFail hook panicking on each")
}

// TestWorker_InternalPanicNet_ReleasesJobWithoutCrash pins the defense-in-depth
// top-level recover in processJob: an unexpected LIBRARY-INTERNAL panic (here the
// storage completion path panics, a site NOT covered by the per-callback/handler
// recovers) must be caught by the outermost net rather than crash the worker
// goroutine, and the still-running job must be released back to pending exactly
// once (not double-completed, not stranded locked).
func TestWorker_InternalPanicNet_ReleasesJobWithoutCrash(t *testing.T) {
	fanOutID := core.UUID("net-fanout")
	var completeCalls atomic.Int32
	store := &atomicMockStorage{mockStorage: &mockStorage{}}
	store.completeWithResultFunc = func(_ context.Context, _ core.UUID, _ string, _ []byte) (*core.FanOut, error) {
		completeCalls.Add(1)
		panic("internal completion boom")
	}

	q := queue.New(store)
	q.Register("net-success", func(context.Context, struct{}) error { return nil })
	w := NewWorker(q, DisableRetry(), WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	job := &core.Job{ID: "net-job", Type: "net-success", Queue: "default", FanOutID: &fanOutID}
	require.NotPanics(t, func() { w.processJob(context.Background(), job) },
		"an internal panic must be caught by the top-level net, not crash the worker")
	assert.Equal(t, int32(1), completeCalls.Load(), "completion path must run exactly once (no double-complete)")
	assert.Equal(t, []core.UUID{job.ID}, store.getReleasedJobIDs(),
		"the internal-panic net must release the still-running job back to pending")
}
