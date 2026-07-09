package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/require"
)

// TestWorker_Start_RejectsConcurrentStart (PKT-13 / E1a) proves the atomic
// re-entry guard rejects an overlapping Start so two run loops cannot race the
// worker's shared state.
func TestWorker_Start_RejectsConcurrentStart(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithOwnershipAuditInterval(0), WithPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := make(chan error, 1)
	go func() { first <- w.Start(ctx) }()
	require.Eventually(t, func() bool { return w.started.Load() }, 2*time.Second, 5*time.Millisecond)

	err := w.Start(ctx) // second, concurrent Start
	require.ErrorIs(t, err, ErrWorkerAlreadyStarted)

	cancel()
	select {
	case <-first:
	case <-time.After(3 * time.Second):
		t.Fatal("first Start did not return after cancel")
	}
}

// TestWorker_Start_RestartSucceedsAfterStop (PKT-13 / E1a) proves a stopped
// worker can be restarted (the guard admits a fresh run once the prior one
// returned) and that shuttingDown was reset.
func TestWorker_Start_RestartSucceedsAfterStop(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithOwnershipAuditInterval(0), WithPollInterval(50*time.Millisecond))

	ctx1, cancel1 := context.WithCancel(context.Background())
	ret1 := make(chan error, 1)
	go func() { ret1 <- w.Start(ctx1) }()
	require.Eventually(t, func() bool { return w.started.Load() }, 2*time.Second, 5*time.Millisecond)
	cancel1()
	<-ret1 // first run stopped

	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	err := w.Start(ctx2)
	require.NotErrorIs(t, err, ErrWorkerAlreadyStarted, "a stopped worker must be restartable")
}

// TestWorker_Shutdown_BoundedDrainOnStuckHandler (PKT-13 / E1b) proves shutdown
// does NOT hang when a handler ignores its context: the forced-drain wait is
// bounded, the handler is abandoned, and Start returns. Pre-fix (single shared
// WaitGroup, unbounded Wait), this hangs forever.
func TestWorker_Shutdown_BoundedDrainOnStuckHandler(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	running := make(chan struct{})
	// unblock is intentionally never closed: the handler stays blocked here,
	// BEFORE any DB access, so the goroutine the bounded drain abandons cannot
	// touch the (soon-closed) DB and race teardown. The blocked goroutine is a
	// benign leak (cleaned up at process exit; the suite uses no goroutine leak
	// checker) — this is exactly the ctx-ignoring handler the test needs.
	unblock := make(chan struct{})
	var once sync.Once
	q.Register("stuck", func(_ context.Context, _ struct{}) error {
		once.Do(func() { close(running) })
		<-unblock // deliberately IGNORE ctx cancellation
		return nil
	})
	_, err := q.Enqueue(context.Background(), "stuck", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(20*time.Millisecond), WithDrainTimeout(100*time.Millisecond), WithOwnershipAuditInterval(0))
	w.forcedHandlerDrainGrace = 150 * time.Millisecond // shrink on this instance only (no global mutation)
	ctx, cancel := context.WithCancel(context.Background())
	ret := make(chan error, 1)
	go func() { ret <- w.Start(ctx) }()

	select {
	case <-running:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}
	cancel() // shut down while the handler is stuck ignoring ctx

	select {
	case <-ret: // Start returned — shutdown did NOT hang on the ctx-ignoring handler
	case <-time.After(2 * time.Second): // >> DrainTimeout(100ms) + grace(150ms)
		t.Fatal("shutdown hung on a ctx-ignoring handler (unbounded drain)")
	}
}

// TestWorker_Shutdown_ReleasesUnderNonFailureIsFailure (PKT-13 / E3) is a
// regression guard: a job interrupted by shutdown must end RELEASED (pending,
// unlocked), NOT completed, even under a custom IsFailure that classifies
// cancellation as non-failure. The E3 reordering makes the shutdown-release the
// explicit first-class path (evaluated before IsFailure could zero the error and
// route the job into the completion branch). NB: this asserts the correct OUTCOME
// — in the current code the non-batch completion also fails on the cancelled ctx
// and falls back to a release, so both orderings converge on pending; the fix
// hardens the intent (clean release, no wasted completion attempt) and is robust
// to any completion path that would otherwise succeed despite cancellation.
func TestWorker_Shutdown_ReleasesUnderNonFailureIsFailure(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()
	q.SetIsFailure(func(_ *core.Job, err error) bool { return !errors.Is(err, context.Canceled) })

	running := make(chan struct{})
	var once sync.Once
	q.Register("cancellable", func(ctx context.Context, _ struct{}) error {
		once.Do(func() { close(running) })
		<-ctx.Done() // respect cancellation
		return ctx.Err()
	})
	jobID, err := q.Enqueue(context.Background(), "cancellable", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(20*time.Millisecond), WithDrainTimeout(50*time.Millisecond), WithOwnershipAuditInterval(0))
	ctx, cancel := context.WithCancel(context.Background())
	ret := make(chan error, 1)
	go func() { ret <- w.Start(ctx) }()

	select {
	case <-running:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	select {
	case <-ret:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after cancel")
	}

	j, err := q.Storage().GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, j)
	require.Equal(t, core.StatusPending, j.Status, "shutdown-cancel must RELEASE (not complete) even under a non-failure IsFailure")
	require.Empty(t, j.LockedBy, "released job must have its lock cleared")
}
