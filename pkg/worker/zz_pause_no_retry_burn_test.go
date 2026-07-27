package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pause(PauseModeAggressive) cancels running handler contexts but wrote nothing
// durable, so the handler's context.Canceled fell into the ordinary failure
// path: Fail() burned an attempt, JobRetrying/JobFailed were emitted, and at the
// default MaxRetries — with the attempt already advanced — the job was
// permanently DEAD-LETTERED. By an operation the README presents as the
// reversible half of Pause/Resume.
//
// FALSE-GREEN TRAP: asserting the job stopped running passes with the bug
// present, because a dead-lettered job is not running either. The discriminating
// observations are that Fail was NEVER called and that Release WAS.
func TestPause_AggressiveReleasesInsteadOfFailing(t *testing.T) {
	var failCalls atomic.Int64
	store := &mockStorage{}
	store.failFunc = func(context.Context, core.UUID, string, string, *time.Time) error {
		failCalls.Add(1)
		return nil
	}

	q := queue.New(store)

	started := make(chan struct{})
	q.Register("slow", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-ctx.Done() // observe the pause cancellation
		return ctx.Err()
	})

	job := &core.Job{
		ID: core.NewID(), Type: "slow", Queue: "default",
		Status: core.StatusRunning, Attempt: 1, MaxRetries: 2,
	}

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); w.processJob(ctx, job) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	w.Pause(core.PauseModeAggressive)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processJob never returned after the pause")
	}

	assert.Zero(t, failCalls.Load(),
		"an aggressive pause must NOT travel the failure path — that is what burned the attempt "+
			"and, at the default MaxRetries, dead-lettered a job that was merely paused")

	store.mu.Lock()
	releasedIDs := append([]core.UUID(nil), store.releasedJobIDs...)
	store.mu.Unlock()
	assert.Contains(t, releasedIDs, job.ID,
		"the paused job must be RELEASED so Resume simply re-dispatches it")
}

// TestPause_AggressiveConsumesTheMarkExactlyOnce guards the leak. A job released
// by a pause is re-dispatched on resume; if the mark survived, a GENUINE failure
// on that later run would be silently swallowed as "just a pause" and released
// forever, turning a dead-letter bug into an infinite-retry bug.
func TestPause_AggressiveConsumesTheMarkExactlyOnce(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}))
	id := core.NewID()

	w.runningJobsMu.Lock()
	w.pauseCancelled[id] = struct{}{}
	w.runningJobsMu.Unlock()

	assert.True(t, w.takePauseCancelled(id), "the first read must see the mark")
	assert.False(t, w.takePauseCancelled(id),
		"the mark must be consumed — a surviving mark would mask a real failure on the job's next run")
}

// TestResume_DoesNotClearMarksForStillRunningJobs is the regression guard for a
// gate finding: Resume used to bulk-clear every mark, which reintroduced the very
// dead-lettering this packet exists to prevent. A handler blocked in I/O has not
// yet surfaced its cancellation, so an operator resuming promptly wiped its mark,
// and the context.Canceled arriving a moment later fell through to the ordinary
// failure path — attempt burned, dead-lettered on the last one.
//
// FALSE-GREEN TRAP: asserting that Resume leaves the map alone tests an internal
// detail and would pass even if the mark were never dropped at all, leaking into
// a later run. The invariant is a PAIR — Resume must not clear it, and the job's
// own completion must.
func TestResume_DoesNotClearMarksForStillRunningJobs(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}))
	id := core.NewID()

	w.runningJobsMu.Lock()
	w.pauseCancelled[id] = struct{}{}
	w.runningJobsMu.Unlock()

	w.Resume()

	assert.True(t, w.takePauseCancelled(id),
		"Resume must NOT clear the mark of a job still running: its handler may not have "+
			"surfaced the cancellation yet, and losing the mark dead-letters it")
}

// TestProcessJob_DropsTheUnconsumedMarkOnCompletion is the other half. A handler
// that finished WITHOUT ever surfacing its cancellation leaves a mark nobody
// consumed; if it survived, a genuine failure on the job's next run would be
// silently swallowed as "just a pause" and released forever.
func TestProcessJob_DropsTheUnconsumedMarkOnCompletion(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("quick", func(context.Context, struct{}) error { return nil })

	w := NewWorker(q)
	job := &core.Job{ID: core.NewID(), Type: "quick", Queue: "default", Status: core.StatusRunning}

	// Mark it as if an aggressive pause had cancelled it, then let it finish
	// normally — the handler never observes the cancellation.
	w.runningJobsMu.Lock()
	w.pauseCancelled[job.ID] = struct{}{}
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	w.processJob(ctx, job)

	assert.False(t, w.takePauseCancelled(job.ID),
		"a finished job must leave no mark behind, or a real failure on its next run is "+
			"silently swallowed as a pause and released forever")
}

// TestPause_AggressiveDoesNotStopTheHeartbeat pins the lease. The heartbeat used
// to return early under an aggressive pause, dropping the lease of a job that is
// still running — and a handler mid-I/O may not observe cancellation for some
// time. Once the lease lapses the stale-lock reaper hands the job to a peer while
// the original handler is still executing it: a pause causing double-execution.
func TestPause_AggressiveDoesNotStopTheHeartbeat(t *testing.T) {
	var beats atomic.Int64
	store := &mockStorage{}
	store.heartbeatFunc = func(context.Context, core.UUID, string) error {
		beats.Add(1)
		return nil
	}

	q := queue.New(store)
	started := make(chan struct{})
	release := make(chan struct{})
	q.Register("stubborn", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-release // deliberately ignores cancellation
		return nil
	})

	job := &core.Job{ID: core.NewID(), Type: "stubborn", Queue: "default", Status: core.StatusRunning}

	w := NewWorker(q)
	// No exported option for this; set the internal cadence directly so the test
	// observes several beats inside its budget.
	w.heartbeatInterval = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); w.processJob(ctx, job) }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	w.Pause(core.PauseModeAggressive)
	before := beats.Load()

	require.Eventually(t, func() bool { return beats.Load() > before }, 5*time.Second, 20*time.Millisecond,
		"a still-running job must keep its lease through an aggressive pause; dropping it lets the "+
			"stale-lock reaper hand the job to a peer while this handler is still executing it")

	// Await processJob before the test returns. It keeps writing to the store and
	// logging after the handler unblocks, and a goroutine outliving its test is a
	// source of flaky -race reports and "log after test finished" panics.
	close(release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processJob never returned after the handler unblocked")
	}
}

// TestPause_ResumeBeforeHandlerSurfacesDoesNotDeadLetter is the end-to-end form
// of the same defect, and the shape an operator actually produces: pause, notice
// the queue is drained, resume — all while a handler is still blocked in I/O and
// has not yet returned its cancellation.
//
// FALSE-GREEN TRAP: without the Resume in the middle this is just
// TestPause_AggressiveReleasesInsteadOfFailing and passes with the bug fully
// present. The Resume is the whole test.
func TestPause_ResumeBeforeHandlerSurfacesDoesNotDeadLetter(t *testing.T) {
	var failCalls atomic.Int64
	store := &mockStorage{}
	store.failFunc = func(context.Context, core.UUID, string, string, *time.Time) error {
		failCalls.Add(1)
		return nil
	}

	q := queue.New(store)
	started := make(chan struct{})
	surface := make(chan struct{})
	q.Register("blocked-in-io", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-ctx.Done() // cancellation arrives...
		<-surface    // ...but the handler is mid-I/O and cannot return yet
		return ctx.Err()
	})

	job := &core.Job{
		ID: core.NewID(), Type: "blocked-in-io", Queue: "default",
		Status: core.StatusRunning, Attempt: 1, MaxRetries: 2,
	}

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); w.processJob(ctx, job) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	w.Pause(core.PauseModeAggressive)
	w.Resume() // the operator resumes before the handler has surfaced
	close(surface)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processJob never returned")
	}

	assert.Zero(t, failCalls.Load(),
		"resuming before the handler surfaces its cancellation must not turn the pause back "+
			"into a failure — that burns the attempt and dead-letters on the last one")

	store.mu.Lock()
	releasedIDs := append([]core.UUID(nil), store.releasedJobIDs...)
	store.mu.Unlock()
	assert.Contains(t, releasedIDs, job.ID, "the job must still be released for re-dispatch")
}

// TestProcessJob_DropsTheMarkEvenWhenTheHandlerPanics covers the defer-ordering
// property the per-job cleanup depends on.
//
// processJob registers its panic-recovery defer BEFORE the runningJobs/mark
// cleanup defer, and defers run LIFO — so the cleanup runs first and the mark is
// dropped even on the panic path. If the two were ever reordered, a panicking job
// would leave its mark behind and a genuine failure on its next run would be
// silently swallowed as a pause.
func TestProcessJob_DropsTheMarkEvenWhenTheHandlerPanics(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("boom", func(context.Context, struct{}) error { panic("handler exploded") })

	w := NewWorker(q)
	job := &core.Job{ID: core.NewID(), Type: "boom", Queue: "default", Status: core.StatusRunning, MaxRetries: 2}

	w.runningJobsMu.Lock()
	w.pauseCancelled[job.ID] = struct{}{}
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NotPanics(t, func() { w.processJob(ctx, job) }, "a handler panic must be recovered")

	assert.False(t, w.takePauseCancelled(job.ID),
		"the mark must be dropped on the panic path too — the cleanup defer is registered after "+
			"the recovery defer and LIFO must keep it running first")
}

// TestPause_GenuineErrorDuringAPauseStillFails guards the `selfCancelled &&`
// conjunction, which had no test.
//
// Pause(Aggressive) marks every running job. A handler that returns a GENUINE
// error at the instant a pause lands therefore carries a mark too — and releasing
// on the mark ALONE would drop that failure on the floor: no Fail, no JobFailed,
// no attempt burned, the error silently discarded. The release must additionally
// require the error to actually BE this worker's self-cancel.
//
// FALSE-GREEN TRAP: a handler returning context.Canceled cannot distinguish the
// two guards, since it satisfies both. The discriminating handler returns a
// non-cancellation error while the mark is set.
func TestPause_GenuineErrorDuringAPauseStillFails(t *testing.T) {
	var failCalls atomic.Int64
	store := &mockStorage{}
	store.failFunc = func(context.Context, core.UUID, string, string, *time.Time) error {
		failCalls.Add(1)
		return nil
	}

	q := queue.New(store)
	started := make(chan struct{})
	release := make(chan struct{})
	q.Register("real-failure", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-release
		return errors.New("the database actually fell over")
	})

	job := &core.Job{
		ID: core.NewID(), Type: "real-failure", Queue: "default",
		Status: core.StatusRunning, Attempt: 1, MaxRetries: 3,
	}

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); w.processJob(ctx, job) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	w.Pause(core.PauseModeAggressive) // marks the job
	close(release)                    // ...and it fails for an unrelated reason

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processJob never returned")
	}

	assert.Equal(t, int64(1), failCalls.Load(),
		"a genuine error that lands during a pause must still travel the failure path — "+
			"releasing on the pause mark alone would discard a real failure entirely")
}

// TestPause_AggressiveEndsTheAttemptSpan guards an observability leak.
//
// Every other disposition ends the attempt's span through a hook — complete,
// fail, retry and waiting each have one, and waitingHook's own comment says a
// parked attempt would otherwise "leak a span that is never exported". The pause
// release was the one path that ended none, so an aggressively-paused job leaked
// a span, and pausing a busy worker leaked one per in-flight job at once.
//
// FALSE-GREEN TRAP: asserting the job was released passes with the leak fully
// present — the release was never the missing part. The discriminating
// observation is that the span-ending hook actually fired.
func TestPause_AggressiveEndsTheAttemptSpan(t *testing.T) {
	var waitingHookCalls atomic.Int64

	q := queue.New(&mockStorage{})
	q.OnJobWaiting(func(context.Context, *core.Job) { waitingHookCalls.Add(1) })

	started := make(chan struct{})
	q.Register("slow", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	job := &core.Job{
		ID: core.NewID(), Type: "slow", Queue: "default",
		Status: core.StatusRunning, Attempt: 1, MaxRetries: 2,
	}

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); w.processJob(ctx, job) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}
	w.Pause(core.PauseModeAggressive)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processJob never returned")
	}

	assert.Equal(t, int64(1), waitingHookCalls.Load(),
		"an aggressively-paused attempt must end its span, or every paused job leaks one")
}
