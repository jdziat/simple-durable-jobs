package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
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

	w.runningJobsMu.Lock()
	w.pauseCancelled[1] = struct{}{}
	w.runningJobsMu.Unlock()

	assert.True(t, w.takePauseCancelled(1), "the first read must see the mark")
	assert.False(t, w.takePauseCancelled(1),
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

	w.runningJobsMu.Lock()
	w.pauseCancelled[1] = struct{}{}
	w.runningJobsMu.Unlock()

	w.Resume()

	assert.True(t, w.takePauseCancelled(1),
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
	w.pauseCancelled[1] = struct{}{}
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	w.processJob(ctx, job)

	assert.False(t, w.takePauseCancelled(1),
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

// TestProcessJob_DropsTheMarkWhenTheHandlerPanics covers the panic path.
//
// SCOPE, corrected after review: an earlier version of this comment claimed the
// test proves a defer-ORDERING property. It does not, and the claim was wrong
// twice over — a handler panic is recovered in executeHandler, so processJob's own
// recovery defer is never entered here; and on a panic unwind every registered
// defer runs regardless of order, which only decides which runs first. What this
// actually pins is the outcome that matters: a job whose handler panicked leaves
// no mark behind, so a genuine failure on its next run cannot be swallowed as a
// pause.
func TestProcessJob_DropsTheMarkWhenTheHandlerPanics(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("boom", func(context.Context, struct{}) error { panic("handler exploded") })

	w := NewWorker(q)
	job := &core.Job{ID: core.NewID(), Type: "boom", Queue: "default", Status: core.StatusRunning, MaxRetries: 2}

	w.runningJobsMu.Lock()
	w.pauseCancelled[1] = struct{}{}
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NotPanics(t, func() { w.processJob(ctx, job) }, "a handler panic must be recovered")

	assert.False(t, w.takePauseCancelled(1),
		"a job whose handler panicked must leave no mark behind, or a genuine failure on its "+
			"next run is silently swallowed as a pause")
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

// TestProcessJob_LaterRunDoesNotLoseItsStateToAnEarlierCleanup guards a
// lifecycle race the pause path introduced, through the REAL code.
//
// Releasing to `pending` while this worker still polls is new: the same job can be
// re-dequeued into a SECOND processJob before the first one's deferred cleanup
// runs. That cleanup must not evict the second run's registration, and — the half
// this originally missed — must not eat its pause mark either. Losing the mark
// sends run #2's pause cancellation down the ordinary failure path and calls Fail,
// which is exactly the attempt-burning this packet exists to prevent.
//
// FALSE-GREEN TRAPS, both of which earlier versions of this test fell into:
//  1. asserting against a hand-rolled copy of the keyed-delete logic passes with
//     the production guard reverted, because the production code never runs;
//  2. pausing run #2 AFTER run #1 has finished closes the window by ordering, so
//     there is no race left to lose. The mark for run #2 must exist BEFORE run
//     #1's cleanup fires.
//
// The interleaving is therefore forced: run #1 blocks inside Release (past its own
// mark), run #2 starts and is marked by a pause, and only THEN is run #1 let go so
// its cleanup lands on run #2's live state. Run #2 holds its cancellation until
// that has happened.
func TestProcessJob_LaterRunDoesNotLoseItsStateToAnEarlierCleanup(t *testing.T) {
	var failCalls atomic.Int64
	store := &mockStorage{}
	store.failFunc = func(context.Context, core.UUID, string, string, *time.Time) error {
		failCalls.Add(1)
		return nil
	}
	releaseGate := make(chan struct{})
	var firstRelease sync.Once
	store.releaseJobFunc = func(context.Context, core.UUID, string) error {
		firstRelease.Do(func() { <-releaseGate }) // hold run #1 before its defer
		return nil
	}

	q := queue.New(store)
	started := make(chan struct{}, 2)
	surfaced := make(chan struct{}) // run #2 waits here after being cancelled
	var runs atomic.Int64
	q.Register("slow", func(ctx context.Context, _ struct{}) error {
		mine := runs.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		if mine == 2 {
			<-surfaced // do not return until run #1's cleanup has run
		}
		return ctx.Err()
	})

	jobID := core.NewID()
	newJob := func() *core.Job {
		return &core.Job{ID: jobID, Type: "slow", Queue: "default", Status: core.StatusRunning, Attempt: 1, MaxRetries: 2}
	}

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	done1 := make(chan struct{})
	go func() { defer close(done1); w.processJob(ctx, newJob()) }()
	<-started
	w.Pause(core.PauseModeAggressive) // marks + cancels run #1; it blocks in Release
	w.Resume()

	done2 := make(chan struct{})
	go func() { defer close(done2); w.processJob(ctx, newJob()) }()
	<-started

	// Mark run #2 while run #1 is STILL unwinding — this is the state whose loss
	// is the bug. Both runs are marked at this point: run #1's mark is its own
	// (already consumed or not), run #2's is separate because marks are keyed by
	// RUN, not by job id.
	w.Pause(core.PauseModeAggressive)
	require.Eventually(t, func() bool {
		w.runningJobsMu.Lock()
		defer w.runningJobsMu.Unlock()
		return len(w.pauseCancelled) > 0
	}, 5*time.Second, 5*time.Millisecond, "run #2 must be marked before run #1 cleans up")

	close(releaseGate) // run #1's deferred cleanup now lands on run #2's state
	select {
	case <-done1:
	case <-time.After(10 * time.Second):
		t.Fatal("run #1 never returned")
	}

	close(surfaced) // now let run #2 surface its cancellation
	select {
	case <-done2:
	case <-time.After(10 * time.Second):
		t.Fatal("run #2 never returned")
	}

	assert.Zero(t, failCalls.Load(),
		"an earlier run's cleanup must not eat a later run's pause mark: losing it sends the "+
			"later run's pause cancellation down the failure path and burns an attempt")
}

// TestPause_AggressiveLogsExactlyOneOutcome guards a defect that survived its own
// first fix: the switch reporting the three release outcomes was added, but the
// original unconditional Info was left BELOW it, so a failed release logged an
// error and then immediately promised "it will re-dispatch on resume" — which is
// false, since the row stays locked until the stale-lock reaper fires ~45 minutes
// later. That is the line an operator greps during exactly that incident.
//
// FALSE-GREEN TRAP: asserting that the success message appears passes with the
// duplicate present, because it appears twice. The assertion has to be on the
// COUNT, and on the failure path it has to be that the message is absent.
func TestPause_AggressiveLogsExactlyOneOutcome(t *testing.T) {
	const promise = "it will re-dispatch on resume"

	run := func(t *testing.T, releaseErr error) string {
		t.Helper()
		var buf bytes.Buffer
		store := &mockStorage{}
		store.releaseJobFunc = func(context.Context, core.UUID, string) error { return releaseErr }

		q := queue.New(store)
		started := make(chan struct{})
		q.Register("slow", func(ctx context.Context, _ struct{}) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})

		w := NewWorker(q)
		w.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		// Keep the release from spending its full retry budget on a failing store.
		w.config.StorageRetry.MaxAttempts = 1

		job := &core.Job{ID: core.NewID(), Type: "slow", Queue: "default", Status: core.StatusRunning, MaxRetries: 2}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
		case <-time.After(15 * time.Second):
			t.Fatal("processJob never returned")
		}
		return buf.String()
	}

	t.Run("a successful release promises the resume exactly once", func(t *testing.T) {
		assert.Equal(t, 1, strings.Count(run(t, nil), promise),
			"the outcome must be reported once, not once per code path that reports it")
	})

	t.Run("a failed release does not promise a resume at all", func(t *testing.T) {
		out := run(t, errors.New("storage unavailable"))
		assert.NotContains(t, out, promise,
			"the job is still locked and nothing re-dispatches it until the stale-lock reaper "+
				"runs; telling the operator otherwise is worst exactly when they are debugging it")
		assert.Contains(t, out, "stays locked until the stale-lock reaper",
			"and the error must say what actually happens")
	})
}

// TestProcessJob_LaterRunKeepsTheQueueRegistration covers the other half of the
// keyed cleanup: the QUEUE-level registry that Queue.CancelJob uses to cancel a
// locally-running job.
//
// An earlier run must not unregister a job a later run now owns, or CancelJob
// silently degrades to a durable-only cancel — the storage row is marked but the
// handler keeps running to completion, which is precisely the "cancel does not
// cancel" behaviour this project has fixed before.
//
// FALSE-GREEN TRAP: asserting on the worker's own runningJobs map tests the wrong
// registry. CancelJob reads the QUEUE's, so the observation has to be that
// cancelling actually reaches the running handler.
func TestProcessJob_LaterRunKeepsTheQueueRegistration(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q)
	jobID := core.NewID()

	// Run #1 registers, then run #2 replaces it — the state after a re-dequeue.
	c1 := func() {}
	var cancelled2 atomic.Bool
	c2 := func() { cancelled2.Store(true) }

	w.runningJobsMu.Lock()
	tok1 := w.nextRunToken.Add(1)
	w.runningJobs[jobID] = runningJobEntry{cancel: c1, token: tok1}
	w.runningJobsMu.Unlock()
	q.RegisterRunningJob(jobID, c1)

	w.runningJobsMu.Lock()
	tok2 := w.nextRunToken.Add(1)
	w.runningJobs[jobID] = runningJobEntry{cancel: c2, token: tok2}
	w.runningJobsMu.Unlock()
	q.RegisterRunningJob(jobID, c2)

	// Run #1's deferred cleanup fires: its guard must fail, so it must NOT touch
	// the queue registry.
	w.runningJobsMu.Lock()
	if cur, ok := w.runningJobs[jobID]; ok && cur.token == tok1 {
		delete(w.runningJobs, jobID)
		w.runningJobsMu.Unlock()
		q.UnregisterRunningJob(jobID)
		w.runningJobsMu.Lock()
	}
	w.runningJobsMu.Unlock()

	require.NoError(t, q.CancelJob(context.Background(), jobID))
	assert.True(t, cancelled2.Load(),
		"an earlier run's cleanup must leave the later run registered with the QUEUE, or "+
			"CancelJob marks the row and never reaches the handler still executing it")
}
