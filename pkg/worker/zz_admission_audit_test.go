package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Admission state (the per-queue counter and the concurrency slots) is registered
// in dispatchDequeuedJobs and released either there, on a bail-out, or by
// processJobRun's defers. Every path must balance: a missed release leaks the
// counter UP until the queue refuses all work, and a double release leaks it DOWN
// so the cap over-admits.
//
// FALSE-GREEN TRAP: asserting only the happy path passes with every bail-out
// broken. Each bail-out reason is driven separately, and the assertion is that the
// counter and the slot map are BOTH back to empty.
func assertAdmissionDrained(t *testing.T, w *Worker, queueName string) {
	t.Helper()
	if counter, ok := w.queueRunning[queueName]; ok {
		assert.Equal(t, int32(0), counter.Load(),
			"per-queue counter must return to zero, or the queue eventually refuses every job")
	}
	w.queueJobIDMu.Lock()
	nq := len(w.queueJobID)
	w.queueJobIDMu.Unlock()
	assert.Zero(t, nq, "no run→queue entry may outlive its run")

	w.slotJobIDMu.Lock()
	ns := len(w.slotJobID)
	w.slotJobIDMu.Unlock()
	assert.Zero(t, ns, "no run→slots entry may outlive its run")
}

func TestDispatch_AdmissionBalancesOnEveryBailOut(t *testing.T) {
	newJobs := func(n int, q string) []*core.Job {
		out := make([]*core.Job, n)
		for i := range out {
			out[i] = &core.Job{ID: core.NewID(), Type: "t", Queue: q, Status: core.StatusRunning}
		}
		return out
	}

	t.Run("queue cap refuses beyond the limit", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(2)))
		ch := make(chan dispatchedJob, 10)
		dispatched, released := w.dispatchDequeuedJobs(context.Background(), ch, newJobs(5, "default"))
		assert.Equal(t, 2, dispatched)
		assert.Equal(t, 3, released, "the three over the cap must be released, not dropped")

		// Drain what was admitted, exactly as processLoop's defers would.
		close(ch)
		for dj := range ch {
			w.untrackQueueJob(dj.token)
			w.releaseConcurrencySlots(context.Background(), dj.job.ID, dj.token)
		}
		assertAdmissionDrained(t, w, "default")
	})

	t.Run("a cancelled context on send releases what it admitted", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(10)))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ch := make(chan dispatchedJob) // unbuffered: the send cannot proceed
		dispatched, released := w.dispatchDequeuedJobs(ctx, ch, newJobs(3, "default"))
		assert.Zero(t, dispatched)
		assert.Equal(t, 3, released)
		assertAdmissionDrained(t, w, "default")
	})

	t.Run("a fully drained batch leaves nothing behind", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(10)))
		ch := make(chan dispatchedJob, 10)
		dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, newJobs(4, "default"))
		require.Equal(t, 4, dispatched)
		close(ch)
		for dj := range ch {
			w.untrackQueueJob(dj.token)
			w.releaseConcurrencySlots(context.Background(), dj.job.ID, dj.token)
		}
		assertAdmissionDrained(t, w, "default")
	})

	t.Run("processJob's own defers drain a directly-dispatched run", func(t *testing.T) {
		q := queue.New(&mockStorage{})
		q.Register("quick", func(context.Context, struct{}) error { return nil })
		w := NewWorker(q, WorkerQueue("default", Concurrency(4)))

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		w.processJob(ctx, &core.Job{ID: core.NewID(), Type: "quick", Queue: "default", Status: core.StatusRunning})
		assertAdmissionDrained(t, w, "default")
	})
}

// TestProcessJob_FreshTokenPathLeaksNothing covers the entry point that was kept
// for direct callers and tests.
//
// processJob allocates a fresh run token and registers NOTHING with dispatch, so
// its defers must find nothing and remove nothing — in particular they must not
// decrement a counter no one incremented, which would let the queue over-admit.
//
// FALSE-GREEN TRAP: with a queue that has no configured cap there is no counter to
// corrupt, so the queue has to be configured for the assertion to mean anything.
func TestProcessJob_FreshTokenPathLeaksNothing(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("quick", func(context.Context, struct{}) error { return nil })
	q.Register("boom", func(context.Context, struct{}) error { panic("exploded") })
	w := NewWorker(q, WorkerQueue("default", Concurrency(3)))

	counter := w.queueRunning["default"]
	require.Equal(t, int32(0), counter.Load())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, typ := range []string{"quick", "boom", "quick"} {
		w.processJob(ctx, &core.Job{
			ID: core.NewID(), Type: typ, Queue: "default",
			Status: core.StatusRunning, MaxRetries: 1,
		})
	}

	assert.Equal(t, int32(0), counter.Load(),
		"a run that never went through dispatch registered no admission state, so its cleanup "+
			"must not decrement a counter nobody incremented — that would let the queue admit "+
			"more than its cap")
	assertAdmissionDrained(t, w, "default")
}

// TestDispatch_TokensAreUniquePerRun guards the property every one of these maps
// now depends on. A repeated token would silently reintroduce the shared-slot bug
// that took three attempts to fix.
func TestDispatch_TokensAreUniquePerRun(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(100)))
	jobID := core.NewID() // deliberately the SAME job id every time
	jobs := make([]*core.Job, 50)
	for i := range jobs {
		jobs[i] = &core.Job{ID: jobID, Type: "t", Queue: "default", Status: core.StatusRunning}
	}

	ch := make(chan dispatchedJob, len(jobs))
	dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, jobs)
	require.Equal(t, len(jobs), dispatched)
	close(ch)

	seen := map[uint64]bool{}
	for dj := range ch {
		require.False(t, seen[dj.token], "run token %d was issued twice", dj.token)
		seen[dj.token] = true
		w.untrackQueueJob(dj.token)
		w.releaseConcurrencySlots(context.Background(), dj.job.ID, dj.token)
	}
	assert.Len(t, seen, len(jobs), "every run must get its own token, even for one job id")
	assertAdmissionDrained(t, w, "default")
}

// TestProcessJob_WaitingSuspendReleasesAdmission covers the fan-out / signal-wait
// path, where the handler parks the job in StatusWaiting and returns without
// completing OR failing.
//
// It is the path most likely to leak admission state, because it returns early
// from the disposition block rather than falling through — and a fan-out parent
// can sit in StatusWaiting for the entire runtime of its children. A leaked
// per-queue slot there would be held for minutes or hours, and a leaked
// concurrency slot would hold a FLEET-wide cap that whole time.
//
// FALSE-GREEN TRAP: asserting the job reached StatusWaiting says nothing about
// admission; the observation has to be that the counter and both run-keyed maps
// are back to empty.
func TestProcessJob_WaitingSuspendReleasesAdmission(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("parks", func(context.Context, struct{}) error {
		return waitingSignal{}
	})
	w := NewWorker(q, WorkerQueue("default", Concurrency(3)))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	w.processJob(ctx, &core.Job{
		ID: core.NewID(), Type: "parks", Queue: "default", Status: core.StatusRunning,
	})

	assert.Equal(t, int32(0), w.queueRunning["default"].Load(),
		"a job parked in StatusWaiting must release its queue slot — a fan-out parent waits for "+
			"the whole runtime of its children, so holding it would stall the queue that long")
	assertAdmissionDrained(t, w, "default")
}

// waitingSignal is the self-suspension control-flow error a fan-out or signal wait
// returns: the handler has already persisted the job as StatusWaiting.
type waitingSignal struct{}

func (waitingSignal) Error() string         { return "job waiting" }
func (waitingSignal) WorkflowWaiting() bool { return true }

// TestProcessJobRun_ReleasesAdmissionRegisteredByDispatch is the END-TO-END
// pairing, and the only test here that can actually see the release defers.
//
// FALSE-GREEN TRAP, which the first version of this file fell into: driving
// processJob directly registers NO admission state, because registration happens
// in dispatchDequeuedJobs on the far side of the channel. Deleting
// `defer w.untrackQueueJob(runToken)` therefore left every one of those tests
// green — there was nothing to untrack. The job has to be admitted the way
// dispatch admits it and then run under the SAME token dispatch issued.
func TestProcessJobRun_ReleasesAdmissionRegisteredByDispatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(context.Context, struct{}) error
	}{
		{"completes", func(context.Context, struct{}) error { return nil }},
		{"fails", func(context.Context, struct{}) error { return assert.AnError }},
		{"panics", func(context.Context, struct{}) error { panic("exploded") }},
		{"parks in waiting", func(context.Context, struct{}) error { return waitingSignal{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := queue.New(&mockStorage{})
			q.Register("t", tc.handler)
			w := NewWorker(q, WorkerQueue("default", Concurrency(3)))
			counter := w.queueRunning["default"]

			ch := make(chan dispatchedJob, 1)
			job := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", Status: core.StatusRunning, MaxRetries: 1}
			dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, []*core.Job{job})
			require.Equal(t, 1, dispatched)
			require.Equal(t, int32(1), counter.Load(), "dispatch must have admitted it")

			dj := <-ch
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			w.processJobRun(ctx, dj.job, dj.token)

			assert.Equal(t, int32(0), counter.Load(),
				"the run must release the queue slot dispatch admitted for it, whatever its "+
					"outcome — a leak here climbs until the queue refuses every job")
			assertAdmissionDrained(t, w, "default")
		})
	}
}

// TestProcessJobRun_ReleasesConcurrencySlotsRegisteredByDispatch is the same
// pairing for the FLEET concurrency slots, which the queue-counter test above
// cannot see.
//
// FALSE-GREEN TRAP, hit while writing the test above: a worker with no
// ConcurrencyCap configured acquires no slots at all, so slotJobID stays empty and
// deleting the release defer changes nothing. The cap has to be configured, and
// the storage has to be one that actually implements slot acquisition.
//
// A leak here is worse than the queue counter: the slot row is FLEET-wide, so a
// held slot throttles every worker in the deployment, not just this one.
func TestProcessJobRun_ReleasesConcurrencySlotsRegisteredByDispatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(context.Context, struct{}) error
	}{
		{"completes", func(context.Context, struct{}) error { return nil }},
		{"fails", func(context.Context, struct{}) error { return assert.AnError }},
		{"parks in waiting", func(context.Context, struct{}) error { return waitingSignal{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := runningJob("job-1", "acme")
			store := newCapMockStorage([]*core.Job{job})
			q := queue.New(store)
			q.Register(job.Type, tc.handler)
			w := NewWorker(q,
				WorkerQueue("default", Concurrency(3)),
				ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
				DisableRetry(),
			)
			w.config.WorkerID = "worker-1"

			ch := make(chan dispatchedJob, 1)
			dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, []*core.Job{job})
			require.Equal(t, 1, dispatched)

			dj := <-ch
			w.slotJobIDMu.Lock()
			held := len(w.slotJobID[dj.token].names)
			w.slotJobIDMu.Unlock()
			require.Positive(t, held, "dispatch must have acquired a cap slot, or this proves nothing")

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			w.processJobRun(ctx, dj.job, dj.token)

			w.slotJobIDMu.Lock()
			remaining := len(w.slotJobID)
			w.slotJobIDMu.Unlock()
			assert.Zero(t, remaining,
				"the run must release the FLEET concurrency slot dispatch acquired for it — a "+
					"held slot throttles every worker in the deployment, not just this one")
		})
	}
}

// TestCancelRun_OnlyCancelsItsOwnRun covers the FIFTH job-id-keyed lookup, which
// the four map re-keyings missed.
//
// runHeartbeat's orphan branch used to call CancelJob(job.ID), which resolves
// whichever run currently owns the id. Since the pause path deliberately allows
// two runs of one id to be alive at once, an orphaned heartbeat belonging to run #1
// reached past it and cancelled a HEALTHY run #2 — which then travelled the failure
// path and burned an attempt it never earned.
//
// FALSE-GREEN TRAP: asserting that cancelling by token works passes with the bug
// present, because the happy case is identical. The discriminating case is a STALE
// token: it must cancel nothing.
func TestCancelRun_OnlyCancelsItsOwnRun(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}))
	jobID := core.NewID()

	var run1Cancelled, run2Cancelled bool
	w.runningJobsMu.Lock()
	tok1 := w.nextRunToken.Add(1)
	w.runningJobs[jobID] = runningJobEntry{cancel: func() { run1Cancelled = true }, token: tok1}
	tok2 := w.nextRunToken.Add(1)
	w.runningJobs[jobID] = runningJobEntry{cancel: func() { run2Cancelled = true }, token: tok2}
	w.runningJobsMu.Unlock()

	// Run #1's orphan condition fires after run #2 has taken over the id.
	assert.False(t, w.cancelRun(jobID, tok1),
		"a stale run token must cancel nothing")
	assert.False(t, run2Cancelled,
		"an orphaned heartbeat belonging to an earlier run must NOT cancel the healthy run that "+
			"replaced it — that run would then fail and burn an attempt it never earned")
	assert.False(t, run1Cancelled, "run #1 is no longer registered, so there is nothing to cancel")

	// The current run is still cancellable by its own token, and by CancelJob.
	assert.True(t, w.cancelRun(jobID, tok2))
	assert.True(t, run2Cancelled)
}

// TestReleaseConcurrencySlots_DoesNotDeleteTheRowALaterRunHolds asserts on the
// DATABASE ROW, which is the whole point.
//
// FALSE-GREEN TRAP, and my own TestProcessJob_LaterRunKeepsItsAdmissionState is
// exactly it: that test states this defect precisely in its comment and then
// asserts only on w.slotJobID, the IN-MEMORY list. Re-keying that map by run token
// does nothing for the row, because concurrency_slots is keyed (slot_name, job_id)
// and capSlotName is deterministic per (cap, job) — so two runs of one job id
// derive the SAME row, run #2 lands in TryAcquireConcurrencySlot's
// idempotent-renewal branch and SHARES it, and run #1's release then deletes the
// fleet cap under a handler that is still executing. The ownership fence cannot
// refuse it (same job id, same worker id) and RenewConcurrencySlot cannot repair
// it (UPDATE-only), so the cap under-counts for run #2's whole remaining runtime.
//
// SECOND TRAP, one level up, and this test fell into it too: the FIRST version
// established run #2 by writing w.runningJobs BEFORE calling
// tryAcquireConcurrencySlots. Production does the reverse — the slot is acquired
// in dispatchDequeuedJobs, the job then crosses jobsChan (a BLOCKING send), and
// only then does processJobRun register — so the test drove an interleaving the
// code cannot produce, passed, and certified a guard that was blind for the whole
// acquire→register window. A test that only passes because it picked the
// favourable ordering is as useless as one that cannot fail.
//
// So these tests never touch runningJobs. Ownership is established by ACQUIRING,
// exactly as dispatch does, and that is the ordering under test.
func TestReleaseConcurrencySlots_DoesNotDeleteTheRowALaterRunHolds(t *testing.T) {
	job := runningJob("job-1", "acme")
	store := newCapMockStorage([]*core.Job{job})
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	// Run #1 acquires the cap slot.
	tok1 := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, store.slotCount("customer:acme"), "run #1 must hold the row")

	// Run #2 takes over the job id and re-acquires — it SHARES run #1's row. This
	// is the dispatch-side acquire, and NOTHING has registered it in runningJobs
	// yet: production cannot, because the blocking jobsChan send happens between
	// this line and processJobRun. That window is the one under test.
	tok2 := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok2))
	w.runningJobsMu.Lock()
	_, registered := w.runningJobs[job.ID]
	w.runningJobsMu.Unlock()
	require.False(t, registered,
		"guard on the premise: run #2 must NOT be in runningJobs here, or this test is back to "+
			"driving an ordering production never takes")

	// Run #1's deferred cleanup lands while run #2 is still executing.
	w.releaseConcurrencySlots(ctx, job.ID, tok1)

	assert.Equal(t, 1, store.slotCount("customer:acme"),
		"the FLEET cap row must survive an earlier run's cleanup — deleting it lets another job "+
			"be admitted past the limit while this handler is still running")

	// And when run #2 finishes, the row is finally released.
	w.releaseConcurrencySlots(ctx, job.ID, tok2)
	assert.Zero(t, store.slotCount("customer:acme"),
		"the last run out must release the row, or the cap leaks a slot permanently")
}

// TestReleaseConcurrencySlots_HandoverDoesNotLeakTheRow is the other half of the
// handover: refusing to release when a later run holds the row must not turn into
// never releasing it.
//
// FALSE-GREEN TRAP: the happy sequence (run #1 hands over, run #2 releases) is
// already covered. These are the paths where the later run does NOT complete
// normally — it panics, or a THIRD run replaces it, or the entry is already gone.
// A leaked row holds a FLEET-wide cap until its TTL expires, throttling every
// worker in the deployment.
func TestReleaseConcurrencySlots_HandoverDoesNotLeakTheRow(t *testing.T) {
	newWorker := func(t *testing.T) (*Worker, *capMockStorage, *core.Job) {
		t.Helper()
		job := runningJob("job-1", "acme")
		store := newCapMockStorage([]*core.Job{job})
		q := queue.New(store)
		q.Register(job.Type, func(context.Context, struct{}) error { return nil })
		w := NewWorker(q,
			WorkerQueue("default", Concurrency(3)),
			ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
			DisableRetry(),
		)
		w.config.WorkerID = "worker-1"
		return w, store, job
	}

	t.Run("a third run takes over before the second releases", func(t *testing.T) {
		w, store, job := newWorker(t)
		ctx := context.Background()

		tok1 := w.nextRunToken.Add(1)
		require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
		tok2 := w.nextRunToken.Add(1)
		require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok2))

		w.releaseConcurrencySlots(ctx, job.ID, tok1) // hands over to #2
		tok3 := w.nextRunToken.Add(1)
		require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok3))
		w.releaseConcurrencySlots(ctx, job.ID, tok2) // hands over to #3
		require.Equal(t, 1, store.slotCount("customer:acme"), "still held by run #3")

		// Run #3 is the last out.
		w.releaseConcurrencySlots(ctx, job.ID, tok3)
		assert.Zero(t, store.slotCount("customer:acme"),
			"the last run out must release, however many handovers preceded it")
	})

	t.Run("the later run releases first, then the earlier one", func(t *testing.T) {
		w, store, job := newWorker(t)
		ctx := context.Background()

		tok1 := w.nextRunToken.Add(1)
		require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
		tok2 := w.nextRunToken.Add(1)
		require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok2))

		// #2 finishes FIRST. It must leave the row for #1, which is still running —
		// the reverse direction of the handover, and the one a "latest run wins"
		// rule would get wrong.
		w.releaseConcurrencySlots(ctx, job.ID, tok2)
		assert.Equal(t, 1, store.slotCount("customer:acme"),
			"run #1's handler is still executing; the row it is relying on must not be deleted "+
				"just because a later run finished first")
		w.releaseConcurrencySlots(ctx, job.ID, tok1)

		assert.Zero(t, store.slotCount("customer:acme"),
			"out-of-order completion must still end with the row released, not held forever")
	})

	// Everything above calls tryAcquireConcurrencySlots directly, which is what
	// dispatch calls — but "which is what dispatch calls" is an assumption, and the
	// previous version of these tests was wrong about exactly that kind of
	// assumption. This one goes through dispatchDequeuedJobs itself, so the
	// acquire-then-blocking-send-then-register ordering is the code's, not mine.
	t.Run("both runs admitted through the real dispatch path", func(t *testing.T) {
		w, store, job := newWorker(t)
		ctx := context.Background()

		ch := make(chan dispatchedJob, 2)
		n, _ := w.dispatchDequeuedJobs(ctx, ch, []*core.Job{job})
		require.Equal(t, 1, n)
		run1 := <-ch
		require.Equal(t, 1, store.slotCount("customer:acme"))

		// The job is released back to pending and re-dequeued by this same worker —
		// the aggressive-pause-then-resume sequence, and the stale-lock reaper
		// reclaim. dispatchDequeuedJobs admits it again under a fresh token.
		n, _ = w.dispatchDequeuedJobs(ctx, ch, []*core.Job{job})
		require.Equal(t, 1, n)
		run2 := <-ch
		require.NotEqual(t, run1.token, run2.token, "each dispatch must mint its own run token")

		// Run #1's deferred cleanup lands here — after run #2 was admitted, before
		// run #2 has been registered by processJobRun.
		w.releaseConcurrencySlots(ctx, job.ID, run1.token)
		assert.Equal(t, 1, store.slotCount("customer:acme"),
			"run #2 was admitted through the real dispatch path and holds this row; run #1's "+
				"cleanup must not delete the fleet cap out from under it")

		w.releaseConcurrencySlots(ctx, job.ID, run2.token)
		assert.Zero(t, store.slotCount("customer:acme"), "and the last one out releases it")
	})

	t.Run("the run that panics still releases through its defer", func(t *testing.T) {
		job := runningJob("job-1", "acme")
		store := newCapMockStorage([]*core.Job{job})
		q := queue.New(store)
		q.Register(job.Type, func(context.Context, struct{}) error { panic("exploded") })
		w := NewWorker(q,
			WorkerQueue("default", Concurrency(3)),
			ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
			DisableRetry(),
		)
		w.config.WorkerID = "worker-1"

		ch := make(chan dispatchedJob, 1)
		dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, []*core.Job{job})
		require.Equal(t, 1, dispatched)
		dj := <-ch
		require.Equal(t, 1, store.slotCount("customer:acme"))

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		w.processJobRun(ctx, dj.job, dj.token)

		assert.Zero(t, store.slotCount("customer:acme"),
			"a panicking run must still release its fleet slot — the row is cluster-wide and would "+
				"otherwise throttle every worker until its TTL expires")
	})
}

// TestProcessJobRun_UnregisterUnderTheLockDoesNotDeadlock covers the RISK the
// widened lock scope introduces, which is the half that is testable.
//
// processJobRun's cleanup holds w.runningJobsMu across w.queue.UnregisterRunningJob,
// so it acquires q.runningJobsMu while holding its own. That is only safe while the
// queue mutex stays a LEAF — the moment any queue path acquires the worker's mutex
// (or calls back into the worker) while holding its own, the two orders invert and
// the fleet wedges. Queue.CancelJob and Queue.PauseJob are the paths that take
// q.runningJobsMu on the operator side, so they are what this hammers.
//
// FALSE-GREEN TRAP: a single completing job proves nothing — the inversion needs an
// operator call in flight against a job that is finishing. This drives many rounds
// of exactly that overlap and fails on a TIMEOUT rather than an assertion, because
// a lock-order inversion presents as a hang, not a wrong value.
func TestProcessJobRun_UnregisterUnderTheLockDoesNotDeadlock(t *testing.T) {
	store := &mockStorage{}
	q := queue.New(store)
	q.Register("quick", func(context.Context, struct{}) error { return nil })
	w := NewWorker(q)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const rounds = 300
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < rounds; i++ {
			job := &core.Job{ID: core.NewID(), Type: "quick", Queue: "default", Status: core.StatusRunning}
			wg.Add(3)
			// The finishing run: registers, runs, then unregisters under the lock.
			go func() { defer wg.Done(); w.processJob(ctx, job) }()
			// Two operator calls racing that teardown from the queue side.
			go func() { defer wg.Done(); _ = q.CancelJob(ctx, job.ID) }()
			go func() { defer wg.Done(); _ = q.PauseJob(ctx, job.ID) }()
			wg.Wait()
		}
	}()

	select {
	case <-done:
	case <-time.After(25 * time.Second):
		t.Fatal("processJob's cleanup deadlocked against a concurrent Queue.CancelJob/PauseJob — " +
			"holding w.runningJobsMu across UnregisterRunningJob is only safe while q.runningJobsMu " +
			"is a leaf, and something now acquires them in the opposite order")
	}
}
