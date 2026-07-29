package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// These two tests were written by an adversarial reviewer against my own round-11
// rework, and both failed on it. They are kept as-is in substance because a repro
// that caught a live defect is worth more than one rewritten to look tidy.
//
// gateCapStorage blocks inside TryAcquireConcurrencySlot AFTER the row has been
// joined, so a test can land a concurrent release in the window between "the DB
// row is joined" and "record() publishes this run into w.slotJobID".
type gateCapStorage struct {
	*capMockStorage
	gateFor string
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (s *gateCapStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error) {
	ok, err := s.capMockStorage.TryAcquireConcurrencySlot(ctx, slotName, jobID, workerID, limit, ttl)
	if slotName == s.gateFor && ok {
		s.once.Do(func() {
			close(s.entered)
			<-s.proceed
		})
	}
	return ok, err
}

// TestTenX_AcquireRecordWindowDropsALiveRow: run #2 has JOINED the row in the
// database but has not yet been published into w.slotJobID. Run #1's release
// scans the map, sees no other holder, and deletes the row run #2 is relying on.

func TestTryAcquireConcurrencySlots_PublishesOwnershipBeforeJoiningTheRow(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &gateCapStorage{
		capMockStorage: base,
		gateFor:        "customer:acme",
		entered:        make(chan struct{}),
		proceed:        make(chan struct{}),
	}
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	// Run #1 holds the row (acquired before the gate arms — drain the gate).
	tok1 := w.nextRunToken.Add(1)
	go func() { <-store.entered; close(store.proceed) }()
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, base.slotCount("customer:acme"))

	// Re-arm the gate for run #2.
	store.once = sync.Once{}
	store.entered = make(chan struct{})
	store.proceed = make(chan struct{})

	tok2 := w.nextRunToken.Add(1)
	done := make(chan bool, 1)
	go func() { done <- w.tryAcquireConcurrencySlots(ctx, job, tok2) }()

	<-store.entered // run #2 has JOINED the row; record() has not run yet.
	w.releaseConcurrencySlots(ctx, job.ID, tok1)
	close(store.proceed)
	require.True(t, <-done, "run #2 believes it acquired the cap slot")

	assert.Equal(t, 1, base.slotCount("customer:acme"),
		"run #2 joined this row before run #1 released; deleting it under a run that "+
			"believes it holds the fleet cap is the over-admission the fence exists to prevent")

	// Demonstrate the consequence: a DIFFERENT job with the same cap key is now
	// admitted past a limit of 1 while run #2 is still executing.
	other := runningJob("job-2", "acme")
	base.jobs["job-2"] = other
	tok3 := w.nextRunToken.Add(1)
	admitted := w.tryAcquireConcurrencySlots(ctx, other, tok3)
	assert.False(t, admitted,
		"a second job under the same cap key must NOT be admitted while run #2 holds the slot")
}

type gatedErrStorage struct {
	*capMockStorage
	failOn   string
	failFrom int
	failCall int
	arm      chan struct{}
	release  chan struct{}
	armed    bool
	mu       sync.Mutex
}

func (s *gatedErrStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error) {
	if slotName == s.failOn {
		s.mu.Lock()
		s.failCall++
		n := s.failCall
		park := s.armed
		if park {
			s.armed = false
		}
		s.mu.Unlock()
		if n >= s.failFrom {
			// Park HERE: run #2 has already recorded cap A and is inside the DB
			// call for cap B. This is where a slow/erroring second acquire sits.
			if park {
				close(s.arm)
				<-s.release
			}
			return false, errors.New("transient db error")
		}
	}
	return s.capMockStorage.TryAcquireConcurrencySlot(ctx, slotName, jobID, workerID, limit, ttl)
}

func TestReleaseConcurrencySlots_HandoverMergesNamesTheSuccessorDoesNotHold(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &gatedErrStorage{
		capMockStorage: base,
		failOn:         "customer:acme",
		failFrom:       2,
		arm:            make(chan struct{}),
		release:        make(chan struct{}),
	}
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		ConcurrencyCap("region", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	tok1 := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, base.slotCount("customer:acme"))
	require.Equal(t, 1, base.slotCount("region:acme"))

	store.mu.Lock()
	store.armed = true
	store.mu.Unlock()
	tok2 := w.nextRunToken.Add(1)
	done := make(chan bool, 1)
	go func() { done <- w.tryAcquireConcurrencySlots(ctx, job, tok2) }()

	<-store.arm

	// PREMISE, asserted rather than assumed. The whole point of the merge is that
	// the successor holds a STRICT SUBSET at the instant the earlier run departs.
	// An earlier version of this test parked on the LAST cap — and because
	// record() publishes intent BEFORE each storage call, run #2 had already
	// recorded BOTH names by then, so the merge had nothing to do and the test
	// passed with the entire fix reverted. Parking on the FIRST cap is what
	// reproduces the subset. If this assertion ever fails, the test has stopped
	// exercising the defect, whatever its later assertions say.
	w.slotJobIDMu.Lock()
	names1 := append([]string(nil), w.slotJobID[tok1].names...)
	names2 := append([]string(nil), w.slotJobID[tok2].names...)
	w.slotJobIDMu.Unlock()
	require.Equal(t, []string{"customer:acme", "region:acme"}, names1,
		"PREMISE: the departing run holds BOTH caps")
	require.Equal(t, []string{"customer:acme"}, names2,
		"PREMISE: the successor holds a STRICT SUBSET at the moment of handover — without that "+
			"there is nothing for the merge to do and this test cannot fail")

	w.releaseConcurrencySlots(ctx, job.ID, tok1) // run #1's deferred cleanup lands
	close(store.release)

	require.False(t, <-done, "run #2's acquire must be refused: its cap errored")

	assert.Zero(t, base.slotCount("customer:acme"), "the shared cap is released by the rollback")
	assert.Zero(t, base.slotCount("region:acme"),
		"NOBODY would hold region:acme without the merge: run #1 handed the job id over and its "+
			"names were DROPPED, and run #2 had not recorded that cap yet. The row would survive "+
			"to its TTL (45 min default), holding one slot of a FLEET-WIDE cap against every "+
			"worker in the deployment")

	w.slotJobIDMu.Lock()
	n := len(w.slotJobID)
	w.slotJobIDMu.Unlock()
	assert.Zero(t, n, "and no bookkeeping entry may outlive either run")
}

// ctxAwareCapStorage refuses every call whose context is already Done, which is
// what a real driver does, and records which release variant was used.
type ctxAwareCapStorage struct {
	*capMockStorage
	mu       sync.Mutex
	fenced   int
	unfenced int
}

func (s *ctxAwareCapStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.capMockStorage.TryAcquireConcurrencySlot(ctx, slotName, jobID, workerID, limit, ttl)
}

func (s *ctxAwareCapStorage) ReleaseConcurrencySlotOwned(ctx context.Context, slotName string, jobID core.UUID, workerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.fenced++
	s.mu.Unlock()
	return s.capMockStorage.ReleaseConcurrencySlot(ctx, slotName, jobID)
}

func (s *ctxAwareCapStorage) ReleaseConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.unfenced++
	s.mu.Unlock()
	return s.capMockStorage.ReleaseConcurrencySlot(ctx, slotName, jobID)
}

// TestTryAcquireConcurrencySlots_RollbackReleasesOnADetachedContext covers a bug
// the rollback rework left behind: all three bail-outs released on the SAME
// context that had just failed the acquire.
//
// On shutdown that context is already Done, so the DELETE is refused too and a
// slot acquired moments earlier survives to its TTL — 45 minutes by default — as
// a fleet-wide cap slot held by a worker that has already exited. Every OTHER
// bail-out in dispatchDequeuedJobs goes through releaseDequeuedJobOnShutdown,
// which builds a WithoutCancel context for exactly this reason.
//
// FALSE-GREEN TRAP: a mock that ignores ctx cannot see this at all — the release
// "succeeds" against a cancelled context and the row disappears. The storage here
// refuses a Done context, which is what a real driver does.
//
// SCOPE, stated because an earlier version of this comment got it wrong: the
// branch actually exercised is the acquire-ERROR path on the FIRST cap (the
// ctx-aware storage refuses a Done context). It is NOT the capSlotName-refusal
// path — capSlotName returns false only when a user CapKey panics, and an empty
// key yields the perfectly valid slot name "region:". The second cap configured
// below is inert here; it is present so the worker has a multi-cap shape.
func TestTryAcquireConcurrencySlots_RollbackReleasesOnADetachedContext(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &ctxAwareCapStorage{capMockStorage: base}
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		ConcurrencyCap("region", 2, CapKey(func(*core.Job) string { return "" })), // refused: empty key
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	// A context that is ALREADY cancelled, exactly as on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Seed the row on a LIVE context, then drive the acquire on the cancelled one.
	// The first cap is refused (empty key -> capSlotName fails), which is rollback
	// exit #1, and it must still remove what a previous run left behind.
	tok := w.nextRunToken.Add(1)
	require.True(t, base.TryAcquireConcurrencySlotOK("customer:acme", job.ID, "worker-1"),
		"seed the row the rollback has to remove")
	w.slotJobIDMu.Lock()
	w.slotJobID[tok] = slotHold{jobID: job.ID, names: []string{"customer:acme"}}
	w.slotJobIDMu.Unlock()

	require.False(t, w.tryAcquireConcurrencySlots(ctx, job, tok),
		"an already-cancelled context must not admit the job")

	assert.Zero(t, base.slotCount("customer:acme"),
		"a rollback on a cancelled context must still remove the row — otherwise a worker that is "+
			"shutting down leaves a FLEET-WIDE cap slot held for its full TTL (45 minutes by default)")
}

// TestReleaseSlotNames_PrefersTheOwnershipFencedRelease covers the choice between
// the two release variants, which was free: reverting releaseSlotNames to always
// call the UNFENCED ReleaseConcurrencySlot left the whole repository green.
//
// The predicate is unit-tested in pkg/storage; the DECISION to use it was not.
// Without the fence, a deferred release from a worker whose job was already
// reclaimed by the stale-lock reaper deletes the row the NEW holder is relying on.
func TestReleaseSlotNames_PrefersTheOwnershipFencedRelease(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &ctxAwareCapStorage{capMockStorage: base}
	q := queue.New(store)
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(2)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
	)
	w.config.WorkerID = "worker-1"

	ctx := context.Background()
	tok := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok))
	w.releaseConcurrencySlots(ctx, job.ID, tok)

	store.mu.Lock()
	fenced, unfenced := store.fenced, store.unfenced
	store.mu.Unlock()
	assert.Positive(t, fenced,
		"the ownership-FENCED release must be preferred when the storage offers it; without the "+
			"worker_id predicate a release from a worker whose job the stale-lock reaper already "+
			"reclaimed deletes the row the NEW holder is relying on")
	assert.Zero(t, unfenced,
		"and the unfenced variant must not be used when the fenced one exists")
}

// TestProcessJob_UnregistersFromTheQueueRegistryOnCompletion covers a call site
// that was free on origin/main and stayed free through this entire campaign:
// deleting `w.queue.UnregisterRunningJob(job.ID)` from processJobRun's cleanup
// leaves ./pkg/worker, ./tests, ./ui, ./pkg/queue and the root package ALL green.
//
// Queue.runningJobs is the registry Queue.CancelJob and Queue.PauseJob consult to
// reach a locally-running handler. Never removing an entry means it grows without
// bound — one retained context.CancelFunc per job the process has EVER run — and
// those operations then act on stale entries for jobs that finished long ago.
//
// FALSE-GREEN TRAP, twice over:
//   - the helper is unit-tested in pkg/queue, and this package's
//     TestProcessJob_LaterRunKeepsTheQueueRegistration covers the run-token GUARD
//     around the call — but there the token check fails first, so Unregister is
//     never reached. Neither says a normal completion cleans up.
//   - my first attempt at this test called RegisterRunningJob/UnregisterRunningJob
//     itself and asserted on the result, which proves those two work and says
//     nothing about whether processJob calls them. That is asserting on a copy of
//     production, one more time.
//
// So the assertion is on the registry SIZE after real completions, via
// Queue.RunningJobCount — which was added for exactly this, because the invariant
// had no observable at all.
func TestProcessJob_UnregistersFromTheQueueRegistryOnCompletion(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("quick", func(context.Context, struct{}) error { return nil })
	q.Register("boom", func(context.Context, struct{}) error { panic("exploded") })
	w := NewWorker(q, WorkerQueue("default", Concurrency(4)))

	require.Zero(t, q.RunningJobCount(), "nothing has run yet")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, typ := range []string{"quick", "quick", "boom", "quick"} {
		w.processJob(ctx, &core.Job{
			ID: core.NewID(), Type: typ, Queue: "default",
			Status: core.StatusRunning, MaxRetries: 1,
		})
	}

	assert.Zero(t, q.RunningJobCount(),
		"every finished job must unregister itself, including one whose handler PANICKED — "+
			"otherwise the registry retains a cancel func per job the process ever ran and "+
			"Queue.CancelJob/PauseJob start acting on stale entries for finished jobs")
}

// TestReleaseConcurrencySlots_HolderScanIsScopedToTheSameJobID covers the
// `other.jobID != jobID` predicate in the holder scan, which was FREE: deleting it
// left pkg/worker green.
//
// Without it, ANY unrelated run holding ANY slot suppresses this run's release —
// so on a worker running more than one capped job (which is the entire point of a
// cap) every fleet cap row leaks to its 45-minute TTL. The scan exists to answer
// "does another token still hold THIS JOB ID", and a scan that answers "is anyone
// holding anything" is not that question.
//
// FALSE-GREEN TRAP: with only ONE job in flight the predicate is unreachable —
// there is no other holder to confuse it with. It needs two jobs with DIFFERENT
// cap keys, released independently.
func TestReleaseConcurrencySlots_HolderScanIsScopedToTheSameJobID(t *testing.T) {
	jobA := runningJob("job-a", "acme")
	jobB := runningJob("job-b", "globex")
	store := newCapMockStorage([]*core.Job{jobA, jobB})
	q := queue.New(store)
	q.Register(jobA.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(4)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	tokA := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, jobA, tokA))
	tokB := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, jobB, tokB))
	require.Equal(t, 1, store.slotCount("customer:acme"))
	require.Equal(t, 1, store.slotCount("customer:globex"))

	// Job A finishes while job B is still running. B holds a slot, but it is a
	// DIFFERENT job id, so it must not suppress A's release.
	w.releaseConcurrencySlots(ctx, jobA.ID, tokA)

	assert.Zero(t, store.slotCount("customer:acme"),
		"an unrelated job holding an unrelated cap must not suppress this release — otherwise "+
			"every fleet cap row leaks to its TTL on any worker running more than one capped job")
	assert.Equal(t, 1, store.slotCount("customer:globex"),
		"and job B's own row is untouched")

	w.releaseConcurrencySlots(ctx, jobB.ID, tokB)
	assert.Zero(t, store.slotCount("customer:globex"))
}
