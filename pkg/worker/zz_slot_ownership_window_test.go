package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	// No manual slotJobID seed: record-before-acquire means the acquire publishes
	// the entry itself. An earlier version hand-seeded it, which was dead weight
	// AND pre-populated a token production always allocates fresh.
	tok := w.nextRunToken.Add(1)
	require.True(t, base.TryAcquireConcurrencySlotOK("customer:acme", job.ID, "worker-1"),
		"seed the ROW the rollback has to remove")

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

// nthGateStorage parks AFTER the Nth successful acquire of a named slot, without
// erroring — so the run it parks continues walking its cap loop afterwards. That
// "afterwards" is the whole point: it is where record() runs again.
type nthGateStorage struct {
	*capMockStorage
	gateFor string
	nth     int
	seen    atomic.Int64
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (s *nthGateStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error) {
	ok, err := s.capMockStorage.TryAcquireConcurrencySlot(ctx, slotName, jobID, workerID, limit, ttl)
	if slotName == s.gateFor && ok && int(s.seen.Add(1)) == s.nth {
		s.once.Do(func() {
			close(s.entered)
			<-s.proceed
		})
	}
	return ok, err
}

// TestTryAcquireConcurrencySlots_RecordDoesNotDiscardHandedOverNames covers the
// interaction between the two halves of the ownership design, which was broken:
// releaseConcurrencySlots MERGES a departing run's names into the survivor, and
// tryAcquireConcurrencySlots' record() then REPLACED that entry wholesale on its
// next iteration. The handover survived exactly one loop step.
//
// FALSE-GREEN TRAP, and my first attempt at this test fell straight into it: I
// parked the successor inside an ERRORING acquire. The loop then breaks
// immediately, record() never runs again, the merged entry is still intact, and
// the rollback releases everything — green with the bug fully present. The defect
// needs the successor to SUCCEED at the cap it is parked in and keep walking, so
// that a later record() has something to overwrite.
//
// The cap key deliberately DRIFTS between the two runs, so the successor never
// re-derives the earlier run's names: they are reachable only through the
// handover, which makes the discard observable rather than masked.
func TestTryAcquireConcurrencySlots_RecordDoesNotDiscardHandedOverNames(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	// Park after the SECOND successful acquire of customer:* — run #1 takes the
	// first, so this parks the successor mid-loop with one more cap still to go.
	store := &nthGateStorage{
		capMockStorage: base,
		gateFor:        "customer:beta",
		nth:            1,
		entered:        make(chan struct{}),
		proceed:        make(chan struct{}),
	}
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })

	var keyN atomic.Int64
	drifting := CapKey(func(*core.Job) string {
		if keyN.Load() == 0 {
			return "acme"
		}
		return "beta"
	})
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, drifting),
		ConcurrencyCap("region", 1, drifting),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	tok1 := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, base.slotCount("customer:acme"))
	require.Equal(t, 1, base.slotCount("region:acme"))

	// The same job id, re-dequeued, now deriving a different key.
	keyN.Store(1)
	tok2 := w.nextRunToken.Add(1)
	done := make(chan bool, 1)
	go func() { done <- w.tryAcquireConcurrencySlots(ctx, job, tok2) }()

	<-store.entered // successor has acquired customer:beta and is parked mid-loop
	w.releaseConcurrencySlots(ctx, job.ID, tok1)

	// PREMISE: the handover put run #1's names onto the successor, and the
	// successor still has another cap to acquire — so a later record() is coming.
	w.slotJobIDMu.Lock()
	merged := append([]string(nil), w.slotJobID[tok2].names...)
	w.slotJobIDMu.Unlock()
	require.ElementsMatch(t, []string{"customer:beta", "customer:acme", "region:acme"}, merged,
		"PREMISE: the successor must be holding the handed-over names AND still be mid-loop — "+
			"without that there is no later record() to discard them and this test cannot fail")

	close(store.proceed)
	require.True(t, <-done, "the successor's acquire succeeds")

	// Both runs are done; the successor was the last one out, so it owed every row.
	w.releaseConcurrencySlots(ctx, job.ID, tok2)

	for _, n := range []string{"customer:acme", "region:acme", "customer:beta", "region:beta"} {
		assert.Zero(t, base.slotCount(n),
			"%s must be released once both runs finish. The earlier run's rows are reachable "+
				"ONLY through the handover, so a record() that replaces rather than merges strands "+
				"them — held by nobody, released by nobody, until the 45-minute slot TTL expires, "+
				"denying one slot of a FLEET-WIDE cap to every worker in the deployment", n)
	}
}

// TestReleaseConcurrencySlots_UnionNeverCrossesJobIDs guards the RISK the union
// fix introduces. record() and the handover both union now, so a run accumulates
// names it did not itself acquire — and the obvious way for that to go wrong is
// for it to accumulate a name belonging to a DIFFERENT job and delete that row on
// its way out.
//
// Also pins that the list stays bounded: a chain of handovers unions the same
// names repeatedly, so the length must stay at the configured cap count rather
// than growing once per departing sibling.
//
// Written pre-emptively against my own fix rather than in response to a finding —
// three consecutive rounds have found a defect inside the previous round's fix,
// so the fix itself is the thing most worth attacking.
func TestReleaseConcurrencySlots_UnionNeverCrossesJobIDs(t *testing.T) {
	jobA := runningJob("job-a", "acme")
	jobB := runningJob("job-b", "globex")
	store := newCapMockStorage([]*core.Job{jobA, jobB})
	q := queue.New(store)
	key := CapKey(func(j *core.Job) string { return j.UniqueKey })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(6)),
		ConcurrencyCap("customer", 2, key),
		ConcurrencyCap("region", 2, key),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	// Job B holds its own rows throughout.
	tokB := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, jobB, tokB))

	// Three overlapping runs of job A, each handing over to the next.
	var toks []uint64
	for i := 0; i < 3; i++ {
		tk := w.nextRunToken.Add(1)
		require.True(t, w.tryAcquireConcurrencySlots(ctx, jobA, tk))
		toks = append(toks, tk)
	}
	for i := 0; i < 2; i++ {
		w.releaseConcurrencySlots(ctx, jobA.ID, toks[i])
		require.Equal(t, 1, store.slotCount("customer:acme"), "still held after handover %d", i)
		require.Equal(t, 1, store.slotCount("customer:globex"), "job B untouched after handover %d", i)
	}
	// Name list must not have grown past the number of configured caps.
	w.slotJobIDMu.Lock()
	n := len(w.slotJobID[toks[2]].names)
	w.slotJobIDMu.Unlock()
	require.LessOrEqual(t, n, 2, "union must not grow past the configured cap count, got %d", n)

	w.releaseConcurrencySlots(ctx, jobA.ID, toks[2])
	require.Zero(t, store.slotCount("customer:acme"), "job A fully released")
	require.Equal(t, 1, store.slotCount("customer:globex"),
		"job B's row must survive job A's releases — a union that crossed job ids would steal it")
	require.Equal(t, 1, store.slotCount("region:globex"))

	w.releaseConcurrencySlots(ctx, jobB.ID, tokB)
	require.Zero(t, store.slotCount("customer:globex"))
}

// gatedReleaseStorage parks INSIDE the storage delete, so a test can try to land
// an acquire while the DELETE is in flight — the window between deciding "nobody
// else holds this row" and actually removing it.
type gatedReleaseStorage struct {
	*capMockStorage
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (s *gatedReleaseStorage) ReleaseConcurrencySlotOwned(ctx context.Context, slotName string, jobID core.UUID, workerID string) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.proceed
	})
	return s.ReleaseConcurrencySlot(ctx, slotName, jobID)
}

// TestReleaseConcurrencySlots_DecisionAndDeleteAreAtomic covers the window between
// the "no other run holds this row" decision and the DELETE that acts on it.
//
// The decision was made under slotJobIDMu and the mutex was then DROPPED before
// the storage call. A later run of the same job id that publishes in that window
// is invisible to the decision, renews the row through TryAcquireConcurrencySlot's
// idempotent branch, and then loses it to the in-flight DELETE — a FLEET-WIDE cap
// slot deleted out from under a handler that is still executing. The ownership
// fence cannot refuse it: same job id, same worker id.
//
// The window is a full DB ROUND TRIP wide, which is orders of magnitude larger
// than the acquire-side window an earlier round called a real defect and fixed.
//
// FALSE-GREEN TRAP, and the neighbouring test named for this exact invariant is
// it: TestReleaseConcurrencySlots_DoesNotDeleteTheRowALaterRunHolds runs run #2's
// acquire to COMPLETION and only then calls release. Strictly sequential — it can
// only ever certify the favourable ordering. Catching this needs the acquire to
// land INSIDE the release, which is what the gate below does.
func TestReleaseConcurrencySlots_DecisionAndDeleteAreAtomic(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &gatedReleaseStorage{
		capMockStorage: base,
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

	tok1 := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, base.slotCount("customer:acme"))

	// Run #1 releases; it parks inside the DELETE.
	releaseDone := make(chan struct{})
	go func() { defer close(releaseDone); w.releaseConcurrencySlots(ctx, job.ID, tok1) }()
	<-store.entered

	// The same job id is re-dequeued and admitted WHILE that delete is in flight.
	// With the decision and the delete made atomic this blocks until the release
	// finishes and then takes a fresh row; without it, it renews the doomed row.
	tok2 := w.nextRunToken.Add(1)
	acquired := make(chan bool, 1)
	go func() { acquired <- w.tryAcquireConcurrencySlots(ctx, job, tok2) }()

	// THE DISCRIMINATING OBSERVATION, and my first version of this test got it
	// wrong: it released the gate immediately, so run #2 had not necessarily done
	// anything yet and the test passed against the broken code. What actually
	// distinguishes the two versions is whether run #2 can COMPLETE while the
	// DELETE is still in flight. It must not — if it can, it has renewed a row
	// that is about to be removed.
	select {
	case <-acquired:
		close(store.proceed)
		<-releaseDone
		t.Fatal("run #2 completed its acquire while run #1's DELETE was still in flight: the " +
			"decision and the delete are not atomic, so run #2 renewed the very row that " +
			"release is about to remove, and will execute holding no fleet-cap row")
	case <-time.After(750 * time.Millisecond):
		// Correctly blocked behind the release.
	}

	close(store.proceed)
	<-releaseDone
	require.True(t, <-acquired, "run #2 must then be admitted, on a fresh row")

	assert.Equal(t, 1, base.slotCount("customer:acme"),
		"run #2 holds the fleet cap row and is about to execute; a release that decided "+
			"'nobody else holds this' BEFORE run #2 published, and then deleted AFTER it did, "+
			"removes the row out from under it and admits another job past the cap")

	w.releaseConcurrencySlots(ctx, job.ID, tok2)
	assert.Zero(t, base.slotCount("customer:acme"), "and the last run out still releases it")
}

// slowReleaseStorage makes every slot DELETE take real time, which is what a
// mutex held across I/O actually costs.
type slowReleaseStorage struct {
	*capMockStorage
	delay    time.Duration
	releases atomic.Int64
}

func (s *slowReleaseStorage) ReleaseConcurrencySlotOwned(ctx context.Context, slotName string, jobID core.UUID, workerID string) error {
	time.Sleep(s.delay)
	s.releases.Add(1)
	return s.ReleaseConcurrencySlot(ctx, slotName, jobID)
}

// TestReleaseConcurrencySlots_SlowDeleteDoesNotWedgeDispatch guards the RISK the
// atomicity fix introduces: slotJobIDMu is now held across a storage call, so
// every acquire blocks behind every release.
//
// The concern is not correctness — holding it is what makes the decision and the
// delete atomic — but liveness. A slow database must not wedge dispatch, deadlock
// the worker, or starve acquires indefinitely. This drives many concurrent
// acquire/release pairs against a storage whose DELETE genuinely sleeps, and fails
// by TIMEOUT, because a lock-order or liveness failure presents as a hang.
//
// Written pre-emptively against my own change rather than in response to a
// finding: a mutex newly held across I/O is the single riskiest thing in this
// branch, and four consecutive rounds have found a defect inside the previous
// round's fix.
func TestReleaseConcurrencySlots_SlowDeleteDoesNotWedgeDispatch(t *testing.T) {
	base := newCapMockStorage(nil)
	store := &slowReleaseStorage{capMockStorage: base, delay: 2 * time.Millisecond}
	q := queue.New(store)
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(64)),
		ConcurrencyCap("customer", 1000, CapKey(func(j *core.Job) string { return j.UniqueKey })),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	const runners = 16
	const each = 12
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < runners; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < each; j++ {
					// A distinct job id per iteration, so these are independent
					// acquire/release pairs contending on one mutex.
					job := runningJob(string(core.NewID()), "tenant")
					tok := w.nextRunToken.Add(1)
					if w.tryAcquireConcurrencySlots(ctx, job, tok) {
						w.releaseConcurrencySlots(ctx, job.ID, tok)
					}
				}
			}(i)
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("dispatch wedged: holding slotJobIDMu across the storage DELETE must serialise " +
			"slot bookkeeping, not deadlock it or starve acquires")
	}

	assert.Equal(t, int64(runners*each), store.releases.Load(),
		"every acquired slot must be released exactly once under contention")
	w.slotJobIDMu.Lock()
	n := len(w.slotJobID)
	w.slotJobIDMu.Unlock()
	assert.Zero(t, n, "and no bookkeeping entry may survive")
}
