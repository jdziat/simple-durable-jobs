package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// ---------------------------------------------------------------------------
// cto-F2 load/soak harness: measure the claim->release write amplification the
// dispatch loop incurs when a fleet rate limit is saturated but concurrency is
// free. This is the DETERMINISTIC, no-DB layer (white-box over the worker's
// dispatch loop, reusing the releaseStateStorage Release simulation). The
// real-DB counterpart lives in benchmarks/ratelimit_churn_bench_test.go.
//
// The churn (resume.md cto-F2): dequeueSlots derives its claim budget purely
// from concurrency headroom and is blind to rate-limit saturation, so every
// poll tick the worker CLAIMS ~releaseBudget jobs (status->running, a DB write),
// the LAST dispatch gate (tryConsumeRateLimits) bounces them, and each is
// RELEASED back to pending (another DB write) — pure write amplification with
// zero forward progress, repeating every PollInterval while saturated.
//
// soakStorage cycles a small fixed set of jobs through pending<->running so the
// same backlog drives unbounded churn, and counts every Dequeue / Release /
// TryConsumeRate the dispatch path performs.
// ---------------------------------------------------------------------------

type soakStorage struct {
	*releaseStateStorage

	mu    sync.Mutex
	order []*core.Job // stable claim order; jobs cycle pending<->running

	allow        atomic.Bool // TryConsumeRate verdict (unkeyed): false = saturated
	dequeueCalls atomic.Int64
	rateChecks   atomic.Int64
	rateAllowed  atomic.Int64

	// keyDeny, when set, decides TryConsumeRate per EFFECTIVE limit name
	// ("<name>:<key>") — modeling a KEYED fleet limit where only some buckets
	// are saturated. Overrides allow.
	keyDeny func(limitName string) bool
}

// newSoakStorage seeds n PENDING jobs (Attempt 0) that the dispatch path will
// claim (Attempt+1) and, while saturated, bounce (Attempt-1) — so a per-job
// attempt is neutral across a claim/bounce cycle. Starts saturated (allow=false).
func newSoakStorage(n int) *soakStorage {
	rs := &releaseStateStorage{
		mockStorage: &mockStorage{},
		jobs:        make(map[string]*core.Job, n),
	}
	s := &soakStorage{releaseStateStorage: rs}
	for i := 0; i < n; i++ {
		j := &core.Job{
			ID:      core.UUID(fmt.Sprintf("soak-%05d", i)),
			Type:    "work",
			Queue:   "default",
			Status:  core.StatusPending,
			Attempt: 0,
		}
		rs.jobs[string(j.ID)] = j
		s.order = append(s.order, j)
	}
	return s
}

// newKeyedSoakStorage models a KEYED fleet limit: perKey pending jobs for the
// saturated hotKey tenant plus perKey for each cold (has-headroom) tenant.
// keyDeny denies only the hot key's effective limit name ("<name>:<hotKey>"),
// so cold-key jobs admit and hot-key jobs bounce — the per-tenant-rate-limit
// shape where one tenant runs hot while others have headroom.
func newKeyedSoakStorage(perKey int, hotKey string, coldKeys []string) *soakStorage {
	rs := &releaseStateStorage{
		mockStorage: &mockStorage{},
		jobs:        make(map[string]*core.Job),
	}
	s := &soakStorage{releaseStateStorage: rs}
	add := func(tenant string) {
		for i := 0; i < perKey; i++ {
			j := &core.Job{
				ID:      core.UUID(fmt.Sprintf("soak-%s-%05d", tenant, i)),
				Type:    "work",
				Queue:   "default",
				Tenant:  tenant,
				Status:  core.StatusPending,
				Attempt: 0,
			}
			rs.jobs[string(j.ID)] = j
			s.order = append(s.order, j)
		}
	}
	add(hotKey)
	for _, k := range coldKeys {
		add(k)
	}
	s.keyDeny = func(limitName string) bool { return strings.HasSuffix(limitName, ":"+hotKey) }
	return s
}

// tenantStats returns, for jobs of the given tenant, the pending/running counts
// and the max Attempt seen — for asserting cold-key dispatch (running) and
// hot-key refund (pending, attempt 0) after a keyed soak.
func (s *soakStorage) tenantStats(tenant string) (pending, running, maxAttempt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.order {
		if j.Tenant != tenant {
			continue
		}
		switch j.Status {
		case core.StatusPending:
			pending++
		case core.StatusRunning:
			running++
		}
		if j.Attempt > maxAttempt {
			maxAttempt = j.Attempt
		}
	}
	return pending, running, maxAttempt
}

// claimLocked transitions a pending job to running (caller holds s.mu),
// mirroring the real claim row-write: status->running, locked_by, attempt+1.
func (s *soakStorage) claimLocked(j *core.Job, workerID string) {
	now := time.Now()
	j.Status = core.StatusRunning
	j.LockedBy = workerID
	j.LockedUntil = &now
	j.StartedAt = &now
	j.Attempt++
	s.dequeueCalls.Add(1) // one claim = one row written up to running
}

// Dequeue claims the next currently-pending job (cycling the fixed set).
func (s *soakStorage) Dequeue(_ context.Context, _ []string, workerID string) (*core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.order {
		if j.Status == core.StatusPending {
			s.claimLocked(j, workerID)
			return j, nil
		}
	}
	return nil, nil
}

// DequeueBatch claims up to limit currently-pending jobs — the realistic
// default dispatch path (batchDequeuer), where a saturated tick bounces the
// whole budget rather than a single job.
func (s *soakStorage) DequeueBatch(_ context.Context, _ []string, workerID string, limit int) ([]*core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*core.Job
	for _, j := range s.order {
		if len(out) >= limit {
			break
		}
		if j.Status == core.StatusPending {
			s.claimLocked(j, workerID)
			out = append(out, j)
		}
	}
	return out, nil
}

func (s *soakStorage) TryConsumeRate(_ context.Context, limitName string, _ float64, _ time.Duration, _ time.Time) (bool, error) {
	s.rateChecks.Add(1)
	denied := !s.allow.Load()
	if s.keyDeny != nil {
		denied = s.keyDeny(limitName)
	}
	if denied {
		return false, nil
	}
	s.rateAllowed.Add(1)
	return true, nil
}

// pendingCount reports how many seeded jobs are currently back at pending.
func (s *soakStorage) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, j := range s.order {
		if j.Status == core.StatusPending {
			n++
		}
	}
	return n
}

// minAttempt / maxAttempt expose the attempt-neutrality invariant: across any
// number of claim/bounce cycles a never-dispatched job must return to Attempt 0.
func (s *soakStorage) attemptRange() (min, max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	min, max = 1<<30, -(1 << 30)
	for _, j := range s.order {
		if j.Attempt < min {
			min = j.Attempt
		}
		if j.Attempt > max {
			max = j.Attempt
		}
	}
	return min, max
}

// soakResult aggregates one soak run's churn counters.
type soakResult struct {
	ticks       int
	dispatched  int
	dequeues    int64
	releases    int64
	rateChecks  int64
	rateAllowed int64
}

// runSaturationSoak drives `ticks` poll ticks (drainDequeuedJobs calls) against
// store, draining and "completing" any dispatched jobs between ticks (untracking
// them so concurrency frees, as a real processLoop would). It returns the churn
// counters. This is the shared instrument both the baseline (Packet 1) and the
// post-fix collapse assertion (Packet 2) drive.
func runSaturationSoak(t testing.TB, w *Worker, store *soakStorage, totalConcurrency, ticks int) soakResult {
	t.Helper()
	jobsChan := make(chan *core.Job, totalConcurrency)
	ctx := context.Background()
	dispatched := 0
	for i := 0; i < ticks; i++ {
		w.drainDequeuedJobs(ctx, jobsChan, totalConcurrency)
		// Drain + "complete" dispatched jobs so concurrency is freed for the next
		// tick, mirroring a processLoop that finishes work between polls.
		for {
			select {
			case job := <-jobsChan:
				dispatched++
				w.releaseConcurrencySlots(ctx, job.ID)
				w.untrackQueueJob(job.ID)
				continue
			default:
			}
			break
		}
	}
	releasedBounces := int64(len(store.getReleasedJobIDs()))
	return soakResult{
		ticks:       ticks,
		dispatched:  dispatched,
		dequeues:    store.dequeueCalls.Load(),
		releases:    releasedBounces,
		rateChecks:  store.rateChecks.Load(),
		rateAllowed: store.rateAllowed.Load(),
	}
}

func newSaturationWorker(store *soakStorage, concurrency int) *Worker {
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(concurrency)),
		RateLimit("fleet", 1_000_000),     // unkeyed; the mock decides allow/deny
		WithDequeueBatchSize(concurrency), // exercise the realistic batch path
		DisableRetry(),
	)
	w.config.WorkerID = "worker-soak"
	return w
}

// TestSaturatedDispatch_ChurnBaseline is the PRE-FIX baseline: with free
// concurrency and a permanently-saturated unkeyed fleet rate limit, the worker
// claims and bounces jobs every tick, making zero progress. This documents the
// cto-F2 amplification the fix must collapse, and asserts the invariants that
// must hold both before AND after the fix (no dispatch under saturation, every
// claim refunded back to pending, attempt-neutral). Packet 2 adds the
// churn-collapse bound that only passes once dequeueSlots is saturation-aware.
func TestSaturatedDispatch_ChurnBaseline(t *testing.T) {
	const concurrency = 8
	const ticks = 20
	store := newSoakStorage(concurrency * 4)
	store.allow.Store(false) // saturated for the whole run
	w := newSaturationWorker(store, concurrency)

	res := runSaturationSoak(t, w, store, concurrency, ticks)

	// Invariants (hold pre- and post-fix):
	assert.Equal(t, 0, res.dispatched, "a saturated fleet limit must dispatch nothing")
	assert.Equal(t, int64(0), res.rateAllowed, "no rate unit may be consumed while saturated")
	assert.Equal(t, store.pendingCount(), len(store.order),
		"every claimed job must be released back to pending (none left running)")
	min, max := store.attemptRange()
	assert.Equal(t, 0, min, "attempt-neutral across claim/bounce: no job below its pre-claim attempt")
	assert.Equal(t, 0, max, "attempt-neutral across claim/bounce: no job stuck above its pre-claim attempt")

	// Baseline characterization (NOT a tight bound here — Packet 2 tightens it):
	// pre-fix this is ~releaseBudget bounces every tick; the fix drives it to ~0.
	t.Logf("cto-F2 BASELINE over %d ticks @ concurrency=%d: dequeues=%d releases(bounces)=%d rateChecks=%d dispatched=%d => churn≈%.1f bounces/tick",
		res.ticks, concurrency, res.dequeues, res.releases, res.rateChecks, res.dispatched,
		float64(res.releases)/float64(res.ticks))
	assert.Greater(t, res.releases, int64(0), "baseline must exhibit claim->release churn (the bug being measured)")
}

// TestSaturatedDispatch_SelfHealsWhenHeadroomReturns proves the no-starvation
// invariant: a recovered rate limit dispatches work again, it is not wedged.
//
// For a FIXED-WINDOW limit, headroom genuinely returns only when the window
// rolls (the count resets) — so the throttle suppresses until the window
// boundary and the worker probes again then. We model the rollover by expiring
// the suppression cache (what a real clock crossing the window boundary does),
// then assert dispatch resumes. This is the documented "up to ~one window of
// added latency on a just-recovered limit" behavior, not a wedge.
func TestSaturatedDispatch_SelfHealsWhenHeadroomReturns(t *testing.T) {
	const concurrency = 4
	store := newSoakStorage(concurrency)
	store.allow.Store(false)
	w := newSaturationWorker(store, concurrency)

	// A few saturated ticks: nothing dispatches (the throttle suppresses after
	// the first probe batch).
	res := runSaturationSoak(t, w, store, concurrency, 5)
	require.Equal(t, 0, res.dispatched, "saturated: no dispatch")

	// The fixed window rolls AND headroom returns.
	store.allow.Store(true)
	expireRateSuppression(w)
	res2 := runSaturationSoak(t, w, store, concurrency, 3)
	assert.Greater(t, res2.dispatched, 0, "a recovered rate limit must dispatch work, not starve it")
}

// expireRateSuppression simulates the rate-limit window rolling over: it clears
// the worker's saturation cache so the next tick probes the (recovered) limit.
func expireRateSuppression(w *Worker) {
	w.rateSaturationMu.Lock()
	for k := range w.rateSaturatedUntil {
		delete(w.rateSaturatedUntil, k)
	}
	w.rateSaturationMu.Unlock()
}

// TestSaturatedDispatch_ThrottleCollapsesChurn is the post-fix proof: under a
// permanently-saturated unkeyed fleet limit, the saturation-aware dequeueSlots
// claims at most ONE probe batch and then suppresses, so total claims/bounces
// across many ticks stay bounded by a single batch instead of growing
// ~concurrency-per-tick (the pre-fix baseline this collapses).
func TestSaturatedDispatch_ThrottleCollapsesChurn(t *testing.T) {
	const concurrency = 8
	const ticks = 50
	store := newSoakStorage(concurrency * 4)
	store.allow.Store(false)
	w := newSaturationWorker(store, concurrency)

	res := runSaturationSoak(t, w, store, concurrency, ticks)

	assert.Equal(t, 0, res.dispatched, "still no dispatch while saturated")
	// One probe batch (<= concurrency) instead of ~concurrency*ticks bounces.
	// The whole 50-tick run claims/bounces no more than a single batch because
	// the test runs within one wall-clock rate window (no rollover).
	assert.LessOrEqual(t, res.releases, int64(concurrency),
		"throttle must collapse churn to a single probe batch, not bounce every tick")
	assert.LessOrEqual(t, res.rateChecks, int64(concurrency),
		"a suppressed tick must not even reach the DB rate gate")
	t.Logf("cto-F2 FIXED over %d ticks @ concurrency=%d: dequeues=%d releases=%d rateChecks=%d (was ~%d each pre-fix)",
		ticks, concurrency, res.dequeues, res.releases, res.rateChecks, concurrency*ticks)
}

// TestSaturatedDispatch_KeyedLimitNotSuppressed proves the scope boundary: a
// KEYED fleet limit cannot be pre-gated (its effective name needs the held
// job), so allRateLimitsUnkeyed is false and dequeueSlots NEVER suppresses —
// behavior is identical to the pre-throttle dispatch loop (continuous churn).
func TestSaturatedDispatch_KeyedLimitNotSuppressed(t *testing.T) {
	const concurrency = 8
	const ticks = 10
	store := newSoakStorage(concurrency * 4)
	store.allow.Store(false)
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(concurrency)),
		RateLimit("fleet", 1_000_000, RateLimitKey(func(*core.Job) string { return "k" })),
		WithDequeueBatchSize(concurrency),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-soak"
	require.False(t, w.allRateLimitsUnkeyed, "a keyed limit must disable the unkeyed throttle")

	res := runSaturationSoak(t, w, store, concurrency, ticks)
	assert.Equal(t, 0, res.dispatched)
	// No suppression: the worker keeps claiming+bouncing every tick, exactly as
	// before the throttle. (~concurrency bounces/tick.)
	assert.Greater(t, res.releases, int64(concurrency),
		"keyed config must NOT be throttled — churn continues unchanged")
	// And it records no suppressed ticks (the throttle never fires for keyed).
	assert.Equal(t, int64(0), w.DequeueSuppressedTicks(), "keyed config records no suppressed ticks")
}

// TestSaturatedDispatch_ChurnMetricsRecorded verifies the cto-F2 observability
// counters: a saturated unkeyed fleet limit records fleet_rate bounces AND
// suppressed ticks, and the counts reflect the collapse (a single probe batch
// of fleet_rate bounces, then many suppressed ticks).
func TestSaturatedDispatch_ChurnMetricsRecorded(t *testing.T) {
	const concurrency = 8
	const ticks = 30
	store := newSoakStorage(concurrency * 4)
	store.allow.Store(false)
	w := newSaturationWorker(store, concurrency)

	runSaturationSoak(t, w, store, concurrency, ticks)

	released := w.DequeueReleasedByReason()
	suppressed := w.DequeueSuppressedTicks()
	t.Logf("released=%v suppressedTicks=%d", released, suppressed)

	assert.Greater(t, released["fleet_rate"], int64(0), "the saturated fleet limit must record fleet_rate bounces")
	assert.LessOrEqual(t, released["fleet_rate"], int64(concurrency), "only one probe batch of fleet_rate bounces under the throttle")
	assert.Zero(t, released["queue_cap"], "no per-queue cap bounces in this scenario")
	assert.Zero(t, released["queue_rate"], "no queue-rate bounces in this scenario")
	assert.Greater(t, suppressed, int64(0), "suppressed ticks must be recorded once the throttle engages")
}

// TestKeyedSaturation_ChurnBaseline records the PRE-fix KEYED-limit churn — the
// gap v1 leaves, because a keyed limit disables the unkeyed throttle — and locks
// the invariants the Phase-1 cooldown (P1b) must preserve: hot-key jobs churn
// (claim->bounce->release) and never dispatch, COLD-key jobs DO dispatch (no
// starvation of healthy keys), and bounced hot-key jobs are refunded to pending
// at attempt 0. P1b will collapse the hot key's rateChecks (the DB locked-tx
// churn) while these invariants — and cold-key dispatch — stay put.
func TestKeyedSaturation_ChurnBaseline(t *testing.T) {
	const perKey = 6
	const concurrency = 16
	const ticks = 20
	coldKeys := []string{"cold-1", "cold-2"}
	store := newKeyedSoakStorage(perKey, "hot", coldKeys)
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(concurrency)),
		RateLimit("fleet", 1_000_000, RateLimitKey(func(j *core.Job) string { return j.Tenant })),
		WithDequeueBatchSize(concurrency),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-soak"
	require.False(t, w.allRateLimitsUnkeyed, "a keyed limit disables the v1 unkeyed throttle")

	res := runSaturationSoak(t, w, store, concurrency, ticks)

	// Cold keys dispatch — no starvation of healthy keys (the property P1b must keep).
	for _, ck := range coldKeys {
		pending, running, _ := store.tenantStats(ck)
		assert.Equal(t, perKey, running, "all %q jobs must dispatch (no starvation of a healthy key)", ck)
		assert.Equal(t, 0, pending, "no %q job left pending", ck)
	}
	assert.Equal(t, perKey*len(coldKeys), res.dispatched, "exactly the cold-key jobs dispatch")

	// Hot key churns: bounced repeatedly, ends pending and attempt-neutral.
	pending, running, maxAttempt := store.tenantStats("hot")
	assert.Equal(t, perKey, pending, "hot-key jobs end pending (bounced, never dispatched)")
	assert.Equal(t, 0, running, "no hot-key job left running")
	assert.Equal(t, 0, maxAttempt, "hot-key bounces are attempt-neutral (refunded)")

	// churn-1 (claim->release row churn) is asserted here because it survives P1b
	// (the per-key cooldown removes only the DB rate check, churn-2); churn-1 only
	// goes away in the optional Phase 2. rateChecks is the churn-2 BASELINE P1b
	// collapses, so it is only LOGGED here (a hard lower bound would not survive
	// P1b) — the collapse assertion lives in P1b, and the real-DB baseline is
	// recorded in benchmarks/ratelimit_churn_bench_test.go.
	assert.Greater(t, res.releases, int64(concurrency), "hot key churns claim->release every tick (churn-1, survives P1b)")
	t.Logf("cto-F2 KEYED BASELINE over %d ticks: dispatched=%d releases=%d rateChecks=%d (hot churns, cold dispatched; P1b collapses rateChecks)",
		ticks, res.dispatched, res.releases, res.rateChecks)
}

// rateSaturationCacheLen reads the saturated-bucket cache size (test-only,
// same-package) for the cap-bound assertion.
func rateSaturationCacheLen(w *Worker) int {
	w.rateSaturationMu.Lock()
	defer w.rateSaturationMu.Unlock()
	return len(w.rateSaturatedUntil)
}

// TestKeyedSaturation_CooldownCollapsesRateChecks is the P1b proof: the per-key
// cooldown skips the DB rate transaction for a hot keyed bucket already denied
// this window, so rateChecks (churn-2) collapse far below releases — while the
// invariants P1a locked still hold (cold-key dispatch unchanged, churn-1 stays,
// attempt-neutral). The whole run is within one wall-clock rate window, so the
// hot key pays ~one DB check total.
func TestKeyedSaturation_CooldownCollapsesRateChecks(t *testing.T) {
	const perKey = 6
	const concurrency = 16
	const ticks = 30
	coldKeys := []string{"cold-1", "cold-2"}
	store := newKeyedSoakStorage(perKey, "hot", coldKeys)
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(concurrency)),
		RateLimit("fleet", 1_000_000, RateLimitKey(func(j *core.Job) string { return j.Tenant })),
		WithDequeueBatchSize(concurrency),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-soak"

	res := runSaturationSoak(t, w, store, concurrency, ticks)

	// Cold keys still dispatch (no starvation) and churn-1 still persists...
	assert.Equal(t, perKey*len(coldKeys), res.dispatched, "cold-key dispatch unchanged by the cooldown")
	assert.Greater(t, res.releases, int64(concurrency), "hot key still churns claim->release (churn-1 untouched)")
	// ...but churn-2 has collapsed: the hot key pays ~one DB check for the whole
	// window, so total rateChecks (cold + ~1 hot probe) is far below releases and
	// bounded by cold checks plus at most one hot probe batch.
	assert.Less(t, res.rateChecks, res.releases/2, "cooldown collapses the DB rate churn far below the claim/release churn")
	assert.LessOrEqual(t, res.rateChecks, int64(perKey*(len(coldKeys)+1)), "rateChecks ~= cold checks + one hot probe")

	// P1c observability: the cooldown-skipped bounces are attributed to
	// fleet_rate_cached, distinct from the (small) fleet_rate count of bounces
	// that actually paid the DB tx; the hot key is in the saturation cache.
	released := w.DequeueReleasedByReason()
	assert.Greater(t, released["fleet_rate_cached"], int64(concurrency), "most hot-key bounces skip the DB tx -> attributed fleet_rate_cached")
	assert.Greater(t, released["fleet_rate_cached"], released["fleet_rate"], "cooldown skips dominate the few real DB denials")
	assert.GreaterOrEqual(t, w.DequeueRateSaturationCacheSize(), int64(1), "the saturated hot key is cached")
	t.Logf("cto-F2 KEYED P1b/P1c over %d ticks: dispatched=%d releases=%d rateChecks=%d fleet_rate=%d fleet_rate_cached=%d cacheSize=%d",
		ticks, res.dispatched, res.releases, res.rateChecks, released["fleet_rate"], released["fleet_rate_cached"], w.DequeueRateSaturationCacheSize())
}

// TestKeyedSaturation_CacheCapBoundsAndDegrades proves the bounded-memory
// guarantee: with cap=1 and TWO saturated hot keys, the cache holds at most one,
// and the second (uncached) hot key gracefully degrades to paying the DB rate
// transaction on every bounce — so the map never grows past the cap regardless
// of key cardinality.
func TestKeyedSaturation_CacheCapBoundsAndDegrades(t *testing.T) {
	const perKey = 4
	const concurrency = 16
	const ticks = 20
	// Two hot keys, both saturated; cap of 1 can cache only one of them.
	store := newKeyedSoakStorage(perKey, "hot-a", []string{"hot-b"})
	store.keyDeny = func(limitName string) bool {
		return strings.HasSuffix(limitName, ":hot-a") || strings.HasSuffix(limitName, ":hot-b")
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(concurrency)),
		RateLimit("fleet", 1_000_000, RateLimitKey(func(j *core.Job) string { return j.Tenant })),
		WithDequeueBatchSize(concurrency),
		WithRateSaturationCacheSize(1),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-soak"
	require.Equal(t, 1, w.config.rateSaturationCap)

	res := runSaturationSoak(t, w, store, concurrency, ticks)

	assert.Equal(t, 0, res.dispatched, "both keys saturated -> nothing dispatches")
	assert.LessOrEqual(t, rateSaturationCacheLen(w), 1, "the saturation cache must never exceed the configured cap")
	// The uncached hot key keeps paying the DB tx every bounce, so rateChecks stay
	// high (graceful degrade to v1-keyed behavior for the over-cap bucket) — far
	// above the single-cached-key collapse.
	assert.Greater(t, res.rateChecks, int64(ticks), "the over-cap hot key degrades to paying the DB rate tx every tick")
	t.Logf("cto-F2 KEYED cap=1 over %d ticks: releases=%d rateChecks=%d cacheLen=%d",
		ticks, res.releases, res.rateChecks, rateSaturationCacheLen(w))
}
