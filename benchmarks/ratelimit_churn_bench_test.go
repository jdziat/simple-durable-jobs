package benchmarks

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// cto-F2 real-DB load/soak harness: the black-box counterpart to
// pkg/worker/dequeue_saturation_test.go. It drives a worker with FREE
// concurrency against a fleet rate limit set far below dispatch capacity, so
// the dispatch loop continuously CLAIMS jobs (status->running) and BOUNCES them
// (Release back to pending) without progress — the cto-F2 write amplification.
//
// churnCountingStorage embeds the concrete *jobs.GormStorage so it PROMOTES
// every optional storage capability the worker selects via type-assertion
// (batchDequeuer, perQueueDequeuer, concurrencySlotStorage, rateLimiterStorage,
// promoter, recovery, the construction-time setters, ...). It overrides only the
// three dequeue paths + Release + TryConsumeRate to count them; everything else
// passes through transparently. NOTE: DequeueBatchPerQueue is the worker's FIRST
// choice (worker.go dequeueAvailableJobs), so it MUST be overridden too or the
// counts would be bypassed.
//
// Runs only against a real DB (PG/MySQL is where the locked-txn + serialization
// cost of the churn is real; sqlite's single writer masks it). Env-gated like
// the other benchmarks; warn-only in CI.
// ---------------------------------------------------------------------------

type churnCountingStorage struct {
	*jobs.GormStorage
	dequeues    atomic.Int64 // jobs claimed up to running
	releases    atomic.Int64 // jobs released back to pending (bounces, on the churn path)
	rateChecks  atomic.Int64 // TryConsumeRate calls (each a locked txn on PG/MySQL)
	rateAllowed atomic.Int64
}

func (s *churnCountingStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	j, err := s.GormStorage.Dequeue(ctx, queues, workerID)
	if err == nil && j != nil {
		s.dequeues.Add(1)
	}
	return j, err
}

func (s *churnCountingStorage) DequeueBatch(ctx context.Context, queues []string, workerID string, limit int) ([]*core.Job, error) {
	js, err := s.GormStorage.DequeueBatch(ctx, queues, workerID, limit)
	s.dequeues.Add(int64(len(js)))
	return js, err
}

func (s *churnCountingStorage) DequeueBatchPerQueue(ctx context.Context, workerID string, budgets map[string]int) ([]*core.Job, error) {
	js, err := s.GormStorage.DequeueBatchPerQueue(ctx, workerID, budgets)
	s.dequeues.Add(int64(len(js)))
	return js, err
}

func (s *churnCountingStorage) Release(ctx context.Context, jobID core.UUID, workerID string) error {
	s.releases.Add(1)
	return s.GormStorage.Release(ctx, jobID, workerID)
}

func (s *churnCountingStorage) TryConsumeRate(ctx context.Context, limitName string, perSecond float64, window time.Duration, now time.Time) (bool, error) {
	s.rateChecks.Add(1)
	ok, err := s.GormStorage.TryConsumeRate(ctx, limitName, perSecond, window, now)
	if ok {
		s.rateAllowed.Add(1)
	}
	return ok, err
}

const (
	churnFleetPerSecond = 10.0 // with the default 1s rate window => ceiling 10/window
	churnConcurrency    = 50
	churnBacklog        = 1500 // ample headroom over the ~260 claims a 2s soak makes
	churnSoakDuration   = 2 * time.Second
)

// BenchmarkSaturatedRateLimitChurn records the cto-F2 baseline (and, post-fix,
// proves the collapse): with a fleet limit of 10/s and concurrency 50, the
// worker can complete only ~10 jobs/s but — until the fix — claims and bounces
// the full concurrency budget every poll tick. It reports completed-jobs
// throughput (must stay pinned at the fleet ceiling) alongside the wasted
// dequeue/release/rate-check counts (which the fix must collapse).
//
// RECORDED BASELINE (unfixed dispatch loop, 2026-06-16, this harness):
//
//	Postgres : completed=30 dequeues=260 releases=230 rateChecks=252 => 24.7 wasteOps/completed
//	MySQL    : completed=20 dequeues=200 releases=180 rateChecks=192 => 28.6 wasteOps/completed
//
// POST-FIX (saturation-feedback dequeue throttle, same harness):
//
//	Postgres : completed=30 dequeues=60 releases=30 rateChecks=60 => 5.0 wasteOps/completed (releases -87%)
//	MySQL    : completed=30 dequeues=60 releases=30 rateChecks=60 => 5.0 wasteOps/completed (releases -83%)
//
// Throughput (completed) holds at the fleet ceiling; churn collapses ~80-87%.
func BenchmarkSaturatedRateLimitChurn(b *testing.B) {
	runChurnSoak(b, false)
}

// BenchmarkSaturatedKeyedRateLimitChurn is the KEYED counterpart: a fleet limit
// keyed by tenant with all backlog on one hot tenant. v1's throttle is disabled
// for keyed configs, so this records the keyed baseline churn the Phase-1 per-key
// cooldown (P1b) must reduce (it collapses rateChecks; releases stay until the
// optional Phase 2).
//
// RECORDED KEYED BASELINE vs P1b (per-key cooldown), 2026-06-16, live, this harness:
//
//	            completed  dequeues  releases  rateChecks
//	PG baseline    30        280       250        275
//	PG + P1b       30        410       380         33   <- rateChecks -88%
//	MySQL baseline 30        190       160        190
//	MySQL + P1b    20        250       230         22   <- rateChecks -88%
//
// The HEADLINE metric is rateChecks: the cooldown collapses the contended,
// serialization-retried TryConsumeRate locked transaction (the saturation
// scaling bottleneck — every worker fighting the same window row's FOR UPDATE
// lock) by ~88% while completed holds at the per-tenant ceiling. releases
// (churn-1) RISE because each bounce is now a cheap single-row UPDATE instead of
// a locked tx, so the dispatch loop bounces faster — this all-hot-tenant soak is
// the worst case for that (a realistic mix has cold tenants doing real work
// instead of spinning). churn-1 is only removed by the optional Phase-2 dequeue
// pushdown. So wasteOps/completed (which weights a locked tx == an UPDATE) is the
// WRONG summary here; rateChecks is the metric that matters.
func BenchmarkSaturatedKeyedRateLimitChurn(b *testing.B) {
	runChurnSoak(b, true)
}

// runChurnSoak runs the fleet-rate-saturated dispatch soak and reports churn vs
// completed throughput. keyed=true keys the limit by tenant and puts the whole
// backlog on one hot tenant (the per-tenant-rate-limit hot-key shape).
func runChurnSoak(b *testing.B, keyed bool) {
	b.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" && os.Getenv("TEST_MYSQL_URL") == "" {
		b.Skip("cto-F2 churn benchmark targets PG/MySQL (sqlite single-writer masks lock contention); set TEST_DATABASE_URL or TEST_MYSQL_URL")
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store := &churnCountingStorage{GormStorage: jobs.NewGormStorage(openBenchDB(b))}
		require.NoError(b, store.Migrate(context.Background()))
		cleanupBenchDB(b, store.DB())
		q := jobs.New(store)

		var completed atomic.Int64
		q.Register("churn.work", func(context.Context, noopArgs) error { return nil })
		q.OnJobComplete(func(context.Context, *jobs.Job) { completed.Add(1) })

		ctx := context.Background()
		for j := 0; j < churnBacklog; j++ {
			opts := []jobs.Option{jobs.QueueOpt(benchQueue)}
			if keyed {
				opts = append(opts, jobs.WithTenant("hot"))
			}
			if _, err := q.Enqueue(ctx, "churn.work", noopArgs{N: j}, opts...); err != nil {
				b.Fatal(err)
			}
		}

		rateLimit := jobs.RateLimit("fleet", churnFleetPerSecond) // unkeyed; default 1s window
		if keyed {
			rateLimit = jobs.RateLimit("fleet", churnFleetPerSecond, jobs.RateLimitKey(func(j *jobs.Job) string { return j.Tenant }))
		}
		worker := jobs.NewWorker(q,
			jobs.WorkerQueue(benchQueue, jobs.Concurrency(churnConcurrency)),
			jobs.WithPollInterval(benchPollInterval),
			rateLimit,
			jobs.WithDrainTimeout(2*time.Second),
		)

		b.StartTimer()
		workerCtx, cancel := context.WithCancel(ctx)
		errCh := startWorker(workerCtx, worker)
		time.Sleep(churnSoakDuration)
		cancel()
		requireWorkerStopped(b, errCh)
		b.StopTimer()

		done := completed.Load()
		deq := store.dequeues.Load()
		rel := store.releases.Load()
		rc := store.rateChecks.Load()
		secs := churnSoakDuration.Seconds()
		b.ReportMetric(float64(done)/secs, "completed/s")
		b.ReportMetric(float64(rel)/secs, "releases/s")
		b.ReportMetric(float64(rc)/secs, "rateChecks/s")
		b.ReportMetric(float64(deq)/secs, "dequeues/s")
		if done > 0 {
			// write-amplification: claim+release+rate-check work per completed job.
			b.ReportMetric(float64(deq+rel+rc)/float64(done), "wasteOps/completed")
		}
		kind := "unkeyed"
		if keyed {
			kind = "keyed(tenant=hot)"
		}
		b.Logf("cto-F2 churn soak [%s] (%s, concurrency=%d, fleet=%.0f/s): completed=%d dequeues=%d releases=%d rateChecks=%d rateAllowed=%d",
			kind, churnSoakDuration, churnConcurrency, churnFleetPerSecond, done, deq, rel, rc, store.rateAllowed.Load())
	}
}
