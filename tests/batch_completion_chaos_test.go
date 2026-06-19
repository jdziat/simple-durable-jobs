package jobs_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// crashBeforeCommitStorage models a worker that is hard-killed in the middle of a
// group-committed flush: the moment after every leaf handler has returned success
// but BEFORE BatchComplete durably commits the batch.
//
// It embeds the concrete *storage.GormStorage (not the jobs.Storage interface) so
// that every storage capability method the worker selects — DequeueBatch,
// DequeueBatchPerQueue, CompleteWithResult, BatchComplete, SetLockDuration, ... —
// is promoted unchanged. Only BatchComplete is overridden, and it deliberately
// NEVER delegates to the real storage and NEVER returns: it records the attempt
// and blocks forever (until the test's cleanup releases `held`).
//
// This is the only faithful in-process crash model. A plain context cancel does
// NOT work: the worker's graceful shutdown drains the accumulator with a 5s
// detached-context flush (batchCompleter.flushOnClose -> context.WithoutCancel),
// which would COMMIT the buffered batch. Returning an error from BatchComplete
// does NOT work either: that is the graceful error-release path
// (TestWorker_BatchCompletion_WriteErrorReleasesWithoutHooks), not a crash. By
// blocking without committing, this wrapper guarantees the worker durably
// completes nothing, so the only path back to "done" is the stale-lock reaper on
// a second worker — exactly the at-least-once recovery this test must prove.
type crashBeforeCommitStorage struct {
	*storage.GormStorage

	held chan struct{} // closed by test cleanup to unblock the abandoned flusher

	mu       sync.Mutex
	attempts int         // number of BatchComplete calls that reached the kill point
	seenIDs  []jobs.UUID // job IDs the accumulator tried (and failed) to commit
}

func (s *crashBeforeCommitStorage) BatchComplete(ctx context.Context, workerID string, items []storage.BatchCompleteItem) ([]jobs.UUID, error) {
	s.mu.Lock()
	s.attempts++
	for _, it := range items {
		s.seenIDs = append(s.seenIDs, it.JobID)
	}
	s.mu.Unlock()

	// Process killed mid-commit: hang here forever (never commit, never return)
	// until the test tears down. The flush goroutine that called us is abandoned,
	// exactly as it would be on SIGKILL.
	<-s.held
	return nil, context.Canceled // only reached at cleanup; the result is discarded
}

func (s *crashBeforeCommitStorage) stats() (attempts int, ids []jobs.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts, append([]jobs.UUID(nil), s.seenIDs...)
}

// TestBatchCompletion_ChaosKillIsAtLeastOnce is the launch gate for the
// group-committed completion accumulator (WithBatchCompletion). It answers the
// "hard question": the accumulator defers a handler's success until the batch
// flushes, so a crash inside that window must NOT lose the job. We prove that a
// job whose handler succeeded but whose batch never committed is re-run by
// recovery and eventually completes — at-least-once, never zero.
//
// Runs against the configured backend (SQLite by default; Postgres/MySQL when
// TEST_DATABASE_URL / TEST_MYSQL_URL is set). The backend-specific commit
// semantics (MySQL datetime(3)/NOW(6) clock skew, binary result encoding) made
// this a multi-dialect concern, so it must pass on all three.
func TestBatchCompletion_ChaosKillIsAtLeastOnce(t *testing.T) {
	_, store := openIntegrationQueue(t)
	gorm, ok := store.(*storage.GormStorage)
	require.True(t, ok, "integration storage must be *GormStorage")

	// Short lease so the orphaned lock from the crashed worker becomes reclaimable
	// quickly. The reaper actually anchors on started_at (set at dequeue), so this
	// is belt-and-suspenders, but it keeps the locked_until COALESCE fallback short
	// too.
	gorm.SetLockDuration(2 * time.Second)

	const n = 4 // a whole in-flight batch is lost by the crash, not just one job

	// Run counter keyed by job ID, shared across the crashing and recovering
	// workers. A job that is re-executed by recovery increments past 1.
	var runs sync.Map // jobs.UUID -> *atomic.Int32
	bumpRun := func(ctx context.Context) {
		id := jobs.JobIDFromContext(ctx)
		v, _ := runs.LoadOrStore(id, new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
	}
	runCount := func(id jobs.UUID) int32 {
		v, ok := runs.Load(id)
		if !ok {
			return 0
		}
		return v.(*atomic.Int32).Load()
	}
	handler := func(ctx context.Context, x int) (int, error) {
		bumpRun(ctx)
		return x * 2, nil
	}

	// --- Worker 1: batched completion, fault-injected to crash mid-commit. ---
	held := make(chan struct{})
	crash := &crashBeforeCommitStorage{GormStorage: gorm, held: held}
	w1done := make(chan struct{})
	t.Cleanup(func() {
		close(held) // release the abandoned flusher so no goroutine leaks past the test
		select {
		case <-w1done:
		case <-time.After(5 * time.Second):
		}
	})

	qFault := jobs.New(crash)
	qFault.Register("leaf.chaos", handler)

	ids := make([]jobs.UUID, n)
	for i := 0; i < n; i++ {
		id, err := qFault.Enqueue(context.Background(), "leaf.chaos", i)
		require.NoError(t, err)
		ids[i] = id
	}

	// maxBatch == n so the SIZE trigger forms exactly one batch from all n
	// completions (they arrive in ms); the time trigger never fires first. NOTE:
	// WithBatchCompletion hard-clamps maxDelay to 5s, so we pass that ceiling
	// rather than a misleadingly larger value — either way the size trigger wins.
	ctx1, cancel1 := context.WithCancel(context.Background())
	w1 := qFault.NewWorker(
		jobs.WithScheduler(false),
		jobs.Concurrency(n),
		jobs.WithDequeueBatchSize(n),
		jobs.WithPollInterval(10*time.Millisecond),
		jobs.WithBatchCompletion(n, 5*time.Second),
	)
	go func() { defer close(w1done); _ = w1.Start(ctx1) }()

	// Wait until every leaf handler has returned success. At this point each job's
	// completion is buffered/blocked in the accumulator and (because the wrapper
	// never commits) durably unwritten — the crash window is open.
	require.Eventually(t, func() bool {
		for _, id := range ids {
			if runCount(id) < 1 {
				return false
			}
		}
		return true
	}, 30*time.Second, 10*time.Millisecond, "all %d leaf handlers must run (their success enters the accumulator)", n)

	// The accumulator must have actually formed a batch and tried to commit it
	// through the batched path — proving we exercise group completion, not the
	// per-job fallback. This ordering gate is also load-bearing: once the flush is
	// frozen inside the overridden BatchComplete, every leaf goroutine parks in
	// Submit awaiting a result that never arrives, so the per-job
	// errBatchCompletionClosed fallback (which would complete through the promoted
	// real CompleteWithResult) is never reached — the crash commits nothing.
	require.Eventually(t, func() bool {
		attempts, _ := crash.stats()
		return attempts >= 1
	}, 5*time.Second, 10*time.Millisecond, "the worker must form a batch and attempt BatchComplete")

	// CRASH: kill worker 1. Its flusher is frozen inside BatchComplete (blocked on
	// `held`), so shutdown cannot drain/commit it; its heartbeat loop stops, so the
	// lock goes stale. Nothing was durably completed.
	cancel1()

	// Sanity: prove the crash genuinely lost the work — no job is completed yet.
	// If this fails, the wrapper committed something and the test would be vacuous.
	for _, id := range ids {
		st, err := gorm.GetJob(context.Background(), id)
		require.NoError(t, err)
		require.NotNil(t, st)
		require.NotEqual(t, jobs.StatusCompleted, st.Status,
			"job %s must NOT be durably completed after a mid-batch crash", id)
	}

	// --- Worker 2: clean recoverer with a fast stale-lock reaper. ---
	qClean := jobs.New(gorm)
	qClean.Register("leaf.chaos", handler)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	w2 := qClean.NewWorker(
		jobs.WithScheduler(false),
		jobs.Concurrency(n),
		jobs.WithDequeueBatchSize(n),
		jobs.WithPollInterval(50*time.Millisecond),
		jobs.WithStaleLockInterval(500*time.Millisecond),
		jobs.WithStaleLockAge(2*time.Second),
		jobs.WithLockDuration(2*time.Second),
		jobs.WithBatchCompletion(n, 25*time.Millisecond),
	)
	go func() { _ = w2.Start(ctx2) }()

	// AT-LEAST-ONCE: every job lost in the crash window is reclaimed and completes.
	require.Eventually(t, func() bool {
		for _, id := range ids {
			st, err := gorm.GetJob(context.Background(), id)
			if err != nil || st == nil || st.Status != jobs.StatusCompleted {
				return false
			}
		}
		return true
	}, 45*time.Second, 100*time.Millisecond, "every crashed-mid-batch leaf must be recovered and completed (none lost)")

	// Recovery genuinely RE-EXECUTED each handler: ran once before the crash, then
	// at least once more during recovery. (If W2 had "completed" them without
	// re-running, the count would still be 1 and this would fail.)
	for _, id := range ids {
		assert.GreaterOrEqualf(t, runCount(id), int32(2),
			"job %s must run again during recovery (at-least-once), got %d runs", id, runCount(id))
	}

	// Results are persisted correctly through the recovered batched completion.
	for i, id := range ids {
		got, err := jobs.LoadResult[int](ctx2, qClean, id)
		require.NoErrorf(t, err, "load result for job %s", id)
		assert.Equalf(t, i*2, got, "job %s result after recovery", id)
	}

	cancel2()
}
