package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

// TestDefaultBatchSizeDrainsRespectingQueueCap is the guard for the P13 default
// lift (DequeueBatchSize 1 -> 10). It proves that a freshly constructed worker
// using the new batched default still:
//
//   - actually picks up the new default (config.DequeueBatchSize == 10);
//   - drains a saturated single queue exactly once (200 distinct completions,
//     zero duplicate job IDs) under the batch path;
//   - never runs more jobs concurrently than the queue's configured cap, even
//     though a batch claim can over-claim past the cap (the surplus is released
//     before it ever runs);
//   - does not inflate any job's attempt count via the over-claim release
//     (Release decrements attempt, inverting the batch claim's attempt+1).
//
// It would catch a future regression where the higher default bypasses
// tryTrackQueueJob and over-runs the cap, or where Release stops decrementing
// attempt.
func TestDefaultBatchSizeDrainsRespectingQueueCap(t *testing.T) {
	// Isolated in-memory SQLite DB; cache=shared so every pooled connection sees
	// the same schema. We keep the *gorm.DB handle so we can scan attempt counts
	// after the drain.
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)

	const (
		totalJobs = 200
		queueCap  = 8
	)

	// A no-op handler that sleeps briefly so several jobs are genuinely in flight
	// at once; this is what lets the over-claim path push past the per-queue cap
	// if tryTrackQueueJob ever stopped enforcing it.
	q.Register("noop", func(ctx context.Context, args struct{}) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})

	// Instrument the per-queue concurrency gauge via the OnJobStart/OnJobComplete
	// hooks. The gauge is incremented when a handler starts and decremented when
	// it completes; we track the observed peak.
	var running atomic.Int64
	var peak atomic.Int64
	bumpPeak := func(cur int64) {
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				return
			}
		}
	}
	q.OnJobStart(func(_ context.Context, _ *core.Job) {
		bumpPeak(running.Add(1))
	})

	// Collect completed job IDs in a mutex-guarded set so we can assert
	// exactly-once (200 distinct completions, no duplicates).
	var mu sync.Mutex
	completed := make(map[string]int, totalJobs)
	done := make(chan struct{})
	q.OnJobComplete(func(_ context.Context, job *core.Job) {
		running.Add(-1)
		mu.Lock()
		completed[string(job.ID)]++
		n := len(completed)
		mu.Unlock()
		if n == totalJobs {
			select {
			case <-done:
			default:
				close(done)
			}
		}
	})

	// Enqueue 200 jobs into a single queue 'default'.
	for i := 0; i < totalJobs; i++ {
		_, err := q.Enqueue(context.Background(), "noop", struct{}{})
		require.NoError(t, err)
	}

	// Build the worker with NO WithDequeueBatchSize: it must take the new
	// batched default. Disable the background reapers so they cannot interfere
	// with the over-claim/release accounting we are asserting on.
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(queueCap)),
		WithPollInterval(10*time.Millisecond),
		WithStaleLockInterval(0),
		WithOwnershipAuditInterval(0),
	)

	// (a) the new default actually took effect.
	assert.Equal(t, 10, w.config.DequeueBatchSize,
		"freshly constructed worker should use the batched default of 10")

	// (b)+(c) drive a real dispatch+drain until all 200 complete.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		mu.Lock()
		n := len(completed)
		mu.Unlock()
		t.Fatalf("drain did not finish: only %d/%d jobs completed", n, totalJobs)
	}

	// Shut the worker down cleanly.
	cancel()
	select {
	case <-workerErr:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after context cancel")
	}

	// (b) exactly-once: 200 distinct completions, zero duplicates.
	mu.Lock()
	require.Len(t, completed, totalJobs, "expected exactly %d distinct completions", totalJobs)
	for id, count := range completed {
		assert.Equalf(t, 1, count, "job %s completed %d times (expected exactly once)", id, count)
	}
	mu.Unlock()

	// (c) the observed peak concurrent running jobs never exceeded the cap. The
	// over-claim-then-release path keeps the per-queue cap inviolate.
	assert.LessOrEqualf(t, peak.Load(), int64(queueCap),
		"peak concurrency %d exceeded the queue cap of %d under the batch path", peak.Load(), queueCap)

	// (d) post-drain DB scan: no completed job's attempt was inflated by
	// over-claim churn. Each job runs exactly once, so attempt must be 1; the
	// surplus claim/release is attempt-neutral (Release decrements the claim's
	// attempt+1 increment).
	var rows []core.Job
	require.NoError(t, db.Find(&rows).Error)
	require.Len(t, rows, totalJobs)
	for _, j := range rows {
		assert.Equalf(t, core.StatusCompleted, j.Status, "job %s not completed: %s", j.ID, j.Status)
		assert.LessOrEqualf(t, j.Attempt, 1, "job %s attempt %d inflated by over-claim release", j.ID, j.Attempt)
	}
}

// TestExplicitSingleRowOverridesDefault documents that operators can opt back
// into strict single-row dequeue: an explicit WithDequeueBatchSize(1) wins over
// the batched default because user options are applied after the default.
func TestExplicitSingleRowOverridesDefault(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), WithDequeueBatchSize(1))
	assert.Equal(t, 1, w.config.DequeueBatchSize)
}
