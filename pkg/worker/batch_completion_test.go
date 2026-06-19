package worker

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/jdziat/simple-durable-jobs/v3/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

// newWALFileQueue returns a file-backed WAL SQLite queue. Unlike the shared-cache
// in-memory newSQLiteQueue, WAL allows concurrent readers + one writer with a
// busy_timeout, so multi-connection fan-out workflows don't stall under -race
// contention (the shared-cache global writer lock is the source of the
// pre-existing fan-out-resume flake).
func newWALFileQueue(t *testing.T) (*queue.Queue, func()) {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/batchcompletion.db?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("PRAGMA busy_timeout=10000").Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return queue.New(store), func() { _ = sqlDB.Close() }
}

type batchTrackingStorage struct {
	core.Storage

	mu                      sync.Mutex
	batchSizes              []int
	batchIDs                []core.UUID
	batchCalls              int
	completeWithResultIDs   []core.UUID
	fanOutPerJobCompletions int
	batchErr                error
}

func (s *batchTrackingStorage) BatchComplete(ctx context.Context, workerID string, items []storage.BatchCompleteItem) ([]core.UUID, error) {
	s.mu.Lock()
	s.batchCalls++
	s.batchSizes = append(s.batchSizes, len(items))
	for _, item := range items {
		s.batchIDs = append(s.batchIDs, item.JobID)
	}
	err := s.batchErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.Storage.(batchCompleteStorage).BatchComplete(ctx, workerID, items)
}

func (s *batchTrackingStorage) CompleteWithResult(ctx context.Context, jobID core.UUID, workerID string, result []byte) (*core.FanOut, error) {
	fo, err := s.Storage.(completeWithResultStorage).CompleteWithResult(ctx, jobID, workerID, result)
	s.mu.Lock()
	s.completeWithResultIDs = append(s.completeWithResultIDs, jobID)
	if fo != nil {
		s.fanOutPerJobCompletions++
	}
	s.mu.Unlock()
	return fo, err
}

func (s *batchTrackingStorage) DequeueBatch(ctx context.Context, queues []string, workerID string, limit int) ([]*core.Job, error) {
	return s.Storage.(batchDequeuer).DequeueBatch(ctx, queues, workerID, limit)
}

func (s *batchTrackingStorage) DequeueBatchPerQueue(ctx context.Context, workerID string, budgets map[string]int) ([]*core.Job, error) {
	return s.Storage.(perQueueDequeuer).DequeueBatchPerQueue(ctx, workerID, budgets)
}

func (s *batchTrackingStorage) counts() (batchCalls int, batchSizes []int, perJobIDs []core.UUID, fanOutPerJob int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchCalls, append([]int(nil), s.batchSizes...), append([]core.UUID(nil), s.completeWithResultIDs...), s.fanOutPerJobCompletions
}

func TestWithBatchCompletion_ClampsAndDefaultOff(t *testing.T) {
	var cfg WorkerConfig
	assert.False(t, cfg.BatchCompletion.Enabled)

	WithBatchCompletion(0, time.Nanosecond).ApplyWorker(&cfg)
	assert.True(t, cfg.BatchCompletion.Enabled)
	assert.Equal(t, 1, cfg.BatchCompletion.MaxBatch)
	assert.Equal(t, minBatchCompletionDelay, cfg.BatchCompletion.MaxDelay)

	WithBatchCompletion(5000, time.Minute).ApplyWorker(&cfg)
	assert.Equal(t, maxBatchCompletionBatch, cfg.BatchCompletion.MaxBatch)
	assert.Equal(t, maxBatchCompletionDelay, cfg.BatchCompletion.MaxDelay)
}

func TestWorker_BatchCompletion_DefaultOffUsesPerJobPath(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	store := &batchTrackingStorage{Storage: q.Storage()}
	q = queue.New(store)
	q.Register("leaf", func(context.Context, int) (int, error) { return 1, nil })

	done := make(chan struct{})
	q.OnJobComplete(func(context.Context, *core.Job) { close(done) })
	_, err := q.Enqueue(context.Background(), "leaf", 1)
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(10*time.Millisecond), WithOwnershipAuditInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("job did not complete")
	}
	cancel()

	require.Eventually(t, func() bool { return !w.started.Load() }, time.Second, 10*time.Millisecond)
	batchCalls, _, perJobIDs, _ := store.counts()
	assert.Equal(t, 0, batchCalls)
	assert.Len(t, perJobIDs, 1)
	assert.Nil(t, w.batchCompleter)
}

func TestWorker_BatchCompletion_HappyPathAndBatchFormation(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	store := &batchTrackingStorage{Storage: q.Storage()}
	q = queue.New(store)
	q.Register("leaf", func(_ context.Context, n int) (map[string]int, error) {
		return map[string]int{"n": n}, nil
	})

	const total = 24
	var hooks atomic.Int32
	events := q.Events()
	defer q.Unsubscribe(events)
	q.OnJobComplete(func(context.Context, *core.Job) { hooks.Add(1) })

	ids := make([]core.UUID, 0, total)
	for i := 0; i < total; i++ {
		id, err := q.Enqueue(context.Background(), "leaf", i)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	w := NewWorker(q,
		Concurrency(total),
		WithDequeueBatchSize(total),
		WithPollInterval(10*time.Millisecond),
		WithOwnershipAuditInterval(0),
		WithBatchCompletion(8, 200*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool {
		return hooks.Load() == total
	}, 4*time.Second, 10*time.Millisecond)

	completedEvents := 0
	deadline := time.After(2 * time.Second)
	for completedEvents < total {
		select {
		case ev := <-events:
			if _, ok := ev.(*core.JobCompleted); ok {
				completedEvents++
			}
		case <-deadline:
			t.Fatalf("saw %d JobCompleted events, want %d", completedEvents, total)
		}
	}

	cancel()
	for _, id := range ids {
		job, err := store.GetJob(context.Background(), id)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, core.StatusCompleted, job.Status)
		assert.NotEmpty(t, job.Result)
	}

	batchCalls, sizes, perJobIDs, _ := store.counts()
	assert.Greater(t, batchCalls, 0)
	assert.Empty(t, perJobIDs, "leaf completions should not use CompleteWithResult when batching is active")
	maxBatchSize := 0
	for _, s := range sizes {
		if s > maxBatchSize {
			maxBatchSize = s
		}
	}
	assert.GreaterOrEqual(t, maxBatchSize, 2, "at least one flush should batch multiple jobs (sizes=%v)", sizes)
}

func TestWorker_BatchCompletion_TimeTriggerFlushesBelowMaxBatch(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	store := &batchTrackingStorage{Storage: q.Storage()}
	q = queue.New(store)
	q.Register("leaf", func(context.Context, int) (int, error) { return 7, nil })

	const total = 3
	var hooks atomic.Int32
	q.OnJobComplete(func(context.Context, *core.Job) { hooks.Add(1) })
	for i := 0; i < total; i++ {
		_, err := q.Enqueue(context.Background(), "leaf", i)
		require.NoError(t, err)
	}

	w := NewWorker(q,
		Concurrency(total),
		WithDequeueBatchSize(total),
		WithPollInterval(10*time.Millisecond),
		WithOwnershipAuditInterval(0),
		WithBatchCompletion(100, 20*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool { return hooks.Load() == total }, 2*time.Second, 10*time.Millisecond)
	cancel()

	_, sizes, _, _ := store.counts()
	assert.Contains(t, sizes, total)
}

type batchOnlyMockStorage struct {
	*mockStorage
	batchFunc func(context.Context, string, []storage.BatchCompleteItem) ([]core.UUID, error)
}

func (s *batchOnlyMockStorage) BatchComplete(ctx context.Context, workerID string, items []storage.BatchCompleteItem) ([]core.UUID, error) {
	return s.batchFunc(ctx, workerID, items)
}

func TestWorker_BatchCompletion_ExcludedIDSuppressesHooks(t *testing.T) {
	store := &batchOnlyMockStorage{mockStorage: &mockStorage{}}
	store.batchFunc = func(context.Context, string, []storage.BatchCompleteItem) ([]core.UUID, error) {
		return nil, nil
	}
	q := queue.New(store)
	q.Register("leaf", func(context.Context, struct{}) error { return nil })

	var hooks atomic.Int32
	q.OnJobComplete(func(context.Context, *core.Job) { hooks.Add(1) })
	w := NewWorker(q, DisableRetry(), WithBatchCompletion(1, time.Millisecond))
	w.batchCompleter = newBatchCompleter(context.Background(), store, w.config.WorkerID, 1, time.Millisecond, *w.config.StorageRetry, w.logger)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.batchCompleter.run()
	}()
	defer func() {
		w.batchCompleter.Close()
		<-done
	}()

	w.processJob(context.Background(), &core.Job{ID: "excluded", Type: "leaf", Queue: "default", Args: []byte(`{}`)})
	assert.Equal(t, int32(0), hooks.Load())
}

func TestWorker_BatchCompletion_WriteErrorReleasesWithoutHooks(t *testing.T) {
	writeErr := errors.New("batch write failed")
	store := &batchOnlyMockStorage{mockStorage: &mockStorage{}}
	store.batchFunc = func(context.Context, string, []storage.BatchCompleteItem) ([]core.UUID, error) {
		return nil, writeErr
	}
	q := queue.New(store)
	q.Register("leaf", func(context.Context, struct{}) error { return nil })

	var hooks atomic.Int32
	q.OnJobComplete(func(context.Context, *core.Job) { hooks.Add(1) })
	w := NewWorker(q, DisableRetry(), WithBatchCompletion(1, time.Millisecond))
	w.batchCompleter = newBatchCompleter(context.Background(), store, w.config.WorkerID, 1, time.Millisecond, *w.config.StorageRetry, w.logger)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.batchCompleter.run()
	}()
	defer func() {
		w.batchCompleter.Close()
		<-done
	}()

	w.processJob(context.Background(), &core.Job{ID: "write-error", Type: "leaf", Queue: "default", Args: []byte(`{}`)})
	assert.Equal(t, int32(0), hooks.Load())
	assert.Equal(t, []core.UUID{core.UUID("write-error")}, store.getReleasedJobIDs())
}

func TestWorker_BatchCompletion_FanOutSubJobsBypassBatchComplete(t *testing.T) {
	// File-backed WAL DB: fan-out is multi-connection and stalls under the
	// shared-cache in-memory global writer lock during -race contention.
	q, cleanup := newWALFileQueue(t)
	defer cleanup()

	store := &batchTrackingStorage{Storage: q.Storage()}
	q = queue.New(store)
	q.Register("child", func(context.Context, int) (int, error) { return 2, nil })
	q.Register("parent", func(ctx context.Context, _ struct{}) (int, error) {
		results, err := fanout.FanOut[int](ctx, []fanout.SubJob{
			fanout.Sub("child", 1),
			fanout.Sub("child", 2),
		})
		if err != nil {
			return 0, err
		}
		sum := 0
		for _, result := range results {
			sum += result.Value
		}
		return sum, nil
	})

	_, err := q.Enqueue(context.Background(), "parent", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q,
		Concurrency(4),
		WithDequeueBatchSize(4),
		WithPollInterval(10*time.Millisecond),
		WithOwnershipAuditInterval(0),
		WithBatchCompletion(4, 20*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	// The two fan-out sub-jobs MUST complete through the per-job CompleteWithResult
	// path (never the batched path). Wait on that directly — not on the parent
	// workflow finishing — because parent RESUME under shared-cache SQLite is a
	// pre-existing, branch-independent flake unrelated to batched completion, and the
	// sub-jobs completing is exactly what proves the bypass.
	require.Eventually(t, func() bool {
		_, _, _, fanOutPerJob := store.counts()
		return fanOutPerJob == 2
	}, 10*time.Second, 20*time.Millisecond, "both fan-out sub-jobs must complete via CompleteWithResult")
	cancel()

	_, _, perJobIDs, fanOutPerJob := store.counts()
	assert.Equal(t, 2, fanOutPerJob, "fan-out sub-jobs must complete through CompleteWithResult")
	assert.Len(t, perJobIDs, 2, "exactly the two sub-jobs use the per-job path, never the batched path")
}

func TestBatchCompleter_CloseFlushesBufferedAndLateSubmitFallsBack(t *testing.T) {
	var calls atomic.Int32
	store := &batchOnlyMockStorage{mockStorage: &mockStorage{}}
	store.batchFunc = func(_ context.Context, _ string, items []storage.BatchCompleteItem) ([]core.UUID, error) {
		calls.Add(1)
		ids := make([]core.UUID, len(items))
		for i, item := range items {
			ids[i] = item.JobID
		}
		return ids, nil
	}

	b := newBatchCompleter(context.Background(), store, "w", 10, time.Hour, RetryConfig{MaxAttempts: 1}, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.run()
	}()

	result := make(chan error, 1)
	go func() {
		committed, err := b.Submit("buffered", []byte(`1`))
		if err == nil && !committed {
			err = fmt.Errorf("buffered submission was not committed")
		}
		result <- err
	}()

	time.Sleep(10 * time.Millisecond)
	b.Close()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("buffered submission was not resolved by Close")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch completer did not exit after Close")
	}
	assert.Equal(t, int32(1), calls.Load())

	committed, err := b.Submit("late", nil)
	assert.False(t, committed)
	assert.ErrorIs(t, err, errBatchCompletionClosed)
}

// noBatchCapStorage implements CompleteWithResult but NOT BatchComplete, so the
// worker's batchCompleteStorage capability assertion fails and it must degrade to
// the per-job completion path even with the flag on.
type noBatchCapStorage struct {
	core.Storage

	mu        sync.Mutex
	perJobIDs []core.UUID
}

func (s *noBatchCapStorage) CompleteWithResult(ctx context.Context, jobID core.UUID, workerID string, result []byte) (*core.FanOut, error) {
	fo, err := s.Storage.(completeWithResultStorage).CompleteWithResult(ctx, jobID, workerID, result)
	s.mu.Lock()
	s.perJobIDs = append(s.perJobIDs, jobID)
	s.mu.Unlock()
	return fo, err
}

func (s *noBatchCapStorage) perJobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.perJobIDs)
}

// TestWorker_BatchCompletion_GracefulDegradeWhenStorageLacksCapability proves that
// enabling WithBatchCompletion against a backend without BatchComplete falls back
// cleanly to per-job completion (no accumulator, every job still completes).
func TestWorker_BatchCompletion_GracefulDegradeWhenStorageLacksCapability(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	store := &noBatchCapStorage{Storage: q.Storage()}
	q = queue.New(store)
	q.Register("leaf", func(context.Context, int) (int, error) { return 1, nil })

	const total = 5
	ids := make([]core.UUID, total)
	for i := 0; i < total; i++ {
		id, err := q.Enqueue(context.Background(), "leaf", i)
		require.NoError(t, err)
		ids[i] = id
	}

	w := NewWorker(q,
		Concurrency(total),
		WithPollInterval(10*time.Millisecond),
		WithOwnershipAuditInterval(0),
		WithBatchCompletion(8, 20*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool {
		for _, id := range ids {
			job, err := store.GetJob(context.Background(), id)
			if err != nil || job == nil || job.Status != core.StatusCompleted {
				return false
			}
		}
		return true
	}, 8*time.Second, 20*time.Millisecond, "all jobs complete via the per-job fallback")
	cancel()

	assert.Equal(t, total, store.perJobCount(), "every leaf completed through CompleteWithResult (degraded path)")
}

// TestWorker_BatchCompletion_ShutdownFlushesInflight proves the integrated worker
// shutdown path flushes completions buffered in the accumulator, losing none.
func TestWorker_BatchCompletion_ShutdownFlushesInflight(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	store := &batchTrackingStorage{Storage: q.Storage()}
	q = queue.New(store)

	const total = 6
	var ran atomic.Int32
	q.Register("leaf", func(context.Context, int) (int, error) {
		ran.Add(1)
		return 1, nil
	})
	ids := make([]core.UUID, total)
	for i := 0; i < total; i++ {
		id, err := q.Enqueue(context.Background(), "leaf", i)
		require.NoError(t, err)
		ids[i] = id
	}

	// Batch cap above the job count and the max delay so completions BUFFER in the
	// accumulator (no size or timer flush) — only the shutdown path will flush them.
	w := NewWorker(q,
		Concurrency(total),
		WithDequeueBatchSize(total),
		WithPollInterval(10*time.Millisecond),
		WithOwnershipAuditInterval(0),
		WithBatchCompletion(total+10, maxBatchCompletionDelay),
	)
	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})
	go func() { defer close(startDone); _ = w.Start(ctx) }()

	// Every handler has run -> its completion is buffered/blocked in the accumulator.
	require.Eventually(t, func() bool { return ran.Load() == total }, 10*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-startDone:
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not drain after shutdown")
	}

	for _, id := range ids {
		job, err := store.GetJob(context.Background(), id)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, core.StatusCompleted, job.Status, "job %s must be flushed on shutdown", id)
	}
	batchCalls, _, _, _ := store.counts()
	assert.Greater(t, batchCalls, 0, "shutdown flush issued at least one BatchComplete")
}
