package metrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/worker"
)

// TestNoHandlerJobCountsAsFailed pins that a job the worker has no handler for is
// counted by jobs.failed.
//
// That job reaches a genuinely terminal state — status='failed' with
// dead_lettered_at set — but the no-handler branch of processJob used to fire no
// fail hook and emit no JobFailed event, so this counter (which is driven from
// OnJobFail) missed the entire class. It is exactly the class a partially-rolled
// out fleet produces: a worker whose registry is a subset of the enqueuer's
// dead-letters every unknown type while the alert built on jobs.failed stays flat.
//
// Enqueue rejects unregistered types, so the fixture uses TWO queues over ONE
// storage: the enqueuing queue knows the type, the worker's queue does not.
func TestNoHandlerJobCountsAsFailed(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	dsn := "file:" + t.TempDir() + "/nohandler.db?_journal_mode=WAL&_busy_timeout=10000&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = sqlDB.Close() })

	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(ctx))

	// The producer: knows the type, so Enqueue accepts it.
	producer := queue.New(store)
	producer.Register("orphaned-type", func(context.Context, struct{}) error { return nil })
	jobID, err := producer.Enqueue(ctx, "orphaned-type", struct{}{}, queue.Retries(0))
	require.NoError(t, err)

	// The consumer: a separate registry with no such handler — the rollout-skew
	// shape. Instrumented, so the counter under test is wired up.
	consumer := queue.New(store)
	Instrument(consumer, WithMeterProvider(mp))

	// The hook fires on the WORKER's goroutine; the assertions read from the test
	// goroutine. Guard both fields or -race reports the test itself.
	var hookMu sync.Mutex
	var failHookCalls int
	var failHookErr error
	consumer.OnJobFail(func(_ context.Context, _ *core.Job, err error) {
		hookMu.Lock()
		defer hookMu.Unlock()
		failHookCalls++
		failHookErr = err
	})
	readHook := func() (int, error) {
		hookMu.Lock()
		defer hookMu.Unlock()
		return failHookCalls, failHookErr
	}

	w := worker.NewWorker(consumer, worker.WithPollInterval(50*time.Millisecond))
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = w.Start(runCtx)
	}()
	defer func() {
		cancel()
		<-workerDone
	}()

	// The row must be genuinely terminal, or "no metric" would be the right answer.
	require.Eventually(t, func() bool {
		job, getErr := store.GetJob(ctx, jobID)
		return getErr == nil && job != nil &&
			job.Status == core.StatusFailed && job.DeadLetteredAt != nil
	}, 8*time.Second, 25*time.Millisecond,
		"a no-handler job must reach terminal failure with DLQ metadata")

	require.Eventually(t, func() bool {
		calls, _ := readHook()
		return calls > 0
	}, 5*time.Second, 25*time.Millisecond,
		"a terminal no-handler failure must fire the fail hooks; without it jobs.failed "+
			"misses every job whose type this worker does not know")

	calls, hookErr := readHook()
	assert.Equal(t, 1, calls, "exactly one terminal failure was reported")
	require.Error(t, hookErr)
	assert.Contains(t, hookErr.Error(), "no handler for orphaned-type",
		"the hook's error must say what actually happened; alert text and the DLQ "+
			"row's reason are built from the same error")

	rm := collectMetrics(t, reader)
	assertCounterPoint(t, rm, metricJobsFailed, 1, map[string]string{
		attrQueue: "default", attrJobType: "orphaned-type", attrOutcome: outcomeFailed,
	})
}
