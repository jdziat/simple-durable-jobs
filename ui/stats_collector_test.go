package ui

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupCollectorTest(t *testing.T) (*StatsCollector, *gormStatsStorage, *queue.Queue) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	err = db.AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{}, &JobStat{})
	require.NoError(t, err)

	statsStore := &gormStatsStorage{db: db}
	store := storage.NewGormStorage(db)
	q := queue.New(store)

	collector := NewStatsCollector(q, statsStore)
	return collector, statsStore, q
}

func TestStatsCollector_EventDrivenCounters(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go collector.Start(ctx)
	collector.WaitReady()

	// Emit events
	job := &core.Job{ID: "test-1", Queue: "default", Type: "test"}
	q.Emit(&core.JobCompleted{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobCompleted{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobFailed{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobRetrying{Job: job, Attempt: 1, Timestamp: time.Now()})

	// Give the collector time to process events
	time.Sleep(200 * time.Millisecond)

	// Force a flush
	collector.Flush(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "default", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, int64(2), stats[0].Completed)
	assert.Equal(t, int64(1), stats[0].Failed)
	assert.Equal(t, int64(1), stats[0].Retried)
}

func TestStatsCollector_IgnoresIrrelevantEvents(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go collector.Start(ctx)
	collector.WaitReady()

	// Emit events that shouldn't create counters
	job := &core.Job{ID: "test-1", Queue: "default", Type: "test"}
	q.Emit(&core.JobStarted{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobPaused{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobResumed{Job: job, Timestamp: time.Now()})

	time.Sleep(200 * time.Millisecond)
	collector.Flush(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "default", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	assert.Empty(t, stats, "No stats rows should be created for non-counter events")
}

func TestStatsCollector_MultipleQueues(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go collector.Start(ctx)
	collector.WaitReady()

	q.Emit(&core.JobCompleted{Job: &core.Job{ID: "1", Queue: "emails", Type: "send"}, Timestamp: time.Now()})
	q.Emit(&core.JobCompleted{Job: &core.Job{ID: "2", Queue: "payments", Type: "charge"}, Timestamp: time.Now()})
	q.Emit(&core.JobFailed{Job: &core.Job{ID: "3", Queue: "emails", Type: "send"}, Timestamp: time.Now()})

	time.Sleep(200 * time.Millisecond)
	collector.Flush(ctx)

	ts := time.Now().Truncate(time.Minute)

	emailStats, err := statsStore.GetStatsHistory(ctx, "emails", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, emailStats, 1)
	assert.Equal(t, int64(1), emailStats[0].Completed)
	assert.Equal(t, int64(1), emailStats[0].Failed)

	paymentStats, err := statsStore.GetStatsHistory(ctx, "payments", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, paymentStats, 1)
	assert.Equal(t, int64(1), paymentStats[0].Completed)
	assert.Equal(t, int64(0), paymentStats[0].Failed)
}

func TestStatsCollector_Retention(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	err = db.AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{}, &JobStat{})
	require.NoError(t, err)

	statsStore := &gormStatsStorage{db: db}
	store := storage.NewGormStorage(db)
	q := queue.New(store)

	collector := NewStatsCollector(q, statsStore, WithStatsCollectorRetention(24*time.Hour))
	assert.Equal(t, 24*time.Hour, collector.retention)
}

func TestStatsCollector_Snapshot(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	// Put some jobs in the storage via the queue's underlying storage.
	store := q.Storage()
	_ = store.Enqueue(ctx, &core.Job{
		ID: "snap-1", Queue: "snap-queue", Type: "work", Status: core.StatusPending,
	})
	_ = store.Enqueue(ctx, &core.Job{
		ID: "snap-2", Queue: "snap-queue", Type: "work", Status: core.StatusPending,
	})

	// Call snapshot directly (unexported method, accessible from same package).
	collector.snapshot(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "snap-queue", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, int64(2), stats[0].Pending)
}

func TestStatsCollector_Prune(t *testing.T) {
	_, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	// Insert an old record and a recent one.
	old := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	recent := time.Now().Truncate(time.Minute)
	err := statsStore.UpsertStatCounters(ctx, "default", old, 1, 0, 0)
	require.NoError(t, err)
	err = statsStore.UpsertStatCounters(ctx, "default", recent, 1, 0, 0)
	require.NoError(t, err)

	collector := NewStatsCollector(q, statsStore, WithStatsCollectorRetention(24*time.Hour))
	collector.prune(ctx)

	all, err := statsStore.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	// Only the recent row should survive.
	assert.Len(t, all, 1)
	assert.Equal(t, recent.Unix(), all[0].Timestamp.Unix())
}

func TestStatsCollector_PruneZeroRetention(t *testing.T) {
	_, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	err := statsStore.UpsertStatCounters(ctx, "default", old, 1, 0, 0)
	require.NoError(t, err)

	// Zero retention means prune is a no-op.
	collector := NewStatsCollector(q, statsStore, WithStatsCollectorRetention(0))
	collector.prune(ctx)

	all, err := statsStore.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.Len(t, all, 1, "zero retention should skip pruning")
}

func TestStatsCollector_SnapshotStorageError(t *testing.T) {
	// snapshot silently ignores GetJobsByStatus errors via continue; this test
	// verifies snapshot does not panic or return an error when storage fails.
	_, _, _ = setupCollectorTest(t)
	ctx := context.Background()

	// Use a broken storage that always errors.
	brokenStore := queue.New(&errStorage{})
	collector := NewStatsCollector(brokenStore, &nopStatsStorage{}, WithStatsCollectorRetention(time.Hour))

	// Should complete without panic.
	assert.NotPanics(t, func() { collector.snapshot(ctx) })
}

// errStorage is a storage that returns errors for GetJobsByStatus.
type errStorage struct{ mockStorage }

func (e *errStorage) GetJobsByStatus(_ context.Context, _ core.JobStatus, _ int) ([]*core.Job, error) {
	return nil, errStorageError
}

var errStorageError = errors.New("simulated storage error")

// nopStatsStorage is a no-op StatsStorage.
type nopStatsStorage struct{}

func (n *nopStatsStorage) MigrateStats(_ context.Context) error { return nil }
func (n *nopStatsStorage) UpsertStatCounters(_ context.Context, _ string, _ time.Time, _, _, _ int64) error {
	return nil
}
func (n *nopStatsStorage) SnapshotQueueDepth(_ context.Context, _ string, _ time.Time, _, _ int64) error {
	return nil
}
func (n *nopStatsStorage) GetStatsHistory(_ context.Context, _ string, _, _ time.Time) ([]JobStat, error) {
	return nil, nil
}
func (n *nopStatsStorage) PruneStats(_ context.Context, _ time.Time) (int64, error) { return 0, nil }
