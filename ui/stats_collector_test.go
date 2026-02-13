package ui

import (
	"context"
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
	time.Sleep(50 * time.Millisecond) // Let collector subscribe to events

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
	time.Sleep(50 * time.Millisecond)

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
	time.Sleep(50 * time.Millisecond)

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
