package ui

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestStatsDB(t *testing.T) *gormStatsStorage {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := &gormStatsStorage{db: db}
	err = s.MigrateStats(context.Background())
	require.NoError(t, err)
	return s
}

func TestGormStatsStorage_UpsertAndQuery(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()
	ts := time.Now().Truncate(time.Minute)

	// First upsert creates a row
	err := s.UpsertStatCounters(ctx, "default", ts, 5, 2, 1)
	require.NoError(t, err)

	// Second upsert increments
	err = s.UpsertStatCounters(ctx, "default", ts, 3, 1, 0)
	require.NoError(t, err)

	// Snapshot queue depth
	err = s.SnapshotQueueDepth(ctx, "default", ts, 10, 3)
	require.NoError(t, err)

	// Query
	stats, err := s.GetStatsHistory(ctx, "", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)

	assert.Equal(t, "default", stats[0].Queue)
	assert.Equal(t, int64(8), stats[0].Completed)
	assert.Equal(t, int64(3), stats[0].Failed)
	assert.Equal(t, int64(1), stats[0].Retried)
	assert.Equal(t, int64(10), stats[0].Pending)
	assert.Equal(t, int64(3), stats[0].Running)
}

func TestGormStatsStorage_QueryByQueue(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()
	ts := time.Now().Truncate(time.Minute)

	err := s.UpsertStatCounters(ctx, "emails", ts, 5, 0, 0)
	require.NoError(t, err)
	err = s.UpsertStatCounters(ctx, "payments", ts, 3, 0, 0)
	require.NoError(t, err)

	stats, err := s.GetStatsHistory(ctx, "emails", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, "emails", stats[0].Queue)
}

func TestGormStatsStorage_Prune(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	recent := time.Now().Truncate(time.Minute)

	err := s.UpsertStatCounters(ctx, "default", old, 1, 0, 0)
	require.NoError(t, err)
	err = s.UpsertStatCounters(ctx, "default", recent, 1, 0, 0)
	require.NoError(t, err)

	pruned, err := s.PruneStats(ctx, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(1), pruned)

	stats, err := s.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, recent.Unix(), stats[0].Timestamp.Unix())
}

func TestGormStatsStorage_SnapshotUpdatesExisting(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()
	ts := time.Now().Truncate(time.Minute)

	// Create row with counters
	err := s.UpsertStatCounters(ctx, "default", ts, 5, 2, 1)
	require.NoError(t, err)

	// Snapshot should update pending/running without clobbering counters
	err = s.SnapshotQueueDepth(ctx, "default", ts, 20, 5)
	require.NoError(t, err)

	stats, err := s.GetStatsHistory(ctx, "", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)

	assert.Equal(t, int64(5), stats[0].Completed)
	assert.Equal(t, int64(2), stats[0].Failed)
	assert.Equal(t, int64(1), stats[0].Retried)
	assert.Equal(t, int64(20), stats[0].Pending)
	assert.Equal(t, int64(5), stats[0].Running)
}

func TestGormStatsStorage_MultipleTimeBuckets(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()

	ts1 := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	ts2 := time.Now().Truncate(time.Minute)

	err := s.UpsertStatCounters(ctx, "default", ts1, 10, 0, 0)
	require.NoError(t, err)
	err = s.UpsertStatCounters(ctx, "default", ts2, 20, 0, 0)
	require.NoError(t, err)

	stats, err := s.GetStatsHistory(ctx, "", ts1.Add(-time.Minute), ts2.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 2)

	// Results are ordered by timestamp ASC
	assert.Equal(t, int64(10), stats[0].Completed)
	assert.Equal(t, int64(20), stats[1].Completed)
}
