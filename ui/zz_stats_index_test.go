package ui

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// job_stats carries one index, unique on (queue, timestamp). Its LEADING column
// is queue, so neither query that scans by time alone can use it: the retention
// prune (DELETE ... WHERE timestamp < ?) and the all-queues history read. Both
// scanned the whole table — which grows by a row per queue per minute and is
// only ever trimmed by the very prune that could not use an index.
//
// pkg/storage carries the same index as versioned migration v38 (the copy that
// holds the fleet lock). This one covers what v38 structurally cannot: on a
// first-ever boot Migrate() runs BEFORE the dashboard is mounted, so job_stats
// does not exist yet, v38 correctly no-ops, and it is recorded as applied and
// never runs again.
//
// FALSE-GREEN TRAP: asserting MigrateStats returns nil passes with the index
// entirely absent. The discriminating observation is the index itself.
func TestMigrateStats_CreatesTheTimestampIndex(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	s := &gormStatsStorage{db: db}

	require.NoError(t, s.MigrateStats(context.Background()))
	assert.True(t, db.Migrator().HasIndex(&JobStat{}, jobStatsTimestampIndex),
		"the prune and the all-queues history read scan by timestamp alone and cannot use the "+
			"(queue, timestamp) unique index")
}

// TestMigrateStats_IsIdempotent covers the re-mount and the multi-process boot:
// MigrateStats runs without the fleet lock, so it must tolerate the index
// already existing rather than failing the dashboard mount.
func TestMigrateStats_IsIdempotent(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	s := &gormStatsStorage{db: db}

	for range 3 {
		require.NoError(t, s.MigrateStats(context.Background()))
	}
	assert.True(t, db.Migrator().HasIndex(&JobStat{}, jobStatsTimestampIndex))
}

// TestEnsureTimestampIndex_ToleratesAConcurrentCreator guards the recovery in the
// UI-side bootstrap — the copy that matters most, because it is the one that runs
// WITHOUT the storage fleet lock, on every dashboard mount.
//
// FALSE-GREEN TRAP, confirmed by a reviewer: TestMigrateStats_IsIdempotent calls
// MigrateStats three times in sequence, so every call after the first returns at
// the LEADING HasIndex guard and the error path is never entered. Deleting the
// recovery entirely left that test green. Reaching the recovery requires the index
// to appear after the guard has already passed, which is what creating it directly
// — behind MigrateStats' back — simulates.
func TestEnsureTimestampIndex_ToleratesAConcurrentCreator(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	s := &gormStatsStorage{db: db}
	ctx := context.Background()

	require.NoError(t, db.AutoMigrate(&JobStat{}))
	// A peer created it after our guard would have passed.
	require.NoError(t, db.Exec(
		"CREATE INDEX "+jobStatsTimestampIndex+" ON job_stats (timestamp)").Error)

	// Drive the CREATE path directly: the leading guard would otherwise short-circuit.
	require.NoError(t, s.createTimestampIndex(ctx),
		"losing the race must not fail the dashboard mount: the index is there, which is all "+
			"this wanted")
	assert.True(t, db.Migrator().HasIndex(&JobStat{}, jobStatsTimestampIndex))
}
