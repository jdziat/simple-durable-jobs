package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Migration v38 indexes job_stats(timestamp). The table is owned by the ui
// package, which imports this one, so it is named as a string and guarded by
// HasTable — a process that never mounts the dashboard has no such table.
//
// FALSE-GREEN TRAP: running Migrate on a fresh database and asserting no error
// passes trivially, because job_stats does not exist there and the migration
// correctly no-ops. The discriminating setup is a database where the table
// ALREADY exists — the upgrade shape this migration is for.
func TestMigrateJobStatsTimestampIndex_OnAnExistingTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	require.NoError(t, db.WithContext(ctx).Exec(
		`CREATE TABLE job_stats (id integer primary key, queue varchar(255), timestamp timestamp)`,
	).Error)

	require.NoError(t, migrateJobStatsTimestampIndex(ctx, db, db.Name()))
	assert.True(t, db.Migrator().HasIndex(jobStatsTable, jobStatsTimestampIndex),
		"the retention prune deletes by timestamp alone and cannot use the (queue, timestamp) index")

	require.NoError(t, migrateJobStatsTimestampIndex(ctx, db, db.Name()),
		"must be idempotent: it re-runs on any database that already has the index")
}

// TestMigrateJobStatsTimestampIndex_NoOpsWithoutTheTable covers the process that
// never mounts the dashboard, and the first-ever boot where Migrate() runs
// before the UI creates the table. ui.gormStatsStorage.MigrateStats carries the
// mirror that covers that second case.
func TestMigrateJobStatsTimestampIndex_NoOpsWithoutTheTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	require.False(t, db.Migrator().HasTable(jobStatsTable))
	require.NoError(t, migrateJobStatsTimestampIndex(ctx, db, db.Name()))
}
