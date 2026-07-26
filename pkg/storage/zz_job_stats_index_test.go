package storage

import (
	"context"
	"testing"

	"gorm.io/gorm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dropJobStats removes the ad-hoc job_stats table these tests create, before the
// test body and again after it.
//
// openTestDB's external-DB isolation (requireCleanStart / cleanupExternalDB) only
// DELETEs rows from externalTestTables and never DROPs a table — and job_stats is
// not one of them. So against the PERSISTENT Postgres and MySQL matrix databases
// the table the first test creates survives into the second (whose
// require.False(HasTable) then fails) and into the NEXT RUN (whose CREATE TABLE
// then fails "already exists"). SQLite gets a fresh :memory: database per test, so
// the defect is invisible on the default leg and reds only the two external legs.
func dropJobStats(t testing.TB, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Migrator().DropTable(jobStatsTable))
	t.Cleanup(func() { _ = db.Migrator().DropTable(jobStatsTable) })
}

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
	dropJobStats(t, db)

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
	dropJobStats(t, db)

	require.False(t, db.Migrator().HasTable(jobStatsTable))
	require.NoError(t, migrateJobStatsTimestampIndex(ctx, db, db.Name()))
}
