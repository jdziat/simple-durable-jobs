package storage

import (
	"context"
	"sync"
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

// TestMigrateJobStatsTimestampIndex_ToleratesAConcurrentCreator covers the race
// this wave itself introduced by adding a SECOND creator of this index.
//
// ui.gormStatsStorage.MigrateStats runs on every dashboard mount and holds NO
// fleet lock, so it can create the index between this migration's HasIndex check
// and its CREATE. Losing that race must not fail Migrate() — a process booting a
// moment after a peer mounted the dashboard would otherwise crash, and the
// mount-UI-then-Migrate ordering is one the Handler godoc explicitly supports.
//
// FALSE-GREEN TRAP, HIT ON THE FIRST ATTEMPT AT THIS TEST: creating the index
// BEFORE calling the migration does not test the recovery at all — the HasIndex
// guard returns early and the CREATE never runs, so it passes with the recovery
// deleted. The race has to be real, so this runs concurrent creators and lets them
// collide. SQLite serialises transactions and cannot exhibit it; there the test
// only asserts the migration stays correct.
func TestMigrateJobStatsTimestampIndex_ToleratesAConcurrentCreator(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	if db.Name() == "sqlite" {
		// openTestDB gives SQLite a bare `:memory:` database, where every pooled
		// connection gets its OWN private one — racing goroutines there fail with
		// "no such table", which tests the harness, not the migration. Both
		// external CI legs run this.
		t.Skip("`:memory:` gives each pooled connection a private database; the pg/mysql legs cover this")
	}
	dropJobStats(t, db)

	require.NoError(t, db.WithContext(ctx).Exec(
		`CREATE TABLE job_stats (id integer primary key, queue varchar(255), timestamp timestamp)`,
	).Error)

	const racers = 16
	errs := make(chan error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release them together so the guards pass before any CREATE lands
			errs <- migrateJobStatsTimestampIndex(ctx, db, db.Name())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err,
			"losing the race to a concurrent creator must not fail the migration: the index is "+
				"there, which is the only thing it wanted")
	}
	assert.True(t, db.Migrator().HasIndex(jobStatsTable, jobStatsTimestampIndex))
}

// TestCreateJobStatsTimestampIndex_ToleratesALostRace is the DETERMINISTIC form
// of the test above.
//
// The concurrency test reaches the recovery branch only probabilistically — the
// racers can serialise and every one of them then returns at the leading HasIndex
// guard. This drives the CREATE directly with the index already present, which is
// exactly the state a lost race leaves, so the recovery is entered on every run
// and on every dialect including SQLite.
func TestCreateJobStatsTimestampIndex_ToleratesALostRace(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	dropJobStats(t, db)

	require.NoError(t, db.WithContext(ctx).Exec(
		`CREATE TABLE job_stats (id integer primary key, queue varchar(255), timestamp timestamp)`,
	).Error)
	require.NoError(t, db.WithContext(ctx).Exec(
		"CREATE INDEX "+jobStatsTimestampIndex+" ON "+jobStatsTable+" (timestamp)").Error)

	require.NoError(t, createJobStatsTimestampIndex(ctx, db, db.Name()),
		"a CREATE that loses to a concurrent creator must not fail the migration: the index is "+
			"there, which is the only thing it wanted")
	assert.True(t, db.Migrator().HasIndex(jobStatsTable, jobStatsTimestampIndex))
}
