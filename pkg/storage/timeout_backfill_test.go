package storage

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestMigratePostgresBackfillsLegacyNullTimeoutAndDeterminism reproduces the
// production boot failure where a pre-v3 baseline carries NULL jobs.timeout (and
// jobs.determinism) rows. Both columns shipped originally untagged/nullable and
// were tightened to `not null;default:0` in the same commit, so the same legacy
// rows hold NULL for both. Applying the NOT NULL constraint (PG SET NOT NULL /
// MySQL MODIFY / migrateIntWidthRightsizing) aborts (PG 23502 / MySQL 1265) at
// boot. The pre-AutoMigrate heal (backfillDispatcherNulls) must coalesce both to 0
// BEFORE the constraint is applied so boot converges.
//
// The test guards the heal directly: after a re-Migrate over a NULL baseline, both
// NULLs are gone (backfilled to 0) AND the very ALTER the user saw fail —
// ALTER COLUMN ... SET NOT NULL — now succeeds. Remove the heal and the NULL rows
// survive Migrate, so that ALTER fails with 23502 and the test goes red.
func TestMigratePostgresBackfillsLegacyNullTimeoutAndDeterminism(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	schemaName := uniqueSchemaAssertionsName("null_backfill")
	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres admin db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+quotePostgresIdent(schemaName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP SCHEMA IF EXISTS "+quotePostgresIdent(schemaName)+" CASCADE").Error)
	})

	db, err := gorm.Open(postgres.Open(postgresDSNWithSearchPath(t, dsn, schemaName)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open isolated postgres schema")
	closeDBOnCleanup(t, db)

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx), "initial migrate")

	// Recreate the legacy shape: drop NOT NULL on both retro-tightened columns and
	// seed a row that holds NULL for both (a single pre-v3 row is NULL in both).
	require.NoError(t, db.Exec("ALTER TABLE jobs ALTER COLUMN timeout DROP NOT NULL").Error)
	require.NoError(t, db.Exec("ALTER TABLE jobs ALTER COLUMN determinism DROP NOT NULL").Error)
	require.NoError(t, db.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, updated_at)
		VALUES ('11111111-1111-1111-1111-111111111111'::uuid, 'legacy', 'default', 'pending', 0, 0, 3, NULL, NULL, now(), now())
	`).Error)

	// Re-migrate: the pre-AutoMigrate heal backfills both NULLs to 0. Boot succeeds.
	require.NoError(t, s.Migrate(ctx), "re-migrate over a legacy NULL baseline must succeed")

	var timeout, determinism int64
	require.NoError(t, db.Raw(
		"SELECT timeout FROM jobs WHERE id = '11111111-1111-1111-1111-111111111111'::uuid",
	).Scan(&timeout).Error)
	require.Equal(t, int64(0), timeout, "legacy NULL timeout backfilled to 0")
	require.NoError(t, db.Raw(
		"SELECT determinism FROM jobs WHERE id = '11111111-1111-1111-1111-111111111111'::uuid",
	).Scan(&determinism).Error)
	require.Equal(t, int64(0), determinism, "legacy NULL determinism backfilled to 0")

	// The exact operations the user saw fail must now succeed, because the heal
	// cleared the NULLs that would have violated them (23502).
	require.NoError(t, db.Exec("ALTER TABLE jobs ALTER COLUMN timeout SET NOT NULL").Error,
		"SET NOT NULL on timeout must succeed after the heal")
	require.NoError(t, db.Exec("ALTER TABLE jobs ALTER COLUMN determinism SET NOT NULL").Error,
		"SET NOT NULL on determinism must succeed after the heal")
}

// TestMigrateMySQLBackfillsLegacyNullTimeoutAndDeterminism is the MySQL analogue.
// MySQL applies NOT NULL to both columns during Migrate (AutoMigrate's column
// rebuild + migrateIntWidthRightsizing's `MODIFY ... NOT NULL DEFAULT 0`); without
// the heal those MODIFYs abort against the legacy NULL row with Error 1265. The
// test asserts Migrate converges, both columns are backfilled to 0, and both are
// NOT NULL again.
func TestMigrateMySQLBackfillsLegacyNullTimeoutAndDeterminism(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	databaseName := uniqueSchemaAssertionsName("null_backfill")
	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open mysql admin db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE DATABASE "+quoteMySQLIdent(databaseName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP DATABASE IF EXISTS "+quoteMySQLIdent(databaseName)).Error)
	})

	db, err := gorm.Open(mysql.Open(mysqlDSNWithDatabase(t, dsn, databaseName)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open isolated mysql database")
	closeDBOnCleanup(t, db)

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx), "initial migrate")

	require.NoError(t, db.Exec("ALTER TABLE jobs MODIFY COLUMN timeout bigint NULL").Error)
	require.NoError(t, db.Exec("ALTER TABLE jobs MODIFY COLUMN determinism int NULL").Error)
	require.NoError(t, db.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, updated_at)
		VALUES (UUID_TO_BIN('11111111-1111-1111-1111-111111111111'), 'legacy', 'default', 'pending', 0, 0, 3, NULL, NULL, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
	`).Error)

	require.NoError(t, s.Migrate(ctx), "re-migrate over a legacy NULL baseline must succeed")

	var timeout, determinism int64
	require.NoError(t, db.Raw(
		"SELECT timeout FROM jobs WHERE id = UUID_TO_BIN('11111111-1111-1111-1111-111111111111')",
	).Scan(&timeout).Error)
	require.Equal(t, int64(0), timeout, "legacy NULL timeout backfilled to 0")
	require.NoError(t, db.Raw(
		"SELECT determinism FROM jobs WHERE id = UUID_TO_BIN('11111111-1111-1111-1111-111111111111')",
	).Scan(&determinism).Error)
	require.Equal(t, int64(0), determinism, "legacy NULL determinism backfilled to 0")

	for _, col := range []string{"timeout", "determinism"} {
		var isNullable string
		require.NoError(t, db.Raw(
			"SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name='jobs' AND column_name=?",
			col,
		).Scan(&isNullable).Error)
		require.Equalf(t, "NO", isNullable, "%s NOT NULL reasserted after the heal", col)
	}
}

// TestMigrateSQLiteBackfillsLegacyNullDispatchToken reproduces an upgrade from
// the first dispatch-fencing release, which shipped jobs.dispatch_token nullable.
// AutoMigrate adds the NOT NULL constraint by rebuilding the column on SQLite, and
// the rebuild aborts if any surviving row holds NULL. backfillDispatcherNulls must
// coalesce those rows to ” before AutoMigrate runs.
func TestMigrateSQLiteBackfillsLegacyNullDispatchToken(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))

	// Recreate the prior release's shape: dispatch_token exists but is nullable.
	require.NoError(t, db.Exec("ALTER TABLE jobs RENAME COLUMN dispatch_token TO dispatch_token_old").Error)
	require.NoError(t, db.Exec("ALTER TABLE jobs ADD COLUMN dispatch_token text").Error)
	require.NoError(t, db.Exec("ALTER TABLE jobs DROP COLUMN dispatch_token_old").Error)
	job := newTestJob("default", "task")
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, db.Exec("UPDATE jobs SET dispatch_token = NULL WHERE id = ?", job.ID).Error)

	require.NoError(t, s.Migrate(ctx), "re-migrate over a NULL dispatch_token must succeed")

	var notnull int
	require.NoError(t, db.Raw("SELECT [notnull] FROM pragma_table_info('jobs') WHERE name='dispatch_token'").Row().Scan(&notnull))
	require.Equal(t, 1, notnull, "dispatch_token is NOT NULL after the upgrade")
	var nulls int
	require.NoError(t, db.Raw("SELECT count(*) FROM jobs WHERE dispatch_token IS NULL").Scan(&nulls).Error)
	require.Zero(t, nulls, "legacy NULLs are backfilled to ''")
}

// TestMigratePostgresBootsWhenTimeoutDeterminismColumnsMissing is the D1
// regression: a v1.0.0–v1.2.1 baseline has NO jobs.timeout / jobs.determinism
// column (both added in #16). backfillDispatcherNulls must build its probe/UPDATE
// from only the columns that exist, so it does not reference the absent columns
// (PG 42703 undefined_column) and crash-loop Migrate at boot. AutoMigrate then
// re-adds them NOT NULL DEFAULT 0. Without the fix the re-Migrate errors on the
// missing-column probe.
func TestMigratePostgresBootsWhenTimeoutDeterminismColumnsMissing(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	schemaName := uniqueSchemaAssertionsName("missing_cols")
	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "open postgres admin db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+quotePostgresIdent(schemaName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP SCHEMA IF EXISTS "+quotePostgresIdent(schemaName)+" CASCADE").Error)
	})

	db, err := gorm.Open(postgres.Open(postgresDSNWithSearchPath(t, dsn, schemaName)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "open isolated postgres schema")
	closeDBOnCleanup(t, db)

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx), "initial migrate")

	// Recreate the pre-#16 baseline: the columns don't exist at all.
	require.NoError(t, db.Exec("ALTER TABLE jobs DROP COLUMN timeout CASCADE").Error)
	require.NoError(t, db.Exec("ALTER TABLE jobs DROP COLUMN determinism CASCADE").Error)
	require.False(t, s.DB().Migrator().HasColumn(&core.Job{}, "timeout"))

	// Re-migrate: the pre-AutoMigrate heal must NOT reference the absent columns.
	require.NoError(t, s.Migrate(ctx), "re-migrate over a baseline missing timeout/determinism must succeed")

	require.True(t, s.DB().Migrator().HasColumn(&core.Job{}, "timeout"), "AutoMigrate re-adds timeout")
	require.True(t, s.DB().Migrator().HasColumn(&core.Job{}, "determinism"), "AutoMigrate re-adds determinism")
	for _, col := range []string{"timeout", "determinism"} {
		var isNullable string
		require.NoError(t, db.Raw(
			"SELECT is_nullable FROM information_schema.columns WHERE table_schema = current_schema() AND table_name='jobs' AND column_name=?", col,
		).Scan(&isNullable).Error)
		require.Equalf(t, "NO", isNullable, "%s re-added NOT NULL", col)
	}
}

// TestMigrateMySQLBootsWhenTimeoutDeterminismColumnsMissing is the D1 regression
// for MySQL (the absent-column reference errors 1054 there).
func TestMigrateMySQLBootsWhenTimeoutDeterminismColumnsMissing(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	databaseName := uniqueSchemaAssertionsName("missing_cols")
	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "open mysql admin db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE DATABASE "+quoteMySQLIdent(databaseName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP DATABASE IF EXISTS "+quoteMySQLIdent(databaseName)).Error)
	})

	db, err := gorm.Open(mysql.Open(mysqlDSNWithDatabase(t, dsn, databaseName)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "open isolated mysql database")
	closeDBOnCleanup(t, db)

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx), "initial migrate")

	require.NoError(t, db.Exec("ALTER TABLE jobs DROP COLUMN timeout").Error)
	require.NoError(t, db.Exec("ALTER TABLE jobs DROP COLUMN determinism").Error)
	require.False(t, s.DB().Migrator().HasColumn(&core.Job{}, "timeout"))

	require.NoError(t, s.Migrate(ctx), "re-migrate over a baseline missing timeout/determinism must succeed")

	require.True(t, s.DB().Migrator().HasColumn(&core.Job{}, "timeout"), "AutoMigrate re-adds timeout")
	require.True(t, s.DB().Migrator().HasColumn(&core.Job{}, "determinism"), "AutoMigrate re-adds determinism")
	for _, col := range []string{"timeout", "determinism"} {
		var isNullable string
		require.NoError(t, db.Raw(
			"SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name='jobs' AND column_name=?", col,
		).Scan(&isNullable).Error)
		require.Equalf(t, "NO", isNullable, "%s re-added NOT NULL", col)
	}
}
