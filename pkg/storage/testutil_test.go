package storage

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func testUUID(name string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String())
}

// openTestDB opens a database for tests.
// When TEST_DATABASE_URL is set it connects to PostgreSQL; otherwise it
// opens a fresh in-memory SQLite instance.
// PostgreSQL connections are pool-limited and closed on test cleanup to
// avoid exceeding max_connections.
func openTestDB(t testing.TB) *gorm.DB {
	t.Helper()

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open mysql test db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)

		cleanupExternalDB(t, db)
		t.Cleanup(func() {
			cleanupExternalDB(t, db)
			_ = sqlDB.Close()
		})
		return db
	}

	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres test db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)

		cleanupExternalDB(t, db)
		t.Cleanup(func() {
			cleanupExternalDB(t, db)
			_ = sqlDB.Close()
		})
		return db
	}

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open in-memory sqlite")
	return db
}

// newPostgresTestStorage opens a Postgres-backed, migrated store DIRECTLY from
// TEST_DATABASE_URL, ignoring TEST_MYSQL_URL. PostgreSQL-specific tests must use
// this instead of newTestStorage: openTestDB prefers TEST_MYSQL_URL, so with
// BOTH live DSNs set (the documented "both DBs available" dev setup) a PG-only
// test would otherwise silently run against MySQL and mis-exercise or skip the
// Postgres branch (codex F-005). Skips when TEST_DATABASE_URL is unset, and
// asserts the resolved dialect is Postgres so future helper drift fails loudly
// at setup rather than as a misleading behavior failure.
func newPostgresTestStorage(t *testing.T) *GormStorage {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping PostgreSQL-specific test")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres test db")

	sqlDB, err := db.DB()
	require.NoError(t, err, "get underlying sql.DB")
	sqlDB.SetMaxOpenConns(2)
	sqlDB.SetMaxIdleConns(1)

	cleanupExternalDB(t, db)
	t.Cleanup(func() {
		cleanupExternalDB(t, db)
		_ = sqlDB.Close()
	})

	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()), "migrate schema")
	require.Equal(t, dialectPostgres, s.dialect(), "postgres-specific test must run on postgres")
	return s
}

// cleanupExternalDB deletes all rows from tables after each test
// so tests are isolated without requiring a fresh database per test.
func cleanupExternalDB(t testing.TB, db *gorm.DB) {
	t.Helper()
	// Order matters: respect foreign key constraints. scheduled_fires and leases
	// hold per-test state too (the scheduler anchor and recovery lease), so they
	// must be cleaned for tests to be isolated on a persistent external DB.
	// schema_migrations is intentionally NOT cleaned — it is the migration ledger.
	// signals included: unconsumed rows leak across tests otherwise (PeekSignal
	// leaves ("j1","other") pending, which TestGetPendingSignalName then finds).
	tables := []string{"signals", "checkpoints", "fan_outs", "queue_states", "jobs", "unique_locks", "scheduled_fires", "leases", "concurrency_slots", "rate_limit_windows"}
	for _, tbl := range tables {
		db.Exec("DELETE FROM " + tbl)
	}
}
