package storage

import (
	"context"
	"os"
	"testing"
	"time"

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

		// Register teardown BEFORE the setup gate: requireCleanStart Fatalf's by
		// design on a dirty DB, and a Fatalf runs no code after it in this
		// function — so registering Close first ensures the pooled connections are
		// still closed (not leaked) when the gate trips.
		t.Cleanup(func() {
			cleanupExternalDB(t, db)
			_ = sqlDB.Close()
		})
		requireCleanStart(t, db)
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

		// Register teardown BEFORE the setup gate: requireCleanStart Fatalf's by
		// design on a dirty DB, and a Fatalf runs no code after it in this
		// function — so registering Close first ensures the pooled connections are
		// still closed (not leaked) when the gate trips.
		t.Cleanup(func() {
			cleanupExternalDB(t, db)
			_ = sqlDB.Close()
		})
		requireCleanStart(t, db)
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

	// Register teardown BEFORE the setup gate (see openTestDB): a gate Fatalf must
	// still close the pooled connections rather than leak them.
	t.Cleanup(func() {
		cleanupExternalDB(t, db)
		_ = sqlDB.Close()
	})
	requireCleanStart(t, db)

	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()), "migrate schema")
	require.Equal(t, dialectPostgres, s.dialect(), "postgres-specific test must run on postgres")
	return s
}

// externalTestTables is every table that holds per-test state on a persistent
// external (Postgres/MySQL) DB, in child-before-parent order (no FKs today, but
// keep the intent). schema_migrations is intentionally EXCLUDED — it is the
// migration ledger and is shared across every test. signals is INCLUDED because
// unconsumed rows leak across tests otherwise (PeekSignal leaves ("j1","other")
// pending, which TestGetPendingSignalName then finds); scheduled_fires and
// leases hold the scheduler anchor and recovery lease.
var externalTestTables = []string{"signals", "checkpoints", "fan_outs", "queue_states", "jobs", "unique_locks", "scheduled_fires", "leases", "concurrency_slots", "rate_limit_windows"}

// deleteAllWithRetry runs `DELETE FROM tbl`, retrying transient serialization /
// deadlock failures before giving up. The external-DB test pool is capped at 2
// connections, so under -race a cleanup DELETE can momentarily deadlock or lose
// a serialization race with a just-settling prior operation; those clear on the
// next attempt. A NON-transient error is returned immediately (retrying a syntax
// or connection-dead error only wastes time).
func deleteAllWithRetry(db *gorm.DB, tbl string) error {
	// A not-yet-migrated table has nothing to clean. Setup cleanup runs BEFORE
	// Migrate, so on a fresh CI database (fresh container, first test) the tables
	// do not exist yet; skip them rather than fail on "undefined table". HasTable
	// is dialect-agnostic, avoiding brittle per-driver error-code matching.
	if !db.Migrator().HasTable(tbl) {
		return nil
	}
	const maxAttempts = 5
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = db.Exec("DELETE FROM " + tbl).Error; err == nil {
			return nil
		}
		if !isSerializationFailure(err) {
			return err
		}
		if attempt < maxAttempts-1 { // no backoff after the final attempt
			time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
		}
	}
	return err
}

// requireCleanStart is the authoritative test-isolation gate, called at SETUP on
// the shared external DB. It clears every per-test table and then ASSERTS the
// tables are actually empty, failing loudly if not.
//
// Why the assertion: Postgres/MySQL tests share one jobs_test database and are
// isolated only by this cleanup (SQLite gets a fresh :memory: DB and never gets
// here). If a prior test's rows survive — a cleanup DELETE that transiently
// failed and was lost, or a genuine leak — the next test silently dequeues those
// extra rows and fails with a mysterious wrong-result "flake" several tests
// removed from the real cause (observed in CI on the dequeue-batch tests). By
// failing at setup we pin the blame to the exact boundary and make shared-DB
// contamination impossible to pass silently.
func requireCleanStart(t testing.TB, db *gorm.DB) {
	t.Helper()
	for _, tbl := range externalTestTables {
		if err := deleteAllWithRetry(db, tbl); err != nil {
			t.Fatalf("requireCleanStart: DELETE FROM %s failed: %v", tbl, err)
		}
	}
	requireTablesEmpty(t, db)
}

// requireTablesEmpty asserts every per-test table is empty, failing loudly (and
// naming the offending table + row count) if not. Factored out of
// requireCleanStart so the isolation gate itself is unit-testable — see
// TestRequireTablesEmpty_DetectsContamination.
func requireTablesEmpty(t testing.TB, db *gorm.DB) {
	t.Helper()
	for _, tbl := range externalTestTables {
		// Not-yet-migrated tables (fresh DB, pre-Migrate) hold zero rows by
		// definition; skip rather than error on "undefined table".
		if !db.Migrator().HasTable(tbl) {
			continue
		}
		var n int64
		if err := db.Raw("SELECT COUNT(*) FROM " + tbl).Scan(&n).Error; err != nil {
			t.Fatalf("requireTablesEmpty: counting %s: %v", tbl, err)
		}
		if n != 0 {
			t.Fatalf("requireTablesEmpty: %s still has %d row(s) — shared-DB "+
				"test isolation is broken (a prior test leaked rows or its cleanup "+
				"DELETE was lost). On the shared jobs_test DB this silently corrupts "+
				"dequeue ordering in the next test.", tbl, n)
		}
	}
}

// cleanupExternalDB best-effort-clears every per-test table at TEARDOWN. It
// retries transient failures but, unlike requireCleanStart, only LOGS a
// persistent error instead of failing the test: some chaos/connection-loss
// tests deliberately leave the connection unhealthy on their way out, and
// failing here would turn their successful run into a spurious failure. It still
// attempts the clear even when the test failed (so the next test starts clean);
// the next test's requireCleanStart is the authoritative gate that fails loudly
// if this teardown was in fact lost.
func cleanupExternalDB(t testing.TB, db *gorm.DB) {
	t.Helper()
	for _, tbl := range externalTestTables {
		if err := deleteAllWithRetry(db, tbl); err != nil {
			t.Logf("cleanupExternalDB: DELETE FROM %s failed (teardown best-effort; "+
				"the next test's requireCleanStart will catch any residue): %v", tbl, err)
		}
	}
}
