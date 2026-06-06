package jobs_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs"
)

var dbCounter atomic.Int64

// openIntegrationDB opens a database for integration tests.
// When TEST_DATABASE_URL is set it connects to PostgreSQL; otherwise
// it creates a unique file-based SQLite database (removed on cleanup).
// PostgreSQL connections are pool-limited and closed on test cleanup to
// avoid exceeding max_connections.
func openIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open mysql integration db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)

		cleanupExternalIntegrationDB(t, db)
		t.Cleanup(func() {
			cleanupExternalIntegrationDB(t, db)
			_ = sqlDB.Close()
		})
		return db
	}

	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres integration db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)

		cleanupExternalIntegrationDB(t, db)
		t.Cleanup(func() {
			cleanupExternalIntegrationDB(t, db)
			_ = sqlDB.Close()
		})
		return db
	}

	n := dbCounter.Add(1)
	dbPath := fmt.Sprintf("/tmp/jobs_test_%d_%d.db", os.Getpid(), n)
	t.Cleanup(func() {
		// WAL mode leaves -wal/-shm sidecar files next to the db.
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(dbPath + suffix)
		}
	})

	// Open with the concurrency-safe DSN the library documents (gorm.go) and
	// stress_test.go uses. A bare DSN runs in rollback-journal mode with an
	// unbounded connection pool whose per-connection busy_timeout is unset, so
	// concurrent workers race the single SQLite writer lock and completion
	// writes transiently fail with SQLITE_BUSY ("database is locked") or
	// SQLITE_READONLY ("attempt to write a readonly database"), exhausting the
	// worker retry budget and dropping a job from the completed set. WAL +
	// busy_timeout (applied to every pooled connection via the DSN, unlike a
	// one-shot PRAGMA) + txlock=immediate serialize writers cleanly.
	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN(dbPath)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open sqlite integration db")
	return db
}

// openIntegrationStorage opens a DB, creates a GormStorage, and migrates.
func openIntegrationStorage(t *testing.T) jobs.Storage {
	t.Helper()
	db := openIntegrationDB(t)
	store := jobs.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()), "migrate schema")
	return store
}

// openIntegrationQueue opens a DB, creates storage + queue.
func openIntegrationQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	store := openIntegrationStorage(t)
	queue := jobs.New(store)
	return queue, store
}

// cleanupExternalIntegrationDB deletes all rows so tests are isolated.
func cleanupExternalIntegrationDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	// scheduled_fires and leases hold per-test state (scheduler anchor, recovery
	// lease); clean them too so integration tests are isolated on a persistent
	// external DB. schema_migrations is intentionally left (migration ledger).
	tables := []string{"checkpoints", "fan_outs", "queue_states", "jobs", "scheduled_fires", "leases"}
	for _, tbl := range tables {
		db.Exec("DELETE FROM " + tbl)
	}
}
