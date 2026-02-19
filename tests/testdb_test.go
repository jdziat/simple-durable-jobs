package jobs_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
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
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres integration db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)

		// Clean before AND after to ensure test isolation.
		cleanupPostgresIntegrationDB(t, db)
		t.Cleanup(func() {
			cleanupPostgresIntegrationDB(t, db)
			_ = sqlDB.Close()
		})
		return db
	}

	n := dbCounter.Add(1)
	dbPath := fmt.Sprintf("/tmp/jobs_test_%d_%d.db", os.Getpid(), n)
	t.Cleanup(func() { _ = os.Remove(dbPath) })

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
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

// cleanupPostgresIntegrationDB deletes all rows so tests are isolated.
func cleanupPostgresIntegrationDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	tables := []string{"checkpoints", "fan_outs", "queue_states", "jobs"}
	for _, tbl := range tables {
		db.Exec("DELETE FROM " + tbl)
	}
}
