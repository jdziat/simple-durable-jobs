package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// openTestDB opens a database for tests.
// When TEST_DATABASE_URL is set it connects to PostgreSQL; otherwise it
// opens a fresh in-memory SQLite instance.
// PostgreSQL connections are pool-limited and closed on test cleanup to
// avoid exceeding max_connections.
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres test db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)

		// Clean before AND after to ensure test isolation.
		cleanupPostgresDB(t, db)
		t.Cleanup(func() {
			cleanupPostgresDB(t, db)
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

// cleanupPostgresDB deletes all rows from tables after each test
// so tests are isolated without requiring a fresh database per test.
func cleanupPostgresDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	// Order matters: respect foreign key constraints.
	tables := []string{"checkpoints", "fan_outs", "queue_states", "jobs"}
	for _, tbl := range tables {
		db.Exec("DELETE FROM " + tbl)
	}
}
