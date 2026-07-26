package storage

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

// Every collation-bearing migration writes `COLLATE utf8mb4_0900_as_cs` with no
// accompanying CHARACTER SET. On a database still defaulting to utf8mb3 — the
// MySQL 5.x-era default, and what an old CREATE DATABASE leaves behind — the
// column inherits utf8mb3 while being handed a utf8mb4 collation, and the server
// rejects it with ER_COLLATION_CHARSET_MISMATCH partway through the ledger:
// a boot crashloop on a legitimate database, with some DDL applied and some not.
//
// Those migrations are already stamped in production ledgers and must not be
// edited, so the repair is a preflight that refuses BEFORE any DDL runs.
//
// FALSE-GREEN TRAP: asserting only that Migrate returns an error passes even if
// the preflight never runs and the failure comes from the collation error
// halfway through — which is the bug. The discriminating assertion is that ZERO
// tables exist afterwards.
func TestMigrate_RefusesUTF8MB3DatabaseBeforeAnyDDL(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set; this defect is MySQL-only")
	}

	admin := openMySQLForCharsetTest(t, dsn)
	const legacyDB = "sdj_charset_preflight_test"

	require.NoError(t, admin.Exec("DROP DATABASE IF EXISTS "+legacyDB).Error)
	require.NoError(t, admin.Exec(
		"CREATE DATABASE "+legacyDB+" CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci").Error)
	t.Cleanup(func() { _ = admin.Exec("DROP DATABASE IF EXISTS " + legacyDB).Error })

	legacy := openMySQLDatabase(t, dsn, legacyDB)
	store := NewGormStorage(legacy)

	err := store.Migrate(context.Background())
	require.Error(t, err, "a utf8mb3 database must be refused, not migrated into a half-applied state")
	assert.Contains(t, err.Error(), "utf8mb3", "the message must name the character set actually found")
	assert.Contains(t, err.Error(), "ALTER DATABASE", "the message must carry the remedy, not just the diagnosis")

	var tables int64
	require.NoError(t, legacy.Raw(
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", legacyDB).Scan(&tables).Error)
	assert.Zero(t, tables,
		"the preflight must run before ANY DDL: a partially-migrated legacy database is the failure being prevented")

	// The remedy printed in the error must actually work — an error message that
	// tells an operator to run something that does not fix it is worse than none.
	require.NoError(t, admin.Exec(
		"ALTER DATABASE "+legacyDB+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci").Error)

	require.NoError(t, store.Migrate(context.Background()),
		"after the documented ALTER DATABASE the same database must migrate cleanly")

	require.NoError(t, legacy.Raw(
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", legacyDB).Scan(&tables).Error)
	assert.Positive(t, tables, "the schema must exist after the remedy")
}

func openMySQLForCharsetTest(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: glogger.Default.LogMode(glogger.Silent)})
	require.NoError(t, err)
	return db
}

// openMySQLDatabase re-points a DSN at a different schema name.
func openMySQLDatabase(t *testing.T, dsn, dbName string) *gorm.DB {
	t.Helper()
	slash := -1
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			slash = i
			break
		}
	}
	require.Positive(t, slash, "DSN must contain a database path separator")
	rest := ""
	for i := slash + 1; i < len(dsn); i++ {
		if dsn[i] == '?' {
			rest = dsn[i:]
			break
		}
	}
	db, err := gorm.Open(mysql.Open(dsn[:slash+1]+dbName+rest),
		&gorm.Config{Logger: glogger.Default.LogMode(glogger.Silent)})
	require.NoError(t, err)
	return db
}
