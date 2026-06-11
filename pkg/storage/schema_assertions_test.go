package storage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	mysqlcfg "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestPostgresSchemaAssertions(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	schemaName := uniqueSchemaAssertionsName("schema_assertions")

	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres test db")
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
	require.NoError(t, s.Migrate(ctx))

	defs := map[string]string{
		"idx_jobs_dead_lettered_at":   "dead_lettered_at IS NOT NULL",
		"idx_jobs_active_unique":      "WHERE",
		"idx_jobs_dequeue_order":      "WHERE",
		"idx_jobs_retention_terminal": "WHERE",
		"idx_jobs_stale_lock":         "COALESCE",
	}
	for indexName, mustContain := range defs {
		indexDef := postgresIndexDef(t, db, indexName)
		require.Contains(t, strings.ToUpper(indexDef), strings.ToUpper(mustContain), "%s definition:\n%s", indexName, indexDef)
	}
	require.Contains(t, postgresIndexDef(t, db, "idx_jobs_stale_lock"), "WHERE", "idx_jobs_stale_lock must stay partial")

	for _, indexName := range []string{
		"idx_jobs_priority",
		"idx_jobs_queue",
		"idx_jobs_locked_until",
		"idx_jobs_dequeue",
		"idx_jobs_unique_key",
	} {
		require.False(t, postgresIndexExists(t, db, indexName), "%s must not exist after migration", indexName)
	}
}

func TestMySQLSchemaAssertions(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	databaseName := uniqueSchemaAssertionsName("schema_assertions")

	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open mysql test db")
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
	require.NoError(t, s.Migrate(ctx))
	requireMySQLDeadLetteredAtPrecision(t, db, 6)

	require.NoError(t, s.Migrate(ctx), "second Migrate must not let AutoMigrate revert dead_lettered_at precision")
	requireMySQLDeadLetteredAtPrecision(t, db, 6)

	var activeUniqueKeyCount int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = 'active_unique_key'
		  AND GENERATION_EXPRESSION <> ''
	`).Scan(&activeUniqueKeyCount).Error)
	require.Equal(t, 1, activeUniqueKeyCount, "active_unique_key generated column must exist")

	for _, indexName := range []string{
		"idx_jobs_priority",
		"idx_jobs_queue",
		"idx_jobs_locked_until",
		"idx_jobs_dequeue",
	} {
		require.False(t, mysqlIndexExists(t, db, indexName), "%s must not exist after migration", indexName)
	}
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_unique_key"), "idx_jobs_unique_key must remain on mysql")
}

func requireMySQLDeadLetteredAtPrecision(t *testing.T, db *gorm.DB, want int) {
	t.Helper()
	var precision int
	require.NoError(t, db.Raw(`
		SELECT DATETIME_PRECISION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = 'dead_lettered_at'
	`).Scan(&precision).Error)
	require.Equal(t, want, precision, "dead_lettered_at must be datetime(%d)", want)
}

func postgresIndexDef(t *testing.T, db *gorm.DB, indexName string) string {
	t.Helper()
	var indexDef string
	require.NoError(t, db.Raw(`
		SELECT pg_get_indexdef(indexrelid)
		FROM pg_index
		WHERE indexrelid = ?::regclass
	`, indexName).Scan(&indexDef).Error)
	require.NotEmpty(t, indexDef, "%s must exist", indexName)
	return indexDef
}

func postgresIndexExists(t *testing.T, db *gorm.DB, indexName string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.Raw("SELECT to_regclass(?) IS NOT NULL", indexName).Scan(&exists).Error)
	return exists
}

func mysqlIndexExists(t *testing.T, db *gorm.DB, indexName string) bool {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND INDEX_NAME = ?
	`, indexName).Scan(&count).Error)
	return count > 0
}

func postgresDSNWithSearchPath(t *testing.T, dsn, schemaName string) string {
	t.Helper()

	u, err := url.Parse(dsn)
	require.NoError(t, err, "parse postgres dsn")
	q := u.Query()
	q.Set("search_path", schemaName)
	u.RawQuery = q.Encode()
	return u.String()
}

func mysqlDSNWithDatabase(t *testing.T, dsn, databaseName string) string {
	t.Helper()

	cfg, err := mysqlcfg.ParseDSN(dsn)
	require.NoError(t, err, "parse mysql dsn")
	cfg.DBName = databaseName
	return cfg.FormatDSN()
}

func uniqueSchemaAssertionsName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
}

func quotePostgresIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteMySQLIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
