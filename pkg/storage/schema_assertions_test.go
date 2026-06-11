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
	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
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
		"idx_jobs_dequeue_eligible":   "WHERE",
		"idx_jobs_retention_terminal": "WHERE",
		"idx_jobs_stale_lock":         "COALESCE",
	}
	for indexName, mustContain := range defs {
		indexDef := postgresIndexDef(t, db, indexName)
		require.Contains(t, strings.ToUpper(indexDef), strings.ToUpper(mustContain), "%s definition:\n%s", indexName, indexDef)
	}
	require.Contains(t, postgresIndexDef(t, db, "idx_jobs_stale_lock"), "WHERE", "idx_jobs_stale_lock must stay partial")
	require.Contains(t, strings.ToUpper(postgresIndexDef(t, db, "idx_jobs_dequeue_eligible")), "COALESCE")

	for _, indexName := range []string{
		"idx_jobs_priority",
		"idx_jobs_queue",
		"idx_jobs_locked_until",
		"idx_jobs_dequeue",
		"idx_jobs_unique_key",
		"idx_jobs_dequeue_order",
	} {
		require.False(t, postgresIndexExists(t, db, indexName), "%s must not exist after migration", indexName)
	}
	requirePostgresCascadeFKs(t, db)
	requirePostgresDispatcherColumnsNotNull(t, db)
	requirePostgresFanOutThresholdDouble(t, db)

	// Regression (P5): a bare AutoMigrate must NOT drift the NOT NULL dispatcher
	// columns or the double-precision threshold back (the core.Job/FanOut tags
	// carry them). A drift would make every startup re-issue a rewriting ALTER.
	require.NoError(t, db.AutoMigrate(&core.Job{}, &core.FanOut{}))
	requirePostgresDispatcherColumnsNotNull(t, db)
	requirePostgresFanOutThresholdDouble(t, db)
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

	var dequeueEligibleCount int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = 'dq_eligible_at'
		  AND GENERATION_EXPRESSION <> ''
	`).Scan(&dequeueEligibleCount).Error)
	require.Equal(t, 1, dequeueEligibleCount, "dq_eligible_at generated column must exist")

	for _, indexName := range []string{
		"idx_jobs_priority",
		"idx_jobs_queue",
		"idx_jobs_locked_until",
		"idx_jobs_dequeue",
		"idx_jobs_dequeue_order",
	} {
		require.False(t, mysqlIndexExists(t, db, indexName), "%s must not exist after migration", indexName)
	}
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_unique_key"), "idx_jobs_unique_key must remain on mysql")
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_dequeue_eligible"), "idx_jobs_dequeue_eligible must exist on mysql")
	requireMySQLCascadeFKs(t, db)
	requireMySQLUniqueKeyCollations(t, db)
	requireMySQLDispatcherColumnsNotNull(t, db)

	// Regression (P5): a bare AutoMigrate must NOT drift the NOT NULL dispatcher
	// columns back to nullable. Before the core.Job `not null` tags, AutoMigrate
	// stripped NOT NULL on every run and the migration restored it — two
	// table-copy MODIFYs per startup. With the tags AutoMigrate leaves them be.
	require.NoError(t, db.AutoMigrate(&core.Job{}))
	requireMySQLDispatcherColumnsNotNull(t, db)
	requireMySQLUniqueKeyCollations(t, db)
}

func TestMySQLMigrateRequiresUTCSession(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	databaseName := uniqueSchemaAssertionsName("utc_check")

	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open mysql test db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE DATABASE "+quoteMySQLIdent(databaseName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP DATABASE IF EXISTS "+quoteMySQLIdent(databaseName)).Error)
	})

	utcDB, err := gorm.Open(mysql.Open(mysqlDSNWithTimeZone(t, mysqlDSNWithDatabase(t, dsn, databaseName), "'+00:00'")), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open utc mysql session")
	closeDBOnCleanup(t, utcDB)
	require.NoError(t, NewGormStorage(utcDB).Migrate(context.Background()))

	nonUTCDB, err := gorm.Open(mysql.Open(mysqlDSNWithTimeZone(t, mysqlDSNWithDatabase(t, dsn, databaseName), "'+05:30'")), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open non-utc mysql session")
	closeDBOnCleanup(t, nonUTCDB)

	err = NewGormStorage(nonUTCDB).Migrate(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "MySQL session clock is not UTC")
	require.Contains(t, err.Error(), "time_zone='+00:00'")
}

func requirePostgresCascadeFKs(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM pg_constraint
		WHERE contype = 'f'
		  AND conname IN ('fk_checkpoints_job', 'fk_signals_job', 'fk_fanouts_parent')
		  AND confdeltype = 'c'
		  AND conrelid IN ('checkpoints'::regclass, 'signals'::regclass, 'fan_outs'::regclass)
	`).Scan(&count).Error)
	require.Equal(t, 3, count, "postgres cascade FKs must exist")
}

func requireMySQLCascadeFKs(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.REFERENTIAL_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE()
		  AND CONSTRAINT_NAME IN ('fk_checkpoints_job', 'fk_signals_job', 'fk_fanouts_parent')
		  AND DELETE_RULE = 'CASCADE'
	`).Scan(&count).Error)
	require.Equal(t, 3, count, "mysql cascade FKs must exist")
}

func requirePostgresDispatcherColumnsNotNull(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE table_schema = CURRENT_SCHEMA()
		  AND table_name = 'jobs'
		  AND column_name IN ('status', 'queue', 'priority', 'attempt', 'max_retries')
		  AND is_nullable = 'NO'
	`).Scan(&count).Error)
	require.Equal(t, 5, count, "postgres dispatcher columns must be NOT NULL")
}

func requirePostgresFanOutThresholdDouble(t *testing.T, db *gorm.DB) {
	t.Helper()
	var dataType string
	require.NoError(t, db.Raw(`
		SELECT data_type
		FROM information_schema.COLUMNS
		WHERE table_schema = CURRENT_SCHEMA()
		  AND table_name = 'fan_outs'
		  AND column_name = 'threshold'
	`).Scan(&dataType).Error)
	require.Equal(t, "double precision", dataType, "postgres fan_outs.threshold must be double precision")
}

func requireMySQLUniqueKeyCollations(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME IN ('unique_key', 'active_unique_key')
		  AND COLLATION_NAME = 'utf8mb4_0900_as_cs'
	`).Scan(&count).Error)
	require.Equal(t, 2, count, "mysql unique key columns must use utf8mb4_0900_as_cs")
}

func requireMySQLDispatcherColumnsNotNull(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME IN ('status', 'queue', 'priority', 'attempt', 'max_retries')
		  AND IS_NULLABLE = 'NO'
	`).Scan(&count).Error)
	require.Equal(t, 5, count, "mysql dispatcher columns must be NOT NULL")
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

func mysqlDSNWithTimeZone(t *testing.T, dsn, timeZone string) string {
	t.Helper()

	cfg, err := mysqlcfg.ParseDSN(dsn)
	require.NoError(t, err, "parse mysql dsn")
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["time_zone"] = timeZone
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
