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
	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// schemaCheckID derives a deterministic, valid UUID from a readable name so the
// raw INSERTs in the schema-assertion helpers satisfy the binary uuid id columns
// (v3) while preserving parent/child references (same name -> same UUID). It is
// returned as core.UUID so Exec binds it through Value() and encodes correctly on
// every dialect (postgres uuid / mysql binary(16) / sqlite blob).
func schemaCheckID(name string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("sdj-schema-check/"+name)).String())
}

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
	beforeDeadLetterType := requirePostgresDeadLetteredAtType(t, db)
	require.Equal(t, postgresColumnType{DataType: "timestamp with time zone", DateTimePrecision: 6}, beforeDeadLetterType)

	require.NoError(t, s.Migrate(ctx), "second Migrate must not alter jobs.dead_lettered_at")
	afterDeadLetterType := requirePostgresDeadLetteredAtType(t, db)
	require.Equal(t, beforeDeadLetterType, afterDeadLetterType, "second Migrate must leave dead_lettered_at type byte-identical")

	defs := map[string]string{
		"idx_jobs_dead_lettered_at":   "dead_lettered_at IS NOT NULL",
		"idx_jobs_active_unique":      "WHERE",
		"idx_jobs_dequeue_eligible":   "WHERE",
		"idx_jobs_retention_terminal": "WHERE",
		"idx_signals_consumed_at":     "consumed_at IS NOT NULL",
		"idx_jobs_stale_lock":         "COALESCE",
	}
	for indexName, mustContain := range defs {
		indexDef := postgresIndexDef(t, db, indexName)
		require.Contains(t, strings.ToUpper(indexDef), strings.ToUpper(mustContain), "%s definition:\n%s", indexName, indexDef)
	}
	require.Contains(t, strings.ToLower(postgresIndexDef(t, db, "idx_jobs_retention_terminal")), "id", "idx_jobs_retention_terminal must include id")
	require.Contains(t, postgresIndexDef(t, db, "idx_jobs_stale_lock"), "WHERE", "idx_jobs_stale_lock must stay partial")
	require.Contains(t, strings.ToUpper(postgresIndexDef(t, db, "idx_jobs_dequeue_eligible")), "COALESCE")
	parentNonterminalDef := strings.ToLower(postgresIndexDef(t, db, "idx_jobs_parent_nonterminal"))
	require.Contains(t, parentNonterminalDef, "parent_job_id", "idx_jobs_parent_nonterminal definition:\n%s", parentNonterminalDef)
	require.Contains(t, parentNonterminalDef, "where", "idx_jobs_parent_nonterminal must stay partial:\n%s", parentNonterminalDef)
	require.True(t,
		strings.Contains(parentNonterminalDef, "not in") || strings.Contains(parentNonterminalDef, "<> all"),
		"idx_jobs_parent_nonterminal must filter terminal statuses:\n%s", parentNonterminalDef,
	)
	rootNonterminalDef := strings.ToLower(postgresIndexDef(t, db, "idx_jobs_root_nonterminal"))
	require.Contains(t, rootNonterminalDef, "root_job_id", "idx_jobs_root_nonterminal definition:\n%s", rootNonterminalDef)
	require.Contains(t, rootNonterminalDef, "where", "idx_jobs_root_nonterminal must stay partial:\n%s", rootNonterminalDef)
	require.True(t,
		strings.Contains(rootNonterminalDef, "not in") || strings.Contains(rootNonterminalDef, "<> all"),
		"idx_jobs_root_nonterminal must filter terminal statuses:\n%s", rootNonterminalDef,
	)
	metadataIndexDef := postgresIndexDef(t, db, "idx_jobs_metadata_gin")
	require.Contains(t, strings.ToLower(metadataIndexDef), "using gin", "idx_jobs_metadata_gin definition:\n%s", metadataIndexDef)
	require.Contains(t, strings.ToUpper(metadataIndexDef), "NULLIF(METADATA", "idx_jobs_metadata_gin definition:\n%s", metadataIndexDef)
	require.Contains(t, metadataIndexDef, "jsonb_path_ops", "idx_jobs_metadata_gin definition:\n%s", metadataIndexDef)
	require.True(t, postgresIndexExists(t, db, "idx_jobs_tenant"), "idx_jobs_tenant must exist")
	statusCreatedDef := strings.ToLower(postgresIndexDef(t, db, "idx_jobs_status_created"))
	require.Contains(t, statusCreatedDef, "status", "idx_jobs_status_created definition:\n%s", statusCreatedDef)
	require.Contains(t, statusCreatedDef, "created_at", "idx_jobs_status_created definition:\n%s", statusCreatedDef)
	queueCreatedDef := strings.ToLower(postgresIndexDef(t, db, "idx_jobs_queue_created"))
	require.Contains(t, queueCreatedDef, "queue", "idx_jobs_queue_created definition:\n%s", queueCreatedDef)
	require.Contains(t, queueCreatedDef, "created_at", "idx_jobs_queue_created definition:\n%s", queueCreatedDef)
	fanOutStatusDef := strings.ToLower(postgresIndexDef(t, db, "idx_jobs_fan_out_status"))
	require.Contains(t, fanOutStatusDef, "fan_out_id", "idx_jobs_fan_out_status definition:\n%s", fanOutStatusDef)
	require.Contains(t, fanOutStatusDef, "status", "idx_jobs_fan_out_status definition:\n%s", fanOutStatusDef)
	require.True(t, postgresIndexExists(t, db, "idx_concurrency_slots_expires_at"), "idx_concurrency_slots_expires_at must exist")
	require.False(t, postgresIndexExists(t, db, "idx_rate_limit_windows_lookup"), "idx_rate_limit_windows_lookup must not exist after migration")
	require.True(t, postgresIndexExists(t, db, "idx_signals_pending"), "idx_signals_pending must remain on postgres")
	signalsPendingDef := strings.ToLower(postgresIndexDef(t, db, "idx_signals_pending"))
	require.Contains(t, signalsPendingDef, "created_at", "idx_signals_pending must include created_at")
	// X1: on Postgres it must be PARTIAL (WHERE consumed_at IS NULL) so the FIFO
	// consume is a no-sort index scan; the full composite still top-N-sorts.
	require.Contains(t, signalsPendingDef, "where", "idx_signals_pending must be partial on postgres:\n%s", signalsPendingDef)
	require.Contains(t, signalsPendingDef, "consumed_at is null", "idx_signals_pending must be partial on consumed_at IS NULL:\n%s", signalsPendingDef)

	for _, indexName := range []string{
		"idx_jobs_priority",
		"idx_jobs_queue",
		"idx_jobs_locked_until",
		"idx_jobs_dequeue",
		"idx_jobs_unique_key",
		"idx_jobs_dequeue_order",
		"idx_jobs_status",
		"idx_jobs_run_at",
		"idx_jobs_fan_out_id",
		"idx_jobs_root_job_id",
		"idx_signals_job_id",
	} {
		require.False(t, postgresIndexExists(t, db, indexName), "%s must not exist after migration", indexName)
	}
	requirePostgresCascadeFKs(t, db)
	requirePostgresDispatcherColumnsNotNull(t, db)
	requirePostgresFanOutThresholdDouble(t, db)
	requirePostgresCheckConstraints(t, db)
	requirePostgresCheckConstraintsEnforced(t, db)

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
	requireMySQLDeadLetteredAtPrecision(t, db, 3)
	requireMySQLDQEligibleAtType(t, db, "datetime(3)")
	requireMySQLQueueTenantCollations(t, db)

	require.NoError(t, s.Migrate(ctx), "second Migrate must not let AutoMigrate revert dead_lettered_at precision")
	requireMySQLDeadLetteredAtPrecision(t, db, 3)
	requireMySQLDQEligibleAtType(t, db, "datetime(3)")
	requireMySQLQueueTenantCollations(t, db)

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
		"idx_jobs_status",
		"idx_jobs_run_at",
		"idx_jobs_fan_out_id",
		"idx_jobs_root_job_id",
	} {
		require.False(t, mysqlIndexExists(t, db, indexName), "%s must not exist after migration", indexName)
	}
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_unique_key"), "idx_jobs_unique_key must remain on mysql")
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_dequeue_eligible"), "idx_jobs_dequeue_eligible must exist on mysql")
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_retention_terminal"), "idx_jobs_retention_terminal must exist on mysql")
	require.Equal(t, []string{"status", "completed_at", "id"}, mysqlIndexColumns(t, db, "jobs", "idx_jobs_retention_terminal"))
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_status_created"), "idx_jobs_status_created must exist on mysql")
	require.Equal(t, []string{"status", "created_at"}, mysqlIndexColumns(t, db, "jobs", "idx_jobs_status_created"))
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_queue_created"), "idx_jobs_queue_created must exist on mysql")
	require.Equal(t, []string{"queue", "created_at"}, mysqlIndexColumns(t, db, "jobs", "idx_jobs_queue_created"))
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_fan_out_status"), "idx_jobs_fan_out_status must exist on mysql")
	require.Equal(t, []string{"fan_out_id", "status"}, mysqlIndexColumns(t, db, "jobs", "idx_jobs_fan_out_status"))
	// idx_jobs_stale_lock must be an EXPRESSION index on MySQL (status +
	// functional COALESCE key part) so the reaper's ORDER BY COALESCE(...) rides
	// the index instead of filesorting — parity with the PG/SQLite partial
	// functional index (R19).
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_stale_lock"), "idx_jobs_stale_lock must exist on mysql")
	var staleLockExprParts int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND INDEX_NAME = 'idx_jobs_stale_lock'
		  AND EXPRESSION IS NOT NULL
	`).Scan(&staleLockExprParts).Error)
	require.GreaterOrEqual(t, staleLockExprParts, 1, "idx_jobs_stale_lock must be a COALESCE expression index so the reaper ORDER BY rides it (no filesort)")
	require.True(t, mysqlTableIndexExists(t, db, "signals", "idx_signals_consumed_at"), "idx_signals_consumed_at must exist on mysql")
	require.False(t, mysqlTableIndexExists(t, db, "signals", "idx_signals_job_id"), "idx_signals_job_id must not exist after migration")
	require.True(t, mysqlTableIndexExists(t, db, "signals", "idx_signals_pending"), "idx_signals_pending must remain on mysql")
	require.Contains(t, mysqlIndexColumns(t, db, "signals", "idx_signals_pending"), "created_at", "idx_signals_pending must include created_at on mysql")
	require.True(t, mysqlTableIndexExists(t, db, "concurrency_slots", "idx_concurrency_slots_expires_at"), "idx_concurrency_slots_expires_at must exist on mysql")
	require.False(t, mysqlTableIndexExists(t, db, "rate_limit_windows", "idx_rate_limit_windows_lookup"), "idx_rate_limit_windows_lookup must not exist after migration")
	requireMySQLGeneratedBinary16Column(t, db, "pending_parent_ref")
	requireMySQLGeneratedBinary16Column(t, db, "pending_root_ref")
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_parent_nonterminal"), "idx_jobs_parent_nonterminal must exist on mysql")
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_root_nonterminal"), "idx_jobs_root_nonterminal must exist on mysql")
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_tenant"), "idx_jobs_tenant must exist on mysql")
	requireMySQLCascadeFKs(t, db)
	requireMySQLUniqueKeyCollations(t, db)
	requireMySQLDispatcherColumnsNotNull(t, db)
	requireMySQLCheckConstraints(t, db)
	requireMySQLCheckConstraintsEnforced(t, db)
	requireMySQLMetadataIntegrity(t, db)

	// Regression (P5): a bare AutoMigrate must NOT drift the NOT NULL dispatcher
	// columns back to nullable. Before the core.Job `not null` tags, AutoMigrate
	// stripped NOT NULL on every run and the migration restored it — two
	// table-copy MODIFYs per startup. With the tags AutoMigrate leaves them be.
	require.NoError(t, db.AutoMigrate(&core.Job{}))
	requireMySQLDispatcherColumnsNotNull(t, db)
	requireMySQLUniqueKeyCollations(t, db)
	requireMySQLQueueTenantCollations(t, db)
}

func TestPostgresDequeuePlanUsesEligibleIndex(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	schemaName := uniqueSchemaAssertionsName("dequeue_plan")

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
	seedDequeuePlanJobs(t, ctx, db)
	analyzeExplainJobs(t, db)

	query := buildSchemaDequeuePlanQuery(s, []string{"default"}, 10)
	var plan string
	require.NoError(t, db.Raw("EXPLAIN (FORMAT JSON) "+query.Statement.SQL.String(), query.Statement.Vars...).Scan(&plan).Error)
	t.Logf("postgres dequeue EXPLAIN JSON:\n%s", plan)
	require.Contains(t, plan, `"Index Name": "idx_jobs_dequeue_eligible"`)
	require.NotContains(t, plan, `"Node Type": "Sort"`)
}

func TestMySQLDequeuePlanUsesEligibleIndexWithoutFilesort(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	databaseName := uniqueSchemaAssertionsName("dequeue_plan")

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
	seedDequeuePlanJobs(t, ctx, db)
	analyzeExplainJobs(t, db)

	query := buildSchemaDequeuePlanQuery(s, []string{"default"}, 10)
	var plan string
	require.NoError(t, db.Raw("EXPLAIN FORMAT=JSON "+query.Statement.SQL.String(), query.Statement.Vars...).Scan(&plan).Error)
	t.Logf("mysql dequeue EXPLAIN JSON:\n%s", plan)
	require.Contains(t, plan, `"key": "idx_jobs_dequeue_eligible"`)
	// EXPLAIN FORMAT=JSON always emits the "using_filesort" field; assert it is
	// false (no filesort), not merely that the substring "filesort" is absent —
	// the field name itself contains "filesort" even when its value is false.
	require.NotContains(t, plan, `"using_filesort": true`)
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

func requirePostgresCheckConstraints(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, constraintName := range checkConstraintNames() {
		var count int
		require.NoError(t, db.Raw(`
			SELECT COUNT(*)
			FROM pg_constraint
			WHERE contype = 'c'
			  AND conname = ?
			  AND conrelid IN ('jobs'::regclass, 'fan_outs'::regclass)
		`, constraintName).Scan(&count).Error)
		require.Equal(t, 1, count, "%s CHECK constraint must exist on postgres", constraintName)
	}
}

func requirePostgresCheckConstraintsEnforced(t *testing.T, db *gorm.DB) {
	t.Helper()
	tx := db.Begin()
	require.NoError(t, tx.Error)
	defer func() {
		require.NoError(t, tx.Rollback().Error)
	}()

	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-pg-parent"), "schema.check", "pending", "default", 0, 0, 3, 0, 0).Error)

	requireExecRejectedWithSavepoint(t, tx, "pg_bad_job_status", `
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-pg-bad-status"), "schema.check", "BOGUS", "default", 0, 0, 3, 0, 0)
	requireExecRejectedWithSavepoint(t, tx, "pg_bad_job_attempt", `
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-pg-bad-attempt"), "schema.check", "pending", "default", 0, -1, 3, 0, 0)
	requireExecRejectedWithSavepoint(t, tx, "pg_bad_fanout_strategy", `
		INSERT INTO fan_outs (id, parent_job_id, total_count, strategy)
		VALUES (?, ?, ?, ?)
	`, schemaCheckID("check-pg-bad-strategy"), schemaCheckID("check-pg-parent"), 1, "nope")
	requireExecRejectedWithSavepoint(t, tx, "pg_bad_fanout_count", `
		INSERT INTO fan_outs (id, parent_job_id, total_count, completed_count)
		VALUES (?, ?, ?, ?)
	`, schemaCheckID("check-pg-bad-count"), schemaCheckID("check-pg-parent"), 1, -1)

	require.NoError(t, tx.Exec(`
		INSERT INTO fan_outs (id, parent_job_id, total_count, completed_count, failed_count, cancelled_count, strategy, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-pg-good-fanout"), schemaCheckID("check-pg-parent"), 1, 0, 0, 0, "fail_fast", "pending").Error)
}

type postgresColumnType struct {
	DataType          string `gorm:"column:data_type"`
	DateTimePrecision int    `gorm:"column:datetime_precision"`
}

func requirePostgresDeadLetteredAtType(t *testing.T, db *gorm.DB) postgresColumnType {
	t.Helper()
	var columnType postgresColumnType
	require.NoError(t, db.Raw(`
		SELECT data_type, datetime_precision
		FROM information_schema.COLUMNS
		WHERE table_schema = CURRENT_SCHEMA()
		  AND table_name = 'jobs'
		  AND column_name = 'dead_lettered_at'
	`).Scan(&columnType).Error)
	require.NotEmpty(t, columnType.DataType, "postgres jobs.dead_lettered_at must exist")
	return columnType
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

func requireMySQLDQEligibleAtType(t *testing.T, db *gorm.DB, want string) {
	t.Helper()
	var columnType string
	require.NoError(t, db.Raw(`
		SELECT COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = 'dq_eligible_at'
	`).Scan(&columnType).Error)
	require.Equal(t, want, columnType, "dq_eligible_at column_type must be %s", want)
}

func requireMySQLQueueTenantCollations(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME IN ('queue', 'tenant')
		  AND COLLATION_NAME = 'utf8mb4_0900_as_cs'
	`).Scan(&count).Error)
	require.Equal(t, 2, count, "mysql queue and tenant columns must use utf8mb4_0900_as_cs")
}

func requireMySQLCheckConstraints(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, constraintName := range checkConstraintNames() {
		var count int
		require.NoError(t, db.Raw(`
			SELECT COUNT(*)
			FROM information_schema.TABLE_CONSTRAINTS
			WHERE CONSTRAINT_SCHEMA = DATABASE()
			  AND CONSTRAINT_TYPE = 'CHECK'
			  AND CONSTRAINT_NAME = ?
		`, constraintName).Scan(&count).Error)
		require.Equal(t, 1, count, "%s CHECK constraint must exist on mysql", constraintName)
	}
}

func requireMySQLCheckConstraintsEnforced(t *testing.T, db *gorm.DB) {
	t.Helper()
	tx := db.Begin()
	require.NoError(t, tx.Error)
	defer func() {
		require.NoError(t, tx.Rollback().Error)
	}()

	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-mysql-parent"), "schema.check", "pending", "default", 0, 0, 3, 0, 0).Error)

	requireExecRejectedWithSavepoint(t, tx, "mysql_bad_job_case", `
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-mysql-bad-case"), "schema.check", "PENDING", "default", 0, 0, 3, 0, 0)
	requireExecRejectedWithSavepoint(t, tx, "mysql_bad_job_status", `
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-mysql-bad-status"), "schema.check", "BOGUS", "default", 0, 0, 3, 0, 0)
	requireExecRejectedWithSavepoint(t, tx, "mysql_bad_job_attempt", `
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-mysql-bad-attempt"), "schema.check", "pending", "default", 0, -1, 3, 0, 0)
	requireExecRejectedWithSavepoint(t, tx, "mysql_bad_fanout_strategy", `
		INSERT INTO fan_outs (id, parent_job_id, total_count, strategy)
		VALUES (?, ?, ?, ?)
	`, schemaCheckID("check-mysql-bad-strategy"), schemaCheckID("check-mysql-parent"), 1, "nope")
	requireExecRejectedWithSavepoint(t, tx, "mysql_bad_fanout_count", `
		INSERT INTO fan_outs (id, parent_job_id, total_count, completed_count)
		VALUES (?, ?, ?, ?)
	`, schemaCheckID("check-mysql-bad-count"), schemaCheckID("check-mysql-parent"), 1, -1)

	require.NoError(t, tx.Exec(`
		INSERT INTO fan_outs (id, parent_job_id, total_count, completed_count, failed_count, cancelled_count, strategy, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("check-mysql-good-fanout"), schemaCheckID("check-mysql-parent"), 1, 0, 0, 0, "fail_fast", "pending").Error)
}

func requireMySQLMetadataIntegrity(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE()
		  AND CONSTRAINT_TYPE = 'CHECK'
		  AND CONSTRAINT_NAME = 'chk_jobs_metadata_json'
	`).Scan(&count).Error)
	require.Equal(t, 1, count, "chk_jobs_metadata_json CHECK constraint must exist on mysql")

	tx := db.Begin()
	require.NoError(t, tx.Error)
	defer func() {
		require.NoError(t, tx.Rollback().Error)
	}()

	now := time.Now().UTC()
	requireExecRejectedWithSavepoint(t, tx, "mysql_bad_metadata_json", `
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-bad-json"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now, "not-valid-json")

	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-empty"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now, "").Error)
	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-null"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now, nil).Error)
	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-omitted"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now).Error)
	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-json-contains-prod"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now, `{"env":"prod"}`).Error)
	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-json-near-value"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now, `{"env":"prod-test"}`).Error)
	require.NoError(t, tx.Exec(`
		INSERT INTO jobs (id, type, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, schemaCheckID("metadata-json-near-key"), "schema.metadata", "default", "pending", 0, 0, 3, 0, 0, now, `{"team":"prod"}`).Error)

	var matches []core.UUID
	require.NoError(t, tx.Raw(`
		SELECT id
		FROM jobs
		WHERE metadata IS NOT NULL
		  AND metadata <> ''
		  AND JSON_CONTAINS(metadata, CAST(? AS JSON))
		ORDER BY id
	`, `{"env":"prod"}`).Scan(&matches).Error)
	require.Equal(t, []core.UUID{schemaCheckID("metadata-json-contains-prod")}, matches, "JSON_CONTAINS must exclude substring-only metadata near-matches")
}

func requireMySQLGeneratedBinary16Column(t *testing.T, db *gorm.DB, columnName string) {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = ?
		  AND DATA_TYPE = 'binary'
		  AND CHARACTER_MAXIMUM_LENGTH = 16
		  AND GENERATION_EXPRESSION <> ''
	`, columnName).Scan(&count).Error)
	require.Equal(t, 1, count, "%s generated binary(16) column must exist", columnName)
}

func checkConstraintNames() []string {
	return []string{
		"chk_jobs_status",
		"chk_jobs_attempt_nonneg",
		"chk_jobs_max_retries_nonneg",
		"chk_fan_outs_strategy",
		"chk_fan_outs_status",
		"chk_fan_outs_counts_nonneg",
	}
}

func requireExecRejectedWithSavepoint(t *testing.T, tx *gorm.DB, savepointName, stmt string, args ...any) {
	t.Helper()
	require.NoError(t, tx.Exec("SAVEPOINT "+savepointName).Error)
	require.Error(t, tx.Exec(stmt, args...).Error)
	require.NoError(t, tx.Exec("ROLLBACK TO SAVEPOINT "+savepointName).Error)
	require.NoError(t, tx.Exec("RELEASE SAVEPOINT "+savepointName).Error)
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
	return mysqlTableIndexExists(t, db, "jobs", indexName)
}

func mysqlTableIndexExists(t *testing.T, db *gorm.DB, tableName, indexName string) bool {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND INDEX_NAME = ?
	`, tableName, indexName).Scan(&count).Error)
	return count > 0
}

func mysqlIndexColumns(t *testing.T, db *gorm.DB, tableName, indexName string) []string {
	t.Helper()
	var columns []string
	require.NoError(t, db.Raw(`
		SELECT COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND INDEX_NAME = ?
		ORDER BY SEQ_IN_INDEX
	`, tableName, indexName).Scan(&columns).Error)
	require.NotEmpty(t, columns, "%s must exist on %s", indexName, tableName)
	return columns
}

func buildSchemaDequeuePlanQuery(s *GormStorage, queues []string, limit int) *gorm.DB {
	var candidates []*core.Job
	eligExpr := s.dequeueEligibleExpr()
	return s.db.Session(&gorm.Session{DryRun: true}).
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where("queue IN ?", queues).
		Where("status = ?", core.StatusPending).
		Where(eligExpr+" <= ?", s.nowExpr()).
		Where("(locked_until IS NULL OR locked_until < ?)", s.nowExpr()).
		Order("priority DESC, " + eligExpr + " ASC").
		Limit(limit).
		Find(&candidates)
}

func seedDequeuePlanJobs(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()

	now := time.Now().UTC()
	queues := []string{"default", "critical", "bulk"}
	// Seed a large backlog so the cost gap between the correct composite-index
	// plan and the idx_jobs_status full-scan+filesort is decisive (not within the
	// optimizer's ~1% noise margin). At a small seed the planner can flip under
	// buffer-pool contention and make the plan-aware guard flaky; at this scale
	// the ~LIMIT-bounded index lookup dwarfs sorting thousands of pending rows.
	const seedCount = 30000
	jobs := make([]*core.Job, 0, seedCount)
	for i := 0; i < seedCount; i++ {
		createdAt := now.Add(-time.Duration(seedCount-i) * time.Second)
		jobs = append(jobs, &core.Job{
			ID:         core.NewID(),
			Type:       "explain.dequeue",
			Args:       []byte(`{}`),
			Queue:      queues[i%len(queues)],
			Priority:   i % 31,
			Status:     core.StatusPending,
			MaxRetries: 3,
			CreatedAt:  createdAt,
			UpdatedAt:  createdAt,
		})
	}
	require.NoError(t, db.WithContext(ctx).CreateInBatches(jobs, 250).Error)
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

// stmtCaptureLogger records every SQL statement GORM executes so a test can
// assert what a 2nd Migrate() actually *issues*, not just the resulting schema.
// This catches a re-issued no-op ALTER (e.g. timestamptz(6)->timestamptz(6))
// that a structural before/after type check cannot see but that still takes an
// AccessExclusiveLock — the P1b dead_lettered_at flap class.
type stmtCaptureLogger struct {
	logger.Interface
	stmts []string
}

// LogMode must return the wrapper (GORM uses the returned logger), otherwise the
// embedded logger would be used and Trace would not be captured.
func (c *stmtCaptureLogger) LogMode(level logger.LogLevel) logger.Interface {
	c.Interface = c.Interface.LogMode(level)
	return c
}

func (c *stmtCaptureLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	c.stmts = append(c.stmts, sql)
}

// TestPostgresDeadLetteredAtMigrateNoFlap is the statement-level P1b regression
// guard: the structural before/after type assertion in TestPostgresSchemaAssertions
// would still pass if the precision:6 tag were reintroduced (the re-issued
// ALTER COLUMN dead_lettered_at TYPE timestamptz(6) leaves the resulting type
// byte-identical), so it cannot catch the actual AccessExclusiveLock flap. This
// captures GORM's statements on a steady-state 2nd Migrate and asserts none
// ALTER dead_lettered_at. Scoped to the column so the pre-existing (S18-adjacent)
// fan_outs.threshold SET DEFAULT churn is ignored.
func TestPostgresDeadLetteredAtMigrateNoFlap(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	schemaName := uniqueSchemaAssertionsName("deadletter_flap")

	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres test db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+quotePostgresIdent(schemaName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP SCHEMA IF EXISTS "+quotePostgresIdent(schemaName)+" CASCADE").Error)
	})

	capture := &stmtCaptureLogger{Interface: logger.Default.LogMode(logger.Silent)}
	db, err := gorm.Open(postgres.Open(postgresDSNWithSearchPath(t, dsn, schemaName)), &gorm.Config{
		Logger: capture,
	})
	require.NoError(t, err, "open isolated postgres schema")
	closeDBOnCleanup(t, db)

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx)) // build the schema
	capture.stmts = nil                // capture only the steady-state 2nd Migrate
	require.NoError(t, s.Migrate(ctx))

	for _, stmt := range capture.stmts {
		lower := strings.ToLower(stmt)
		require.Falsef(t, strings.Contains(lower, "alter") && strings.Contains(lower, "dead_lettered_at"),
			"a steady-state 2nd Migrate must not ALTER dead_lettered_at (precision flap reintroduced?): %s", stmt)
	}
}
