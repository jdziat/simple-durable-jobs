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
	"gorm.io/gorm/clause"
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
		"idx_jobs_stale_lock":         "COALESCE",
	}
	for indexName, mustContain := range defs {
		indexDef := postgresIndexDef(t, db, indexName)
		require.Contains(t, strings.ToUpper(indexDef), strings.ToUpper(mustContain), "%s definition:\n%s", indexName, indexDef)
	}
	require.Contains(t, postgresIndexDef(t, db, "idx_jobs_stale_lock"), "WHERE", "idx_jobs_stale_lock must stay partial")
	require.Contains(t, strings.ToUpper(postgresIndexDef(t, db, "idx_jobs_dequeue_eligible")), "COALESCE")
	metadataIndexDef := postgresIndexDef(t, db, "idx_jobs_metadata_gin")
	require.Contains(t, strings.ToLower(metadataIndexDef), "using gin", "idx_jobs_metadata_gin definition:\n%s", metadataIndexDef)
	require.Contains(t, strings.ToUpper(metadataIndexDef), "NULLIF(METADATA", "idx_jobs_metadata_gin definition:\n%s", metadataIndexDef)
	require.Contains(t, metadataIndexDef, "jsonb_path_ops", "idx_jobs_metadata_gin definition:\n%s", metadataIndexDef)
	require.True(t, postgresIndexExists(t, db, "idx_jobs_tenant"), "idx_jobs_tenant must exist")

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
	requireMySQLDeadLetteredAtPrecision(t, db, 3)

	require.NoError(t, s.Migrate(ctx), "second Migrate must not let AutoMigrate revert dead_lettered_at precision")
	requireMySQLDeadLetteredAtPrecision(t, db, 3)

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
	require.True(t, mysqlIndexExists(t, db, "idx_jobs_tenant"), "idx_jobs_tenant must exist on mysql")
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
			ID:         fmt.Sprintf("dequeue-plan-%04d", i),
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
