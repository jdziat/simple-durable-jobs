package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

/*
OPENLINE P1 dequeue-index analysis:

The predicate anchors were re-verified in dequeue_batch.go and gorm.go before
this test was added. The existing v1 migration creates idx_jobs_dequeue as
(status, queue, priority, created_at) on MySQL, and a pending-row partial index
on (queue, priority DESC, created_at ASC) for PostgreSQL/SQLite.

Live EXPLAIN was run against the actual GORM dequeue shape after Migrate(ctx)
created the dequeue indexes and after seeding 2,400 pending rows across multiple
queues, followed by ANALYZE so the planner had current statistics:

	SELECT *
	FROM jobs
	WHERE queue IN ('default','critical')
	  AND status = 'pending'
	  AND (run_at IS NULL OR run_at <= NOW(6))
	  AND (locked_until IS NULL OR locked_until < NOW(6))
	ORDER BY priority DESC, created_at ASC
	LIMIT 25
	FOR UPDATE SKIP LOCKED

Baseline evidence before v9:

	MySQL existing indexes:
	id=1 select_type=SIMPLE table=jobs partitions=NULL type=ALL possible_keys=idx_jobs_queue,idx_jobs_status,idx_jobs_run_at,idx_jobs_locked_until,idx_jobs_dequeue,idx_jobs_retention_terminal,idx_jobs_stale_lock key=NULL key_len=NULL ref=NULL rows=2401 filtered=66.56 Extra=Using where; Using filesort

	PostgreSQL existing indexes:
	->  Sort  (cost=8.31..8.31 rows=1 width=2818) (actual time=2.103..2.105 rows=25 loops=1)
	      Sort Key: priority DESC, created_at
	      ->  Index Scan using idx_jobs_status on jobs  (cost=0.27..8.30 rows=1 width=2818) (actual time=0.019..0.593 rows=1600 loops=1)

Candidate evidence showed a clear available improvement, so P1 ships
schemaMigration{Version: 9} with idx_jobs_dequeue_order rather than a no-op.
SQLite gets the same partial order-first index shape as Postgres for backend
parity:

	CREATE INDEX IF NOT EXISTS idx_jobs_dequeue_order
	ON jobs (priority DESC, created_at ASC, queue)
	WHERE status = 'pending'

Live post-v9 evidence:

	MySQL classic EXPLAIN:
	id=1 select_type=SIMPLE table=jobs partitions=NULL type=range possible_keys=idx_jobs_queue,idx_jobs_status,idx_jobs_run_at,idx_jobs_locked_until,idx_jobs_dequeue,idx_jobs_retention_terminal,idx_jobs_stale_lock,idx_jobs_dequeue_order key=idx_jobs_dequeue_order key_len=83 ref=NULL rows=2400 filtered=61.530677795410156 Extra=Using index condition; Using where

	MySQL EXPLAIN FORMAT=JSON excerpt:
	"ordering_operation": {
	  "using_filesort": false,
	  "table": {
	    "table_name": "jobs",
	    "access_type": "range",
	    "key": "idx_jobs_dequeue_order",
	    "used_key_parts": ["status"],
	    "rows_examined_per_scan": 2400,
	    "index_condition": "((`jobs_test`.`jobs`.`status` = 'pending') and (`jobs_test`.`jobs`.`queue` in ('default','critical')))"
	  }
	}

	PostgreSQL EXPLAIN (ANALYZE, BUFFERS):
	Limit  (cost=0.28..13.20 rows=25 width=504) (actual time=0.025..0.056 rows=25 loops=1)
	  Buffers: shared hit=102
	  ->  LockRows  (cost=0.28..827.30 rows=1600 width=504) (actual time=0.025..0.054 rows=25 loops=1)
	        Buffers: shared hit=102
	        ->  Index Scan using idx_jobs_dequeue_order on jobs  (cost=0.28..811.30 rows=1600 width=504) (actual time=0.021..0.044 rows=25 loops=1)
	              Filter: (((queue)::text = ANY ('{default,critical}'::text[])) AND ((status)::text = 'pending'::text) AND ((run_at IS NULL) OR (run_at <= now())) AND ((locked_until IS NULL) OR (locked_until < now())))
	              Rows Removed by Filter: 12
	              Buffers: shared hit=77
	Planning:
	  Buffers: shared hit=196
	Planning Time: 1.338 ms
	Execution Time: 0.089 ms
*/

func TestMigrationsApplyAndRecordAllVersions(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	s := NewGormStorage(db)

	require.NoError(t, s.Migrate(ctx))
	require.NoError(t, s.Migrate(ctx), "migrate should be idempotent")

	maxVersion := 0
	expectedNames := make(map[int]string, len(schemaMigrations))
	for _, migration := range schemaMigrations {
		require.Greater(t, migration.Version, 0, "migration versions must be positive")
		require.NotEmpty(t, migration.Name, "migration %d must have a name", migration.Version)
		require.NotNil(t, migration.Up, "migration %d must have an Up function", migration.Version)
		require.NotContains(t, expectedNames, migration.Version, "duplicate migration version %d", migration.Version)
		expectedNames[migration.Version] = migration.Name
		if migration.Version > maxVersion {
			maxVersion = migration.Version
		}
	}
	require.Equal(t, len(schemaMigrations), maxVersion, "schema migrations must be contiguous through max version")

	var applied []core.SchemaMigration
	require.NoError(t, s.DB().WithContext(ctx).Order("version ASC").Find(&applied).Error)
	require.Len(t, applied, len(schemaMigrations))

	for version := 1; version <= maxVersion; version++ {
		row := applied[version-1]
		require.Equal(t, version, row.Version)
		require.Equal(t, expectedNames[version], row.Name)
		require.False(t, row.AppliedAt.IsZero(), "migration %d should have applied_at", version)
	}

	require.Equal(t, "unique_locks", expectedNames[10])
	requireMigrationRecorded(t, ctx, s.DB(), 10, "unique_locks")
	require.True(t, s.DB().Migrator().HasTable(&core.UniqueLock{}), "unique_locks table should exist")
	require.True(t, s.DB().Migrator().HasIndex(&core.UniqueLock{}, "idx_unique_locks_expires_at"), "unique_locks expiry index should exist")
}

func TestDequeueExplainPlanMySQL(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open mysql test db")
	closeDBOnCleanup(t, db)
	cleanupExternalDB(t, db)
	t.Cleanup(func() { cleanupExternalDB(t, db) })

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))
	seedDequeueExplainJobs(t, ctx, db)
	analyzeExplainJobs(t, db)
	requireMigrationRecorded(t, ctx, db, 13, "dequeue_eligibility_index")

	query := buildDequeueExplainQuery(db, []string{"default", "critical"}, 25)
	sqlText := db.Explain(query.Statement.SQL.String(), query.Statement.Vars...)
	t.Logf("mysql dequeue query:\n%s", sqlText)

	rows, err := db.Raw("EXPLAIN "+query.Statement.SQL.String(), query.Statement.Vars...).Rows()
	require.NoError(t, err, "mysql explain")
	defer func() { _ = rows.Close() }()

	plan := scanExplainRows(t, rows)
	require.NotEmpty(t, plan)
	joined := strings.Join(plan, "\n")
	t.Logf("mysql EXPLAIN:\n%s", joined)
	require.Contains(t, joined, "key=idx_jobs_dequeue_eligible")
	// P9 reordered the MySQL index to (status, priority DESC, dq_eligible_at, queue):
	// status='pending' is now a leading equality, so the access type is "ref" (an
	// equality lookup that returns rows already ordered by priority,dq_eligible_at)
	// rather than the old "range" seek on a dq_eligible_at-leading index.
	require.Contains(t, joined, "type=ref")
	require.NotContains(t, joined, "Using filesort")

	jsonRows, err := db.Raw("EXPLAIN FORMAT=JSON "+query.Statement.SQL.String(), query.Statement.Vars...).Rows()
	require.NoError(t, err, "mysql explain json")
	defer func() { _ = jsonRows.Close() }()

	jsonPlan := scanExplainRows(t, jsonRows)
	require.NotEmpty(t, jsonPlan)
	jsonJoined := strings.Join(jsonPlan, "\n")
	t.Logf("mysql EXPLAIN FORMAT=JSON:\n%s", jsonJoined)
	require.Contains(t, jsonJoined, `"key": "idx_jobs_dequeue_eligible"`)
}

func TestDequeueExplainPlanPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres test db")
	closeDBOnCleanup(t, db)
	cleanupExternalDB(t, db)
	t.Cleanup(func() { cleanupExternalDB(t, db) })

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))
	seedDequeueExplainJobs(t, ctx, db)
	analyzeExplainJobs(t, db)
	requireMigrationRecorded(t, ctx, db, 13, "dequeue_eligibility_index")

	query := buildDequeueExplainQuery(db, []string{"default", "critical"}, 25)
	sqlText := db.Explain(query.Statement.SQL.String(), query.Statement.Vars...)
	t.Logf("postgres dequeue query:\n%s", sqlText)

	rows, err := db.Raw("EXPLAIN (ANALYZE, BUFFERS) "+query.Statement.SQL.String(), query.Statement.Vars...).Rows()
	require.NoError(t, err, "postgres explain")
	defer func() { _ = rows.Close() }()

	plan := scanExplainRows(t, rows)
	require.NotEmpty(t, plan)
	joined := strings.Join(plan, "\n")
	t.Logf("postgres EXPLAIN:\n%s", joined)
	require.Contains(t, joined, "Index Scan using idx_jobs_dequeue_eligible")
	require.NotContains(t, joined, "Sort  (")
}

func TestDeadLetterMetaContainsExplainPlanPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres test db")
	closeDBOnCleanup(t, db)
	cleanupExternalDB(t, db)
	t.Cleanup(func() { cleanupExternalDB(t, db) })

	ctx := context.Background()
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))
	seedDeadLetterMetaExplainJobs(t, ctx, db)
	analyzeExplainJobs(t, db)
	requireMigrationRecorded(t, ctx, db, 16, "dlq_metadata_index")

	rows, err := db.Raw(`
		EXPLAIN
		SELECT id
		FROM jobs
		WHERE dead_lettered_at IS NOT NULL
		  AND (NULLIF(metadata, '')::jsonb) @> ?::jsonb
	`, `{"region":"us"}`).Rows()
	require.NoError(t, err, "postgres dlq metadata explain")
	defer func() { _ = rows.Close() }()

	plan := scanExplainRows(t, rows)
	require.NotEmpty(t, plan)
	joined := strings.Join(plan, "\n")
	t.Logf("postgres DLQ metadata EXPLAIN:\n%s", joined)
	require.Contains(t, joined, "Bitmap Index Scan")
	require.Contains(t, joined, "idx_jobs_metadata_gin")
	require.NotContains(t, joined, "Seq Scan")
}

func buildDequeueExplainQuery(db *gorm.DB, queues []string, limit int) *gorm.DB {
	var candidates []*core.Job
	nowExpr := gorm.Expr("NOW()")
	eligExpr := "COALESCE(run_at, created_at)"
	if strings.Contains(strings.ToLower(db.Name()), "mysql") {
		nowExpr = gorm.Expr("NOW(6)")
		eligExpr = "dq_eligible_at"
	}
	return db.Session(&gorm.Session{DryRun: true}).
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where("queue IN ?", queues).
		Where("status = ?", core.StatusPending).
		Where(eligExpr+" <= ?", nowExpr).
		Where("(locked_until IS NULL OR locked_until < ?)", nowExpr).
		Order("priority DESC, " + eligExpr + " ASC").
		Limit(limit).
		Find(&candidates)
}

func seedDequeueExplainJobs(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.WithContext(ctx).Where("1 = 1").Delete(&core.Job{}).Error)

	now := time.Now().UTC()
	queues := []string{"default", "critical", "bulk"}
	statuses := []core.JobStatus{core.StatusCompleted, core.StatusFailed, core.StatusRunning, core.StatusCancelled}
	jobs := make([]*core.Job, 0, 4400)
	for i := 0; i < 2400; i++ {
		createdAt := now.Add(-time.Duration(2400-i) * time.Second)
		status := statuses[i%len(statuses)]
		if i%10 == 0 {
			status = core.StatusPending
		}
		job := &core.Job{
			ID:         fmt.Sprintf("explain-%04d", i),
			Type:       "explain.dequeue",
			Args:       []byte(`{}`),
			Queue:      queues[i%len(queues)],
			Priority:   i % 17,
			Status:     status,
			MaxRetries: 3,
			CreatedAt:  createdAt,
			UpdatedAt:  createdAt,
		}
		jobs = append(jobs, job)
	}
	// Large future-dated pending backlog (the S01 pathology): forces the planner
	// to prefer the eligibility range index over a status='pending' ref that would
	// have to walk every pending row. Below ~750 future rows MySQL still picks
	// idx_jobs_status; 2000 keeps the eligible-range index the clear winner.
	futureRunAt := now.Add(24 * time.Hour)
	for i := 0; i < 2000; i++ {
		createdAt := now.Add(-time.Duration(2000-i) * time.Second)
		jobs = append(jobs, &core.Job{
			ID:         fmt.Sprintf("explain-future-%04d", i),
			Type:       "explain.dequeue",
			Args:       []byte(`{}`),
			Queue:      queues[i%len(queues)],
			Priority:   i % 23,
			Status:     core.StatusPending,
			MaxRetries: 3,
			RunAt:      &futureRunAt,
			CreatedAt:  createdAt,
			UpdatedAt:  createdAt,
		})
	}
	require.NoError(t, db.WithContext(ctx).CreateInBatches(jobs, 250).Error)
}

func seedDeadLetterMetaExplainJobs(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.WithContext(ctx).Where("1 = 1").Delete(&core.Job{}).Error)

	now := time.Now().UTC()
	jobs := make([]*core.Job, 0, 5000)
	for i := 0; i < 5000; i++ {
		deadLetteredAt := now.Add(-time.Duration(i) * time.Second)
		region := "eu"
		if i%200 == 0 {
			region = "us"
		}
		jobs = append(jobs, &core.Job{
			ID:               fmt.Sprintf("dlqmeta-%04d", i),
			Type:             "explain.dlqmeta",
			Args:             []byte(`{}`),
			Queue:            "default",
			Status:           core.StatusFailed,
			MaxRetries:       1,
			Metadata:         core.MetadataMap{"region": region, "env": "prod"},
			DeadLetteredAt:   &deadLetteredAt,
			DeadLetterReason: "max retries exhausted: boom",
			CompletedAt:      &deadLetteredAt,
			CreatedAt:        deadLetteredAt,
			UpdatedAt:        deadLetteredAt,
		})
	}
	require.NoError(t, db.WithContext(ctx).CreateInBatches(jobs, 250).Error)
}

func analyzeExplainJobs(t *testing.T, db *gorm.DB) {
	t.Helper()
	switch {
	case strings.Contains(strings.ToLower(db.Name()), "mysql"):
		require.NoError(t, db.Exec("ANALYZE TABLE jobs").Error)
	case strings.Contains(strings.ToLower(db.Name()), "postgres"):
		require.NoError(t, db.Exec("ANALYZE jobs").Error)
	}
}

func requireMigrationRecorded(t *testing.T, ctx context.Context, db *gorm.DB, version int, name string) {
	t.Helper()
	var migration core.SchemaMigration
	require.NoError(t, db.WithContext(ctx).First(&migration, "version = ?", version).Error)
	require.Equal(t, name, migration.Name)
	require.False(t, migration.AppliedAt.IsZero())
}

func scanExplainRows(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	cols, err := rows.Columns()
	require.NoError(t, err)
	out := make([]string, 0)
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		parts := make([]string, 0, len(cols))
		for i, col := range cols {
			value := "NULL"
			if values[i].Valid {
				value = values[i].String
			}
			parts = append(parts, col+"="+value)
		}
		out = append(out, strings.Join(parts, "\t"))
	}
	require.NoError(t, rows.Err())
	return out
}

func closeDBOnCleanup(t testing.TB, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	require.NoError(t, err, "get underlying sql.DB")
	sqlDB.SetMaxOpenConns(2)
	sqlDB.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
}
