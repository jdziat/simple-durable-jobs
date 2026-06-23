package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func TestWithIndexedMetadataKeys_DeduplicatesOrderStable(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := NewGormStorage(db, WithIndexedMetadataKeys("team", "env", "team", "region", "env"))
	assert.Equal(t, []string{"team", "env", "region"}, store.indexedMetadataKeyList())
}

func TestIndexedMetadataKeyPattern(t *testing.T) {
	valid := []string{"a", "A", "team_1", strings.Repeat("x", 48)}
	for _, key := range valid {
		assert.Truef(t, indexedMetadataKeyPattern.MatchString(key), "%q should be valid", key)
	}

	invalid := []string{"", "a-b", "a b", "team.env", strings.Repeat("x", 49)}
	for _, key := range invalid {
		assert.Falsef(t, indexedMetadataKeyPattern.MatchString(key), "%q should be invalid", key)
	}
}

func TestMySQLMetadataGenColumnDefinition(t *testing.T) {
	assert.Equal(
		t,
		"meta_team",
		mysqlMetadataGenColumn("team"),
	)
	assert.Equal(
		t,
		"meta_team VARCHAR(255) COLLATE utf8mb4_bin GENERATED ALWAYS AS (JSON_VALUE(metadata, '$.team' RETURNING CHAR(255) NULL ON ERROR)) STORED",
		mysqlMetadataGenColumnDefinition("team"),
	)
}

func TestMySQLIndexedMetadataKeys_Live(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	ctx := context.Background()

	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	closeDBOnCleanup(t, adminDB)

	databaseName := uniqueSchemaAssertionsName("sdj_meta_idx")
	require.NoError(t, adminDB.Exec("CREATE DATABASE "+quoteMySQLIdent(databaseName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP DATABASE IF EXISTS "+quoteMySQLIdent(databaseName)).Error)
	})

	db, err := gorm.Open(mysql.Open(mysqlDSNWithDatabase(t, dsn, databaseName)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	closeDBOnCleanup(t, db)

	baseStore := NewGormStorage(db)
	require.NoError(t, baseStore.Migrate(ctx))
	insertMySQLEmptyMetadataJob(t, ctx, db, testUUID("meta-empty-before-gencol"))

	store := NewGormStorage(db, WithIndexedMetadataKeys("team", "env"))
	require.NoError(t, store.Migrate(ctx))
	assert.True(t, db.Migrator().HasColumn("jobs", "meta_team"))
	assert.True(t, db.Migrator().HasColumn("jobs", "meta_env"))
	assert.True(t, db.Migrator().HasIndex("jobs", "idx_jobs_meta_team"))
	assert.True(t, db.Migrator().HasIndex("jobs", "idx_jobs_meta_env"))
	assertMySQLMetadataGeneratedColumn(t, db, "team")
	assertMySQLMetadataGeneratedColumn(t, db, "env")

	countColumnsAfterFirst := countMySQLMetadataColumns(t, db)
	countAfterFirst := countMySQLMetadataIndexes(t, db)
	require.NoError(t, store.Migrate(ctx))
	assert.Equal(t, countColumnsAfterFirst, countMySQLMetadataColumns(t, db))
	assert.Equal(t, countAfterFirst, countMySQLMetadataIndexes(t, db))

	seedIndexedMetadataJobs(t, ctx, store)
	insertMySQLEmptyMetadataJob(t, ctx, db, testUUID("meta-empty-after-gencol"))
	assertMySQLGeneratedColumnNull(t, db, testUUID("meta-empty-before-gencol"), "team")
	assertMySQLGeneratedColumnNull(t, db, testUUID("meta-empty-after-gencol"), "team")
	seedMySQLIndexedMetadataExplainRows(t, ctx, db)
	require.NoError(t, db.Exec("ANALYZE TABLE jobs").Error)

	plan := explainMySQLMetadataFilter(t, db, "team", "alpha")
	assert.Equal(t, "idx_jobs_meta_team", plan.Key)
	assert.Equal(t, "ref", plan.Type)
	assert.NotEqual(t, "ALL", plan.Type)

	assertSameMetadataSearchIDs(t, ctx, store, NewGormStorage(db), core.MetadataMap{"team": "alpha"})
	assert.NotContains(t, metadataSearchIDs(t, ctx, store, core.MetadataMap{"team": "alpha"}), string(testUUID("meta-Alpha-us")))
	assertSameMetadataSearchIDs(t, ctx, store, NewGormStorage(db), core.MetadataMap{"region": "us"})
	assertSameMetadataSearchIDs(t, ctx, store, NewGormStorage(db), core.MetadataMap{"team": "alpha", "region": "us"})

	mixedPlan := explainMySQLMetadataFilter(t, db, "team", "alpha")
	assert.Equal(t, "idx_jobs_meta_team", mixedPlan.Key)
	assert.Equal(t, "ref", mixedPlan.Type)
}

func TestMySQLIndexedMetadataKeys_InvalidKeyFailsMigrateBeforeIndexes(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}
	ctx := context.Background()

	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	closeDBOnCleanup(t, adminDB)

	databaseName := uniqueSchemaAssertionsName("sdj_meta_idx_bad")
	require.NoError(t, adminDB.Exec("CREATE DATABASE "+quoteMySQLIdent(databaseName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP DATABASE IF EXISTS "+quoteMySQLIdent(databaseName)).Error)
	})

	db, err := gorm.Open(mysql.Open(mysqlDSNWithDatabase(t, dsn, databaseName)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	closeDBOnCleanup(t, db)

	store := NewGormStorage(db, WithIndexedMetadataKeys("team", "a-b"))
	err = store.Migrate(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `storage: indexed metadata key "a-b" must match [A-Za-z0-9_]{1,48}`)
	assert.False(t, db.Migrator().HasColumn("jobs", "meta_team"))
	assert.False(t, db.Migrator().HasIndex("jobs", "idx_jobs_meta_team"))
}

func TestPostgresIndexedMetadataKeys_NoOpLive(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()

	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	closeDBOnCleanup(t, adminDB)

	schemaName := uniqueSchemaAssertionsName("sdj_meta_idx")
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+quotePostgresIdent(schemaName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP SCHEMA IF EXISTS "+quotePostgresIdent(schemaName)+" CASCADE").Error)
	})

	db, err := gorm.Open(postgres.Open(postgresDSNWithSearchPath(t, dsn, schemaName)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	closeDBOnCleanup(t, db)

	store := NewGormStorage(db, WithIndexedMetadataKeys("team", "a-b"))
	require.NoError(t, store.Migrate(ctx))
	seedIndexedMetadataJobs(t, ctx, store)

	filter := core.MetadataMap{"team": "alpha"}
	jobs, total, err := store.SearchJobs(ctx, core.JobFilter{MetaContains: &filter, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.ElementsMatch(t, []string{
		string(testUUID("meta-alpha-us")),
		string(testUUID("meta-alpha-eu")),
	}, jobIDs(jobs))

	var metaIndexes []string
	require.NoError(t, db.Raw(
		"SELECT indexname FROM pg_indexes WHERE schemaname = ? AND tablename = 'jobs' AND indexname LIKE 'idx\\_jobs\\_meta\\_%'",
		schemaName,
	).Scan(&metaIndexes).Error)
	assert.Empty(t, metaIndexes)
	var metaColumns []string
	require.NoError(t, db.Raw(
		"SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = 'jobs' AND column_name LIKE 'meta\\_%'",
		schemaName,
	).Scan(&metaColumns).Error)
	assert.Empty(t, metaColumns)

	dryRun := db.Session(&gorm.Session{DryRun: true}).Model(&core.Job{})
	stmt := applyMetaContains(store, dryRun, &filter).Find(&[]core.Job{}).Statement
	assert.Contains(t, stmt.SQL.String(), "@>")
	assert.NotContains(t, stmt.SQL.String(), "meta_team")
}

func seedIndexedMetadataJobs(t *testing.T, ctx context.Context, store *GormStorage) {
	t.Helper()
	now := time.Now().UTC()
	jobs := []*core.Job{
		{
			ID:        testUUID("meta-alpha-us"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Metadata:  map[string]string{"team": "alpha", "env": "prod", "region": "us"},
			Args:      []byte(`{}`),
			CreatedAt: now,
		},
		{
			ID:        testUUID("meta-beta-us"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Metadata:  map[string]string{"team": "beta", "env": "prod", "region": "us"},
			Args:      []byte(`{}`),
			CreatedAt: now.Add(time.Second),
		},
		{
			ID:        testUUID("meta-alpha-eu"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Metadata:  map[string]string{"team": "alpha", "env": "dev", "region": "eu"},
			Args:      []byte(`{}`),
			CreatedAt: now.Add(2 * time.Second),
		},
		{
			ID:        testUUID("meta-Alpha-us"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Metadata:  map[string]string{"team": "Alpha", "env": "prod", "region": "us"},
			Args:      []byte(`{}`),
			CreatedAt: now.Add(3 * time.Second),
		},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}
}

func insertMySQLEmptyMetadataJob(t *testing.T, ctx context.Context, db *gorm.DB, id core.UUID) {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, db.WithContext(ctx).Exec(`
		INSERT INTO jobs (id, type, args, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, updated_at, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, "metadata.empty", []byte(`{}`), "default", core.StatusPending, 0, 0, 3, 0, 0, now, now, "").Error)
}

func seedMySQLIndexedMetadataExplainRows(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.WithContext(ctx).Exec(`
		INSERT INTO jobs (id, type, args, queue, status, priority, attempt, max_retries, timeout, determinism, created_at, updated_at, metadata)
		SELECT UNHEX(REPLACE(UUID(), '-', '')),
		       'metadata.explain',
		       CAST('{}' AS BINARY),
		       'default',
		       'pending',
		       0,
		       0,
		       3,
		       0,
		       0,
		       UTC_TIMESTAMP(6),
		       UTC_TIMESTAMP(6),
		       JSON_OBJECT('team', 'bulk', 'env', 'prod', 'region', 'zz')
		FROM information_schema.columns c1
		CROSS JOIN information_schema.columns c2
		LIMIT 5000
	`).Error)
}

func assertMySQLMetadataGeneratedColumn(t *testing.T, db *gorm.DB, key string) {
	t.Helper()
	type columnInfo struct {
		ColumnName           string
		Extra                string
		CollationName        string
		GenerationExpression string
	}
	var col columnInfo
	require.NoError(t, db.Raw(`
		SELECT column_name, extra, collation_name, generation_expression
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = 'jobs'
		  AND column_name = ?
	`, mysqlMetadataGenColumn(key)).Scan(&col).Error)
	require.Equal(t, mysqlMetadataGenColumn(key), col.ColumnName)
	assert.Equal(t, "utf8mb4_bin", col.CollationName)
	assert.Contains(t, strings.ToLower(col.Extra), "stored generated")
	assert.Contains(t, strings.ToLower(col.GenerationExpression), "json_value")
}

func assertMySQLGeneratedColumnNull(t *testing.T, db *gorm.DB, id core.UUID, key string) {
	t.Helper()
	var value sql.NullString
	require.NoError(t, db.Raw(
		"SELECT "+mysqlMetadataGenColumn(key)+" FROM jobs WHERE id = ?",
		id,
	).Scan(&value).Error)
	assert.False(t, value.Valid)
}

func countMySQLMetadataColumns(t *testing.T, db *gorm.DB) int {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = 'jobs'
		  AND column_name LIKE 'meta\_%'
	`).Scan(&count).Error)
	return count
}

func countMySQLMetadataIndexes(t *testing.T, db *gorm.DB) int {
	t.Helper()
	var count int
	require.NoError(t, db.Raw(`
		SELECT COUNT(DISTINCT index_name)
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		  AND table_name = 'jobs'
		  AND index_name LIKE 'idx_jobs_meta_%'
	`).Scan(&count).Error)
	return count
}

type mysqlExplainRow struct {
	Type string `gorm:"column:type"`
	Key  string `gorm:"column:key"`
}

func explainMySQLMetadataFilter(t *testing.T, db *gorm.DB, key, value string) mysqlExplainRow {
	t.Helper()
	var rows []mysqlExplainRow
	require.NoError(t, db.Raw(
		"EXPLAIN SELECT * FROM jobs WHERE "+mysqlMetadataGenColumn(key)+" = ?",
		value,
	).Scan(&rows).Error)
	require.NotEmpty(t, rows)
	return rows[0]
}

func assertSameMetadataSearchIDs(t *testing.T, ctx context.Context, indexed, fallback *GormStorage, filter core.MetadataMap) {
	t.Helper()
	indexedIDs := metadataSearchIDs(t, ctx, indexed, filter)
	fallbackIDs := metadataSearchIDs(t, ctx, fallback, filter)
	assert.Equal(t, fallbackIDs, indexedIDs, fmt.Sprintf("filter %#v should match JSON_CONTAINS path", filter))
}

func metadataSearchIDs(t *testing.T, ctx context.Context, store *GormStorage, filter core.MetadataMap) []string {
	t.Helper()
	jobs, _, err := store.SearchJobs(ctx, core.JobFilter{MetaContains: &filter, Limit: 20})
	require.NoError(t, err)
	ids := jobIDs(jobs)
	sort.Strings(ids)
	return ids
}
