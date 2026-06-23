package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestForbidZeroUUIDRefsChecks(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" && os.Getenv("TEST_MYSQL_URL") == "" {
		t.Skip("TEST_DATABASE_URL/TEST_MYSQL_URL not set")
	}

	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		t.Run("postgres", func(t *testing.T) {
			schemaName := uniqueSchemaAssertionsName("zero_uuid_refs")

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

			requireForbidZeroUUIDRefsChecks(t, NewGormStorage(db), db, dialectPostgres)
		})
	}

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		t.Run("mysql", func(t *testing.T) {
			databaseName := uniqueSchemaAssertionsName("zero_uuid_refs")

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

			requireForbidZeroUUIDRefsChecks(t, NewGormStorage(db), db, dialectMySQL)
		})
	}
}

func requireForbidZeroUUIDRefsChecks(t *testing.T, s *GormStorage, db *gorm.DB, dialect string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.Migrate(ctx))

	zero := core.NilUUID
	for _, column := range []string{"parent_job_id", "root_job_id", "fan_out_id"} {
		err := insertSchemaCheckJob(ctx, db, schemaCheckID("zero-"+column), column, &zero)
		require.Error(t, err, "%s zero UUID ref must be rejected", column)
	}

	require.NoError(t, insertSchemaCheckJob(ctx, db, schemaCheckID("null-ref"), "", nil), "NULL refs must be allowed")
	valid := schemaCheckID("valid-non-zero-ref")
	require.NoError(t, insertSchemaCheckJob(ctx, db, schemaCheckID("valid-ref"), "parent_job_id", &valid), "non-zero refs must be allowed")

	require.NoError(t, s.Migrate(ctx), "second Migrate must be idempotent")

	require.NoError(t, dropZeroUUIDRefChecks(ctx, db, dialect))
	require.NoError(t, db.WithContext(ctx).Delete(&core.SchemaMigration{}, "version = ?", 28).Error)

	precleanID := schemaCheckID("preclean-zero-ref")
	require.NoError(t, insertSchemaCheckJob(ctx, db, precleanID, "parent_job_id", &zero))
	require.NoError(t, s.Migrate(ctx), "v28 must pre-clean zero UUID refs before adding CHECK constraints")

	var nullCount int64
	require.NoError(t, db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ? AND parent_job_id IS NULL", precleanID).
		Count(&nullCount).Error)
	require.Equal(t, int64(1), nullCount, "v28 must heal a pre-existing zero UUID parent ref to NULL")
}

func insertSchemaCheckJob(ctx context.Context, db *gorm.DB, id core.UUID, refColumn string, refValue *core.UUID) error {
	columns := "id, type, status, queue, priority, attempt, max_retries, timeout, determinism"
	placeholders := "?, ?, ?, ?, ?, ?, ?, ?, ?"
	args := []any{id, "schema.zero_uuid_refs", core.StatusPending, "default", 0, 0, 3, 0, 0}
	if refColumn != "" {
		columns += ", " + refColumn
		placeholders += ", ?"
		args = append(args, refValue)
	}
	return db.WithContext(ctx).Exec(
		fmt.Sprintf("INSERT INTO jobs (%s) VALUES (%s)", columns, placeholders),
		args...,
	).Error
}

func dropZeroUUIDRefChecks(ctx context.Context, db *gorm.DB, dialect string) error {
	for _, name := range []string{
		"chk_jobs_parent_job_not_zero",
		"chk_jobs_root_job_not_zero",
		"chk_jobs_fan_out_not_zero",
	} {
		var err error
		switch dialect {
		case dialectPostgres:
			err = db.WithContext(ctx).Exec("ALTER TABLE jobs DROP CONSTRAINT IF EXISTS " + name).Error
		case dialectMySQL:
			err = db.WithContext(ctx).Exec("ALTER TABLE jobs DROP CHECK " + name).Error
		}
		if err != nil {
			return err
		}
	}
	return nil
}
