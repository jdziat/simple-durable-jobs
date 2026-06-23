package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestMigrateConvertsLegacySQLiteTextUUIDsToBinary(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: builds a sqlite-syntax legacy schema; the PG/MySQL conversion paths are verified against live populated DBs")
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	rootID := testUUID("legacy-root")
	childID := testUUID("legacy-child")
	fanOutID := testUUID("legacy-fanout")
	checkpointID := testUUID("legacy-checkpoint")
	signalID := testUUID("legacy-signal")
	heldSlotJobID := testUUID("legacy-held-slot")
	uniqueLockJobID := testUUID("legacy-unique-lock")

	createLegacySQLiteUUIDSchema(t, db)
	require.NoError(t, db.Exec(`
		INSERT INTO jobs (
			id, type, args, queue, tenant, metadata, priority, status, previous_status,
			attempt, max_retries, timeout, determinism, last_error, dead_letter_reason,
			created_at, updated_at, locked_by, unique_key, parent_job_id, root_job_id,
			fan_out_id, fan_out_index
		) VALUES
		(?, 'root', X'7B7D', 'default', '', '{}', 0, 'waiting', '', 0, 3, 0, 0, '', '', ?, ?, '', '', NULL, NULL, NULL, 0),
		(?, 'child', X'7B7D', 'default', '', '{}', 1, 'pending', '', 0, 3, 0, 0, '', '', ?, ?, '', '', ?, ?, ?, 0)
	`, rootID.String(), now, now, childID.String(), now, now, rootID.String(), rootID.String(), fanOutID.String()).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO fan_outs (
			id, parent_job_id, total_count, completed_count, failed_count, cancelled_count,
			strategy, threshold, status, cancel_on_fail, created_at, updated_at
		) VALUES (?, ?, 1, 0, 0, 0, 'fail_fast', 1.0, 'pending', false, ?, ?)
	`, fanOutID.String(), rootID.String(), now, now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO checkpoints (
			id, job_id, call_index, call_type, result, error, error_kind, error_cause,
			error_delay_nanos, created_at
		) VALUES (?, ?, 0, 'call', X'01', '', '', '', 0, ?)
	`, checkpointID.String(), childID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO signals (id, job_id, name, payload, created_at)
		VALUES (?, ?, 'ready', X'02', ?)
	`, signalID.String(), childID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at)
		VALUES ('sentinel', '', '', ?), ('held', ?, 'worker-1', ?)
	`, now, heldSlotJobID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
		VALUES ('abc123', ?, ?, ?)
	`, uniqueLockJobID.String(), now, now).Error)

	require.NoError(t, convertLegacyStringUUIDColumns(ctx, db, dialectSQLite))
	assertLegacyUUIDRowsConverted(t, db, map[string]core.UUID{
		"root":       rootID,
		"child":      childID,
		"fan_out":    fanOutID,
		"checkpoint": checkpointID,
		"signal":     signalID,
		"held_slot":  heldSlotJobID,
		"unique":     uniqueLockJobID,
	})

	require.NoError(t, convertLegacyStringUUIDColumns(ctx, db, dialectSQLite), "second conversion must be a no-op")
	assertLegacyUUIDRowsConverted(t, db, map[string]core.UUID{
		"root":       rootID,
		"child":      childID,
		"fan_out":    fanOutID,
		"checkpoint": checkpointID,
		"signal":     signalID,
		"held_slot":  heldSlotJobID,
		"unique":     uniqueLockJobID,
	})
}

func TestSQLiteUUIDConversionResumesWhenJobsIDAlreadyBinary(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: simulates a partial sqlite UUID conversion state")
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	rootID := testUUID("partial-root")
	childID := testUUID("partial-child")
	fanOutID := testUUID("partial-fanout")
	checkpointID := testUUID("partial-checkpoint")
	signalID := testUUID("partial-signal")
	heldSlotJobID := testUUID("partial-held-slot")
	uniqueLockJobID := testUUID("partial-unique-lock")

	createLegacySQLiteUUIDSchema(t, db)
	require.NoError(t, db.Exec(`
		INSERT INTO jobs (
			id, type, args, queue, tenant, metadata, priority, status, previous_status,
			attempt, max_retries, timeout, determinism, last_error, dead_letter_reason,
			created_at, updated_at, locked_by, unique_key, parent_job_id, root_job_id,
			fan_out_id, fan_out_index
		) VALUES
		(?, 'root', X'7B7D', 'default', '', '{}', 0, 'waiting', '', 0, 3, 0, 0, '', '', ?, ?, '', '', NULL, NULL, NULL, 0),
		(?, 'child', X'7B7D', 'default', '', '{}', 1, 'pending', '', 0, 3, 0, 0, '', '', ?, ?, '', '', ?, ?, ?, 0)
	`, rootID.String(), now, now, childID.String(), now, now, rootID.String(), rootID.String(), fanOutID.String()).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO fan_outs (
			id, parent_job_id, total_count, completed_count, failed_count, cancelled_count,
			strategy, threshold, status, cancel_on_fail, created_at, updated_at
		) VALUES (?, ?, 1, 0, 0, 0, 'fail_fast', 1.0, 'pending', false, ?, ?)
	`, fanOutID.String(), rootID.String(), now, now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO checkpoints (
			id, job_id, call_index, call_type, result, error, error_kind, error_cause,
			error_delay_nanos, created_at
		) VALUES (?, ?, 0, 'call', X'01', '', '', '', 0, ?)
	`, checkpointID.String(), childID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO signals (id, job_id, name, payload, created_at)
		VALUES (?, ?, 'ready', X'02', ?)
	`, signalID.String(), childID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at)
		VALUES ('sentinel', '', '', ?), ('held', ?, 'worker-1', ?)
	`, now, heldSlotJobID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
		VALUES ('abc123', ?, ?, ?)
	`, uniqueLockJobID.String(), now, now).Error)

	require.NoError(t, rewriteSQLiteUUIDColumn(ctx, db, legacyUUIDColumn{table: "jobs", column: "id", primary: true}))
	jobsIDLegacy, err := sqliteJobsIDIsLegacyText(ctx, db)
	require.NoError(t, err)
	require.False(t, jobsIDLegacy, "partial state must look complete to the old jobs.id-only guard")

	require.NoError(t, convertLegacyStringUUIDColumns(ctx, db, dialectSQLite))
	assertLegacyUUIDRowsConverted(t, db, map[string]core.UUID{
		"root":       rootID,
		"child":      childID,
		"fan_out":    fanOutID,
		"checkpoint": checkpointID,
		"signal":     signalID,
		"held_slot":  heldSlotJobID,
		"unique":     uniqueLockJobID,
	})
}

func TestPreMigrationFenceFreshDBRecordsDoneAndSkipsSecondRun(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: verifies pre-migration fence behavior through the default Migrate path")
	}
	ctx := context.Background()
	s := NewGormStorage(db)

	calls := 0
	restore := replacePreMigrationsForTest(t, []preMigration{
		{
			Name: "uuid_binary_conversion",
			Up: func(context.Context, *gorm.DB, string) error {
				calls++
				return nil
			},
		},
	})
	defer restore()

	require.NoError(t, s.Migrate(ctx))
	require.Equal(t, 1, calls)
	assertPreMigrationDone(t, ctx, db, "uuid_binary_conversion")

	require.NoError(t, s.Migrate(ctx), "second Migrate must skip completed pre-migration")
	require.Equal(t, 1, calls)
	assertPreMigrationDone(t, ctx, db, "uuid_binary_conversion")
}

func TestPreMigrationFenceCrashResumeRerunsStartedLegacySQLiteConversion(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: simulates STARTED-without-DONE pre-migration recovery")
	}
	ctx := context.Background()
	s := NewGormStorage(db)
	now := time.Now().UTC().Truncate(time.Second)
	ids := seedLegacySQLiteUUIDRows(t, db, "pre-fence-resume", now)

	require.NoError(t, ensurePreMigrationTable(ctx, db, dialectSQLite))
	startedAt := now.Add(-time.Minute)
	require.NoError(t, db.WithContext(ctx).Create(&core.PreMigration{
		Name:      "uuid_binary_conversion",
		StartedAt: &startedAt,
	}).Error)

	require.NoError(t, s.applyPreMigrations(ctx, db))

	assertPreMigrationDone(t, ctx, db, "uuid_binary_conversion")
	assertLegacyUUIDRowsConverted(t, db, ids)
}

func TestPreMigrationFenceBackwardCompatAlreadyConvertedSQLiteRecordsDone(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: builds a sqlite already-converted schema")
	}
	ctx := context.Background()
	s := NewGormStorage(db)
	now := time.Now().UTC().Truncate(time.Second)
	ids := seedLegacySQLiteUUIDRows(t, db, "pre-fence-backcompat", now)

	require.NoError(t, convertLegacyStringUUIDColumns(ctx, db, dialectSQLite))
	require.False(t, db.Migrator().HasTable(&core.PreMigration{}), "new fence table should not exist before upgrade")

	require.NoError(t, s.applyPreMigrations(ctx, db))

	assertPreMigrationDone(t, ctx, db, "uuid_binary_conversion")
	assertLegacyUUIDRowsConverted(t, db, ids)
}

func TestRecreateMySQLConvertedCompositeIndexesRestoresFullDefinitions(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: seeds bug-shaped indexes via sqlite syntax; live MySQL index parity is verified against a populated DB")
	}
	ctx := context.Background()

	createLegacySQLiteUUIDSchema(t, db)
	for _, stmt := range []string{
		"CREATE UNIQUE INDEX idx_checkpoints_job_call ON checkpoints (call_index, call_type)",
		"CREATE INDEX idx_signals_pending ON signals (name, consumed_at, created_at)",
		"CREATE INDEX idx_signals_consumed_at ON signals (consumed_at)",
		"CREATE INDEX idx_jobs_fan_out_status ON jobs (status)",
		"CREATE INDEX idx_jobs_retention_terminal ON jobs (status, completed_at)",
		"CREATE INDEX idx_fan_outs_parent_status ON fan_outs (status)",
	} {
		require.NoError(t, db.Exec(stmt).Error)
	}

	stats, err := recreateMySQLConvertedCompositeIndexes(ctx, db)
	require.NoError(t, err)
	assert.Positive(t, stats.dropped)
	assert.Positive(t, stats.created)
	stats, err = recreateMySQLConvertedCompositeIndexes(ctx, db)
	require.NoError(t, err, "second rebuild must be idempotent")
	assert.Zero(t, stats.dropped, "second rebuild must not drop matching indexes")
	assert.Zero(t, stats.created, "second rebuild must not create matching indexes")

	for _, spec := range mysqlConvertedCompositeIndexes {
		assertSQLiteIndexColumns(t, db, spec.table, spec.name, spec.columns)
		assertSQLiteIndexUnique(t, db, spec.table, spec.name, spec.unique)
	}
}

func TestRecreateMySQLConvertedCompositeIndexesNoChurnWhenDefinitionsMatch(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: observes MySQL conversion helper DDL decisions via sqlite metadata")
	}
	ctx := context.Background()

	createLegacySQLiteUUIDSchema(t, db)
	createMatchingSQLiteMySQLConvertedCompositeIndexes(t, db)

	stats, err := recreateMySQLConvertedCompositeIndexes(ctx, db)
	require.NoError(t, err)
	assert.Zero(t, stats.dropped, "matching composite indexes must not be dropped")
	assert.Zero(t, stats.created, "matching composite indexes must not be recreated")

	for _, spec := range mysqlConvertedCompositeIndexes {
		assertSQLiteIndexColumns(t, db, spec.table, spec.name, spec.columns)
		assertSQLiteIndexUnique(t, db, spec.table, spec.name, spec.unique)
	}
}

func TestRecreateMySQLConvertedCompositeIndexesRepairsBinaryNoMarkerCrashWindow(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-only: simulates all-binary UUID columns plus a missing post-conversion index")
	}
	ctx := context.Background()

	createLegacySQLiteUUIDSchema(t, db)
	require.NoError(t, declareSQLiteUUIDColumnsAsBlob(ctx, db))
	createMatchingSQLiteMySQLConvertedCompositeIndexes(t, db)
	require.NoError(t, db.Exec("DROP INDEX idx_jobs_retention_terminal").Error)

	stats, err := recreateMySQLConvertedCompositeIndexes(ctx, db)
	require.NoError(t, err)
	assert.Zero(t, stats.dropped)
	assert.Equal(t, 1, stats.created)

	for _, spec := range mysqlConvertedCompositeIndexes {
		assertSQLiteIndexColumns(t, db, spec.table, spec.name, spec.columns)
		assertSQLiteIndexUnique(t, db, spec.table, spec.name, spec.unique)
	}
}

func TestMySQLUUIDConversionProcessesJobsIDLast(t *testing.T) {
	ordered := mysqlLegacyUUIDColumnsInConversionOrder()
	require.NotEmpty(t, ordered)
	assert.Equal(t, legacyUUIDColumn{table: "jobs", column: "id", primary: true}, ordered[len(ordered)-1])
	for _, col := range ordered[:len(ordered)-1] {
		assert.False(t, col.table == "jobs" && col.column == "id")
	}
}

func TestMigrateMySQLUUIDConversionConvergesFromPartialStates(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set")
	}

	adminDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open mysql admin db")
	closeDBOnCleanup(t, adminDB)

	// These cover the two dangerous auto-committed MySQL DDL crash windows:
	// a child UUID column is binary while jobs.id is still varchar, and all UUID
	// columns are binary while generated columns, indexes, FKs, and the completion
	// marker have not yet been restored.
	for _, tc := range []struct {
		name    string
		partial func(*testing.T, context.Context, *gorm.DB)
	}{
		{
			name: "mid_child_before_jobs_id",
			partial: func(t *testing.T, ctx context.Context, db *gorm.DB) {
				t.Helper()
				require.NoError(t, convertMySQLLegacyUUIDColumn(ctx, db, legacyUUIDColumn{table: "checkpoints", column: "job_id"}))
			},
		},
		{
			name: "binary_no_marker",
			partial: func(t *testing.T, ctx context.Context, db *gorm.DB) {
				t.Helper()
				for _, col := range mysqlLegacyUUIDColumnsInConversionOrder() {
					require.NoError(t, convertMySQLLegacyUUIDColumn(ctx, db, col), "%s.%s", col.table, col.column)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			databaseName := uniqueSchemaAssertionsName("uuid_mysql_converge")
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
			ids := seedLegacyMySQLUUIDRows(t, db, "mysql-"+tc.name)
			tc.partial(t, ctx, db)
			require.NoError(t, ensurePreMigrationTable(ctx, db, dialectMySQL))
			require.NoError(t, db.WithContext(ctx).
				Where("name = ?", "uuid_binary_conversion").
				Delete(&core.PreMigration{}).Error)

			require.NoError(t, NewGormStorage(db).Migrate(ctx), "partial MySQL UUID conversion state must converge")
			assertMySQLUUIDConversionConverged(t, ctx, db, ids)

			capture := &stmtCaptureLogger{Interface: logger.Default.LogMode(logger.Silent)}
			require.NoError(t, NewGormStorage(db.Session(&gorm.Session{Logger: capture})).Migrate(ctx), "final Migrate must be idempotent")
			for _, stmt := range capture.stmts {
				require.Falsef(t, strings.HasPrefix(strings.ToLower(strings.TrimSpace(stmt)), "alter "),
					"steady-state MySQL UUID conversion must issue 0 ALTERs: %s", stmt)
			}
		})
	}
}

func createMatchingSQLiteMySQLConvertedCompositeIndexes(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, spec := range mysqlConvertedCompositeIndexes {
		unique := ""
		if spec.unique {
			unique = "UNIQUE "
		}
		require.NoError(t, db.Exec(
			"CREATE "+unique+"INDEX "+spec.name+" ON "+spec.table+" ("+strings.Join(spec.columns, ", ")+")",
		).Error)
	}
}

func seedLegacyMySQLUUIDRows(t *testing.T, db *gorm.DB, prefix string) map[string]core.UUID {
	t.Helper()

	createLegacyMySQLUUIDSchema(t, db)
	now := time.Now().UTC().Truncate(time.Second)
	ids := map[string]core.UUID{
		"root":        testUUID(prefix + "-root"),
		"child":       testUUID(prefix + "-child"),
		"fan_out":     testUUID(prefix + "-fanout"),
		"checkpoint":  testUUID(prefix + "-checkpoint"),
		"signal":      testUUID(prefix + "-signal"),
		"held_slot":   testUUID(prefix + "-held-slot"),
		"unique_lock": testUUID(prefix + "-unique-lock"),
	}

	require.NoError(t, db.Exec(`
		INSERT INTO jobs (
			id, type, args, queue, tenant, metadata, priority, status, previous_status,
			attempt, max_retries, timeout, determinism, last_error, dead_letter_reason,
			created_at, updated_at, locked_by, unique_key, parent_job_id, root_job_id,
			fan_out_id, fan_out_index
		) VALUES
		(?, 'root', X'7B7D', 'default', '', '{}', 0, 'waiting', '', 0, 3, 0, 0, '', '', ?, ?, '', '', NULL, NULL, NULL, 0),
		(?, 'child', X'7B7D', 'default', '', '{}', 1, 'pending', '', 0, 3, 0, 0, '', '', ?, ?, '', '', ?, ?, ?, 0)
	`, ids["root"].String(), now, now, ids["child"].String(), now, now, ids["root"].String(), ids["root"].String(), ids["fan_out"].String()).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO fan_outs (
			id, parent_job_id, total_count, completed_count, failed_count, cancelled_count,
			strategy, threshold, status, cancel_on_fail, created_at, updated_at
		) VALUES (?, ?, 1, 0, 0, 0, 'fail_fast', 1.0, 'pending', false, ?, ?)
	`, ids["fan_out"].String(), ids["root"].String(), now, now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO checkpoints (
			id, job_id, call_index, call_type, result, error, error_kind, error_cause,
			error_delay_nanos, created_at
		) VALUES (?, ?, 0, 'call', X'01', '', '', '', 0, ?)
	`, ids["checkpoint"].String(), ids["child"].String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO signals (id, job_id, name, payload, created_at)
		VALUES (?, ?, 'ready', X'02', ?)
	`, ids["signal"].String(), ids["child"].String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at)
		VALUES ('sentinel', '', '', ?), ('held', ?, 'worker-1', ?)
	`, now, ids["held_slot"].String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
		VALUES ('abc123', ?, ?, ?)
	`, ids["unique_lock"].String(), now, now).Error)

	return ids
}

func createLegacyMySQLUUIDSchema(t *testing.T, db *gorm.DB) {
	t.Helper()

	stmts := []string{
		`
		CREATE TABLE jobs (
			id varchar(36) NOT NULL PRIMARY KEY,
			type varchar(255) NOT NULL DEFAULT '',
			args blob,
			queue varchar(255) NOT NULL DEFAULT 'default',
			tenant varchar(255) NULL,
			metadata json NULL,
			priority bigint NOT NULL DEFAULT 0,
			status varchar(20) NOT NULL DEFAULT 'pending',
			previous_status varchar(20) NULL,
			attempt bigint NOT NULL DEFAULT 0,
			max_retries bigint NOT NULL DEFAULT 3,
			timeout bigint NOT NULL DEFAULT 0,
			determinism bigint NOT NULL DEFAULT 0,
			last_error text NULL,
			dead_lettered_at datetime(6) NULL,
			dead_letter_reason text NULL,
			run_at datetime(6) NULL,
			started_at datetime(6) NULL,
			completed_at datetime(6) NULL,
			created_at datetime(6) NULL,
			updated_at datetime(6) NULL,
			locked_by varchar(255) NULL,
			locked_until datetime(6) NULL,
			last_heartbeat_at datetime(6) NULL,
			unique_key varchar(255) NULL,
			parent_job_id varchar(36) NULL,
			root_job_id varchar(36) NULL,
			fan_out_id varchar(36) NULL,
			fan_out_index bigint DEFAULT 0,
			result blob,
			trace_context blob
		)`,
		`
		CREATE TABLE fan_outs (
			id varchar(36) NOT NULL PRIMARY KEY,
			parent_job_id varchar(36) NOT NULL,
			total_count bigint NOT NULL,
			completed_count bigint DEFAULT 0,
			failed_count bigint DEFAULT 0,
			cancelled_count bigint DEFAULT 0,
			strategy varchar(20) DEFAULT 'fail_fast',
			threshold double DEFAULT 1.0,
			status varchar(20) DEFAULT 'pending',
			timeout_at datetime(6) NULL,
			cancel_on_fail boolean DEFAULT false,
			created_at datetime(6) NULL,
			updated_at datetime(6) NULL
		)`,
		`
		CREATE TABLE checkpoints (
			id varchar(36) NOT NULL PRIMARY KEY,
			job_id varchar(36) NOT NULL,
			call_index bigint NOT NULL,
			call_type varchar(255) NOT NULL,
			result blob,
			error text NULL,
			error_kind varchar(64) NULL,
			error_cause text NULL,
			error_delay_nanos bigint DEFAULT 0,
			created_at datetime(6) NULL
		)`,
		`
		CREATE TABLE signals (
			id varchar(36) NOT NULL PRIMARY KEY,
			job_id varchar(36) NOT NULL,
			name varchar(255) NOT NULL,
			payload blob,
			consumed_at datetime(6) NULL,
			created_at datetime(6) NULL
		)`,
		`
		CREATE TABLE concurrency_slots (
			slot_name varchar(255) NOT NULL,
			job_id varchar(36) NOT NULL,
			worker_id varchar(255) NULL,
			expires_at datetime(6) NULL,
			PRIMARY KEY (slot_name, job_id)
		)`,
		`
		CREATE TABLE unique_locks (
			scope_hash varchar(64) NOT NULL PRIMARY KEY,
			job_id varchar(36) NOT NULL,
			expires_at datetime(6) NOT NULL,
			created_at datetime(6) NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		require.NoError(t, db.Exec(stmt).Error)
	}
}

func assertMySQLUUIDConversionConverged(t *testing.T, ctx context.Context, db *gorm.DB, ids map[string]core.UUID) {
	t.Helper()

	for _, col := range legacyUUIDColumns {
		if !db.Migrator().HasTable(col.table) {
			continue
		}
		assertMySQLBinaryUUIDColumn(t, ctx, db, col.table, col.column)
	}
	requireMySQLPrimaryKey(t, ctx, db, "jobs")
	for _, name := range []string{"pending_parent_ref", "pending_root_ref"} {
		assertMySQLBinaryUUIDColumn(t, ctx, db, "jobs", name)
	}
	for _, spec := range mysqlConvertedCompositeIndexes {
		matches, err := mysqlConvertedCompositeIndexMatches(db, spec)
		require.NoError(t, err)
		require.Truef(t, matches, "%s.%s should be restored", spec.table, spec.name)
	}
	for _, fk := range []struct {
		table string
		name  string
	}{
		{table: "checkpoints", name: "fk_checkpoints_job"},
		{table: "fan_outs", name: "fk_fanouts_parent"},
		{table: "signals", name: "fk_signals_job"},
	} {
		requireMySQLForeignKey(t, ctx, db, fk.table, fk.name)
	}
	complete, err := mysqlUUIDBinaryConversionComplete(ctx, db)
	require.NoError(t, err)
	require.True(t, complete, "completion marker should be set")

	requireMySQLUUIDValue(t, ctx, db, "jobs", "id", "type = 'root'", ids["root"])
	requireMySQLUUIDValue(t, ctx, db, "jobs", "id", "type = 'child'", ids["child"])
	requireMySQLUUIDValue(t, ctx, db, "jobs", "parent_job_id", "type = 'child'", ids["root"])
	requireMySQLUUIDValue(t, ctx, db, "jobs", "root_job_id", "type = 'child'", ids["root"])
	requireMySQLUUIDValue(t, ctx, db, "jobs", "fan_out_id", "type = 'child'", ids["fan_out"])
	requireMySQLUUIDValue(t, ctx, db, "fan_outs", "id", "status = 'pending'", ids["fan_out"])
	requireMySQLUUIDValue(t, ctx, db, "fan_outs", "parent_job_id", "status = 'pending'", ids["root"])
	requireMySQLUUIDValue(t, ctx, db, "checkpoints", "id", "call_type = 'call'", ids["checkpoint"])
	requireMySQLUUIDValue(t, ctx, db, "checkpoints", "job_id", "call_type = 'call'", ids["child"])
	requireMySQLUUIDValue(t, ctx, db, "signals", "id", "name = 'ready'", ids["signal"])
	requireMySQLUUIDValue(t, ctx, db, "signals", "job_id", "name = 'ready'", ids["child"])
	requireMySQLUUIDValue(t, ctx, db, "concurrency_slots", "job_id", "slot_name = 'held'", ids["held_slot"])
	requireMySQLUUIDValue(t, ctx, db, "unique_locks", "job_id", "scope_hash = 'abc123'", ids["unique_lock"])
	requireMySQLUUIDValue(t, ctx, db, "concurrency_slots", "job_id", "slot_name = 'sentinel'", core.NilUUID)
}

func assertMySQLBinaryUUIDColumn(t *testing.T, ctx context.Context, db *gorm.DB, table, column string) {
	t.Helper()

	var row struct {
		DataType string `gorm:"column:DATA_TYPE"`
		Length   int64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
	}
	require.NoError(t, db.WithContext(ctx).Raw(`
		SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME = ?
	`, table, column).Scan(&row).Error)
	require.Equal(t, "binary", strings.ToLower(row.DataType), table+"."+column)
	require.Equal(t, int64(16), row.Length, table+"."+column)
}

func requireMySQLPrimaryKey(t *testing.T, ctx context.Context, db *gorm.DB, table string) {
	t.Helper()

	var count int64
	require.NoError(t, db.WithContext(ctx).Raw(`
		SELECT COUNT(*)
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND CONSTRAINT_TYPE = 'PRIMARY KEY'
	`, table).Scan(&count).Error)
	require.Equal(t, int64(1), count, table+" primary key")
}

func requireMySQLForeignKey(t *testing.T, ctx context.Context, db *gorm.DB, table, name string) {
	t.Helper()

	var count int64
	require.NoError(t, db.WithContext(ctx).Raw(`
		SELECT COUNT(*)
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND CONSTRAINT_NAME = ?
		  AND CONSTRAINT_TYPE = 'FOREIGN KEY'
	`, table, name).Scan(&count).Error)
	require.Equal(t, int64(1), count, table+"."+name)
}

func requireMySQLUUIDValue(t *testing.T, ctx context.Context, db *gorm.DB, table, column, where string, want core.UUID) {
	t.Helper()

	var got string
	require.NoError(t, db.WithContext(ctx).Raw(
		fmt.Sprintf("SELECT BIN_TO_UUID(%s) FROM %s WHERE %s LIMIT 1", column, table, where),
	).Scan(&got).Error)
	if strings.EqualFold(got, nilUUIDString) {
		got = core.NilUUID.String()
	}
	require.Equal(t, want.String(), strings.ToLower(got), table+"."+column)
}

func replacePreMigrationsForTest(t *testing.T, migrations []preMigration) func() {
	t.Helper()
	original := preMigrations
	preMigrations = migrations
	return func() {
		preMigrations = original
	}
}

func assertPreMigrationDone(t *testing.T, ctx context.Context, db *gorm.DB, name string) {
	t.Helper()

	var row core.PreMigration
	require.NoError(t, db.WithContext(ctx).First(&row, "name = ?", name).Error)
	require.NotNil(t, row.StartedAt, "pre-migration should have started_at")
	require.NotNil(t, row.CompletedAt, "pre-migration should have completed_at")
}

func seedLegacySQLiteUUIDRows(t *testing.T, db *gorm.DB, prefix string, now time.Time) map[string]core.UUID {
	t.Helper()

	rootID := testUUID(prefix + "-root")
	childID := testUUID(prefix + "-child")
	fanOutID := testUUID(prefix + "-fanout")
	checkpointID := testUUID(prefix + "-checkpoint")
	signalID := testUUID(prefix + "-signal")
	heldSlotJobID := testUUID(prefix + "-held-slot")
	uniqueLockJobID := testUUID(prefix + "-unique-lock")

	createLegacySQLiteUUIDSchema(t, db)
	require.NoError(t, db.Exec(`
		INSERT INTO jobs (
			id, type, args, queue, tenant, metadata, priority, status, previous_status,
			attempt, max_retries, timeout, determinism, last_error, dead_letter_reason,
			created_at, updated_at, locked_by, unique_key, parent_job_id, root_job_id,
			fan_out_id, fan_out_index
		) VALUES
		(?, 'root', X'7B7D', 'default', '', '{}', 0, 'waiting', '', 0, 3, 0, 0, '', '', ?, ?, '', '', NULL, NULL, NULL, 0),
		(?, 'child', X'7B7D', 'default', '', '{}', 1, 'pending', '', 0, 3, 0, 0, '', '', ?, ?, '', '', ?, ?, ?, 0)
	`, rootID.String(), now, now, childID.String(), now, now, rootID.String(), rootID.String(), fanOutID.String()).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO fan_outs (
			id, parent_job_id, total_count, completed_count, failed_count, cancelled_count,
			strategy, threshold, status, cancel_on_fail, created_at, updated_at
		) VALUES (?, ?, 1, 0, 0, 0, 'fail_fast', 1.0, 'pending', false, ?, ?)
	`, fanOutID.String(), rootID.String(), now, now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO checkpoints (
			id, job_id, call_index, call_type, result, error, error_kind, error_cause,
			error_delay_nanos, created_at
		) VALUES (?, ?, 0, 'call', X'01', '', '', '', 0, ?)
	`, checkpointID.String(), childID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO signals (id, job_id, name, payload, created_at)
		VALUES (?, ?, 'ready', X'02', ?)
	`, signalID.String(), childID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at)
		VALUES ('sentinel', '', '', ?), ('held', ?, 'worker-1', ?)
	`, now, heldSlotJobID.String(), now).Error)
	require.NoError(t, db.Exec(`
		INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
		VALUES ('abc123', ?, ?, ?)
	`, uniqueLockJobID.String(), now, now).Error)

	return map[string]core.UUID{
		"root":       rootID,
		"child":      childID,
		"fan_out":    fanOutID,
		"checkpoint": checkpointID,
		"signal":     signalID,
		"held_slot":  heldSlotJobID,
		"unique":     uniqueLockJobID,
	}
}

func createLegacySQLiteUUIDSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	stmts := []string{
		`
		CREATE TABLE jobs (
			` + "`id`" + ` TEXT PRIMARY KEY,
			` + "`type`" + ` TEXT,
			` + "`args`" + ` BLOB,
			` + "`queue`" + ` TEXT DEFAULT 'default',
			` + "`tenant`" + ` TEXT,
			` + "`metadata`" + ` TEXT,
			` + "`priority`" + ` INTEGER NOT NULL DEFAULT 0,
			` + "`status`" + ` TEXT NOT NULL DEFAULT 'pending',
			` + "`previous_status`" + ` TEXT,
			` + "`attempt`" + ` INTEGER NOT NULL DEFAULT 0,
			` + "`max_retries`" + ` INTEGER NOT NULL DEFAULT 3,
			` + "`timeout`" + ` INTEGER NOT NULL DEFAULT 0,
			` + "`determinism`" + ` INTEGER NOT NULL DEFAULT 0,
			` + "`last_error`" + ` TEXT,
			` + "`dead_lettered_at`" + ` datetime,
			` + "`dead_letter_reason`" + ` TEXT,
			` + "`run_at`" + ` datetime,
			` + "`started_at`" + ` datetime,
			` + "`completed_at`" + ` datetime,
			` + "`created_at`" + ` datetime,
			` + "`updated_at`" + ` datetime,
			` + "`locked_by`" + ` TEXT,
			` + "`locked_until`" + ` datetime,
			` + "`last_heartbeat_at`" + ` datetime,
			` + "`unique_key`" + ` TEXT,
			` + "`parent_job_id`" + ` TEXT,
			` + "`root_job_id`" + ` TEXT,
			` + "`fan_out_id`" + ` TEXT,
			` + "`fan_out_index`" + ` INTEGER DEFAULT 0,
			` + "`result`" + ` BLOB,
			` + "`trace_context`" + ` BLOB
		)`,
		`
		CREATE TABLE fan_outs (
			` + "`id`" + ` TEXT PRIMARY KEY,
			` + "`parent_job_id`" + ` TEXT NOT NULL,
			` + "`total_count`" + ` INTEGER NOT NULL,
			` + "`completed_count`" + ` INTEGER DEFAULT 0,
			` + "`failed_count`" + ` INTEGER DEFAULT 0,
			` + "`cancelled_count`" + ` INTEGER DEFAULT 0,
			` + "`strategy`" + ` TEXT DEFAULT 'fail_fast',
			` + "`threshold`" + ` REAL DEFAULT 1.0,
			` + "`status`" + ` TEXT DEFAULT 'pending',
			` + "`timeout_at`" + ` datetime,
			` + "`cancel_on_fail`" + ` boolean DEFAULT false,
			` + "`created_at`" + ` datetime,
			` + "`updated_at`" + ` datetime
		)`,
		`
		CREATE TABLE checkpoints (
			` + "`id`" + ` TEXT PRIMARY KEY,
			` + "`job_id`" + ` TEXT NOT NULL,
			` + "`call_index`" + ` INTEGER NOT NULL,
			` + "`call_type`" + ` TEXT NOT NULL,
			` + "`result`" + ` BLOB,
			` + "`error`" + ` TEXT,
			` + "`error_kind`" + ` TEXT,
			` + "`error_cause`" + ` TEXT,
			` + "`error_delay_nanos`" + ` INTEGER DEFAULT 0,
			` + "`created_at`" + ` datetime
		)`,
		`
		CREATE TABLE signals (
			` + "`id`" + ` TEXT PRIMARY KEY,
			` + "`job_id`" + ` TEXT NOT NULL,
			` + "`name`" + ` TEXT NOT NULL,
			` + "`payload`" + ` BLOB,
			` + "`consumed_at`" + ` datetime,
			` + "`created_at`" + ` datetime
		)`,
		`
		CREATE TABLE concurrency_slots (
			` + "`slot_name`" + ` TEXT NOT NULL,
			` + "`job_id`" + ` TEXT NOT NULL,
			` + "`worker_id`" + ` TEXT,
			` + "`expires_at`" + ` datetime,
			PRIMARY KEY (` + "`slot_name`" + `,` + "`job_id`" + `)
		)`,
		`
		CREATE TABLE unique_locks (
			` + "`scope_hash`" + ` TEXT PRIMARY KEY,
			` + "`job_id`" + ` TEXT NOT NULL,
			` + "`expires_at`" + ` datetime NOT NULL,
			` + "`created_at`" + ` datetime NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		require.NoError(t, db.Exec(stmt).Error)
	}
}

func assertSQLiteIndexColumns(t *testing.T, db *gorm.DB, table, index string, want []string) {
	t.Helper()

	type indexColumn struct {
		Seqno int    `gorm:"column:seqno"`
		Name  string `gorm:"column:name"`
	}
	var rows []indexColumn
	require.NoError(t, db.Raw("PRAGMA index_info("+index+")").Scan(&rows).Error)
	require.Len(t, rows, len(want), table+"."+index)

	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.Name)
	}
	assert.Equal(t, want, got, table+"."+index)
}

func assertSQLiteIndexUnique(t *testing.T, db *gorm.DB, table, index string, want bool) {
	t.Helper()

	type indexListRow struct {
		Name   string `gorm:"column:name"`
		Unique int    `gorm:"column:unique"`
	}
	var rows []indexListRow
	require.NoError(t, db.Raw("PRAGMA index_list("+table+")").Scan(&rows).Error)
	for _, row := range rows {
		if row.Name == index {
			assert.Equal(t, want, row.Unique == 1, table+"."+index)
			return
		}
	}
	require.Failf(t, "index not found", "%s.%s", table, index)
}

func assertLegacyUUIDRowsConverted(t *testing.T, db *gorm.DB, ids map[string]core.UUID) {
	t.Helper()

	var root, child core.Job
	require.NoError(t, db.First(&root, "id = ?", ids["root"]).Error)
	require.NoError(t, db.First(&child, "id = ?", ids["child"]).Error)
	assert.Equal(t, ids["root"], root.ID)
	assert.Equal(t, ids["child"], child.ID)
	require.NotNil(t, child.ParentJobID)
	require.NotNil(t, child.RootJobID)
	require.NotNil(t, child.FanOutID)
	assert.Equal(t, ids["root"], *child.ParentJobID)
	assert.Equal(t, ids["root"], *child.RootJobID)
	assert.Equal(t, ids["fan_out"], *child.FanOutID)

	var fanOut core.FanOut
	require.NoError(t, db.First(&fanOut, "id = ?", ids["fan_out"]).Error)
	assert.Equal(t, ids["fan_out"], fanOut.ID)
	assert.Equal(t, ids["root"], fanOut.ParentJobID)

	var checkpoint core.Checkpoint
	require.NoError(t, db.First(&checkpoint, "id = ?", ids["checkpoint"]).Error)
	assert.Equal(t, ids["checkpoint"], checkpoint.ID)
	assert.Equal(t, ids["child"], checkpoint.JobID)

	var signal core.Signal
	require.NoError(t, db.First(&signal, "id = ?", ids["signal"]).Error)
	assert.Equal(t, ids["signal"], signal.ID)
	assert.Equal(t, ids["child"], signal.JobID)

	var sentinel core.ConcurrencySlot
	require.NoError(t, db.First(&sentinel, "slot_name = ?", "sentinel").Error)
	assert.Equal(t, core.NilUUID, sentinel.JobID)
	var held core.ConcurrencySlot
	require.NoError(t, db.First(&held, "slot_name = ?", "held").Error)
	assert.Equal(t, ids["held_slot"], held.JobID)

	var unique core.UniqueLock
	require.NoError(t, db.First(&unique, "scope_hash = ?", "abc123").Error)
	assert.Equal(t, ids["unique"], unique.JobID)

	for _, orphanCheck := range []struct {
		name  string
		query string
	}{
		{name: "checkpoints", query: "SELECT COUNT(*) FROM checkpoints c LEFT JOIN jobs j ON j.id = c.job_id WHERE j.id IS NULL"},
		{name: "signals", query: "SELECT COUNT(*) FROM signals s LEFT JOIN jobs j ON j.id = s.job_id WHERE j.id IS NULL"},
		{name: "fan_outs", query: "SELECT COUNT(*) FROM fan_outs f LEFT JOIN jobs j ON j.id = f.parent_job_id WHERE j.id IS NULL"},
	} {
		var orphans int64
		require.NoError(t, db.Raw(orphanCheck.query).Scan(&orphans).Error)
		assert.Zero(t, orphans, orphanCheck.name)
	}

	for _, probe := range []struct {
		table string
		col   string
		where string
	}{
		{table: "jobs", col: "id", where: "id IS NOT NULL"},
		{table: "jobs", col: "parent_job_id", where: "parent_job_id IS NOT NULL"},
		{table: "fan_outs", col: "id", where: "1 = 1"},
		{table: "checkpoints", col: "job_id", where: "1 = 1"},
		{table: "signals", col: "job_id", where: "1 = 1"},
		{table: "concurrency_slots", col: "job_id", where: "slot_name = 'sentinel'"},
		{table: "unique_locks", col: "job_id", where: "scope_hash = 'abc123'"},
	} {
		var typ string
		require.NoError(t, db.Raw("SELECT typeof("+probe.col+") FROM "+probe.table+" WHERE "+probe.where+" LIMIT 1").Scan(&typ).Error)
		assert.Equal(t, "blob", typ, probe.table+"."+probe.col)
	}
}
