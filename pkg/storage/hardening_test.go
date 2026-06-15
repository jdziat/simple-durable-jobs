package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// TestIsBenignDDLError covers the MySQL "already applied by a concurrent worker"
// error classifier directly — it only fires on a real DDL race otherwise.
func TestIsBenignDDLError(t *testing.T) {
	for _, c := range []struct {
		msg  string
		want bool
	}{
		{"Error 1061 (42000): Duplicate key name 'idx_jobs_dequeue'", true},
		{"Duplicate key name 'idx'", true},
		{"Error 1060 (42S21): Duplicate column name 'active_unique_key'", true},
		{"Duplicate column name 'x'", true},
		{"Error 1091 (42000): Can't DROP 'idx'; check that column/key exists", true},
		{"check that column/key exists", true},
		{"Error 1826 (HY000): Duplicate foreign key constraint name 'fk_checkpoints_job'", true},
		{"Duplicate foreign key constraint name 'fk'", true},
		{"Error 3822 (HY000): Duplicate check constraint name 'chk_jobs_status'", true},
		{"Duplicate check constraint name 'chk'", true},
		{"Error 1146: Table doesn't exist", false},
		{"some unrelated error", false},
		{"", false},
	} {
		var err error
		if c.msg != "" {
			err = errors.New(c.msg)
		}
		assert.Equalf(t, c.want, isBenignDDLError(err), "msg=%q", c.msg)
	}
}

// TestRunMigrations_ConcurrentSafe simulates a fleet calling Migrate() at once
// against a not-yet-migrated database. Before the concurrency fix this crashed
// most callers (index DROP/CREATE and ledger-INSERT races); it must now return
// no error from any caller and leave a correct ledger. Meaningful only on the
// multi-worker backends — in-memory SQLite gives each pooled connection its own
// database.
func TestRunMigrations_ConcurrentSafe(t *testing.T) {
	s := newTestStorage(t)
	if s.isSQLite {
		t.Skip("in-memory SQLite gives each pooled connection its own DB; concurrency is meaningful only on Postgres/MySQL")
	}
	ctx := context.Background()

	// Simulate "not yet migrated" so a concurrent Migrate has real work to race
	// on: drop the ledger and the reworked index.
	require.NoError(t, s.db.Migrator().DropTable(&core.SchemaMigration{}))
	_ = s.db.Migrator().DropIndex(&core.Job{}, "idx_jobs_dequeue")

	// Deliberately SMALL pool: the migration work runs on the lock-holding
	// connection, so far more concurrent callers than connections must still
	// serialize cleanly and NOT deadlock (an earlier fix held the lock on a
	// dedicated conn and deadlocked here).
	const workers = 6
	if sqlDB, err := s.db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(2)
	}

	var wg sync.WaitGroup
	errs := make([]error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = s.Migrate(ctx) // full path, fleet-lock-serialized
		}(i)
	}
	close(start) // release all goroutines simultaneously
	wg.Wait()

	for i, err := range errs {
		assert.NoErrorf(t, err, "concurrent Migrate worker %d must not error", i)
	}

	var versions []int
	require.NoError(t, s.db.Model(&core.SchemaMigration{}).Order("version").Pluck("version", &versions).Error)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}, versions, "every migration recorded exactly once")
	assert.False(t, s.db.Migrator().HasIndex(&core.Job{}, "idx_jobs_dequeue"), "redundant dequeue index absent after v12")

	// Pathological single-connection pool must not deadlock (lock + work share
	// the one connection). Guard with a deadline so a regression fails fast.
	if sqlDB, err := s.db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	require.NoError(t, s.db.Migrator().DropTable(&core.SchemaMigration{}))
	done := make(chan error, 1)
	go func() { done <- s.Migrate(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err, "Migrate at MaxOpenConns=1 must succeed")
	case <-time.After(30 * time.Second):
		t.Fatal("Migrate deadlocked at MaxOpenConns=1")
	}
}

func TestForbidNullFanOutCountersMigration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" && os.Getenv("TEST_MYSQL_URL") == "" {
		t.Skip("TEST_DATABASE_URL/TEST_MYSQL_URL not set")
	}

	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		t.Run("postgres", func(t *testing.T) {
			schemaName := uniqueSchemaAssertionsName("fanout_counters")

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

			requireForbidNullFanOutCountersMigration(t, NewGormStorage(db), db, dialectPostgres)
		})
	}

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		t.Run("mysql", func(t *testing.T) {
			databaseName := uniqueSchemaAssertionsName("fanout_counters")

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

			requireForbidNullFanOutCountersMigration(t, NewGormStorage(db), db, dialectMySQL)
		})
	}
}

func requireForbidNullFanOutCountersMigration(t *testing.T, s *GormStorage, db *gorm.DB, dialect string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.Migrate(ctx))
	require.NoError(t, s.Migrate(ctx), "second Migrate must be idempotent")

	require.NoError(t, insertFanOutIndexCheckJob(ctx, db, schemaCheckID("fanout-index-zero"), 0))
	require.NoError(t, insertFanOutCounterCheckFanOut(ctx, db,
		schemaCheckID("counter-parent-valid"), schemaCheckID("counter-valid"), 2, 1, 0, 1,
	))

	require.Error(t, insertFanOutIndexCheckJob(ctx, db, schemaCheckID("fanout-index-null"), nil), "NULL fan_out_index must be rejected")
	require.Error(t, insertFanOutCounterCheckFanOut(ctx, db,
		schemaCheckID("counter-parent-null"), schemaCheckID("counter-null"), 1, nil, 0, 0,
	), "NULL fan-out counter must be rejected")

	require.NoError(t, dropFanOutCounterNullChecks(ctx, db, dialect))
	require.NoError(t, db.WithContext(ctx).Delete(&core.SchemaMigration{}, "version = ?", 31).Error)

	precleanJobID := schemaCheckID("fanout-index-preclean")
	precleanFanOutID := schemaCheckID("counter-preclean")
	require.NoError(t, insertFanOutIndexCheckJob(ctx, db, precleanJobID, nil))
	require.NoError(t, insertFanOutCounterCheckFanOut(ctx, db,
		schemaCheckID("counter-parent-preclean"), precleanFanOutID, 3, nil, nil, nil,
	))

	require.NoError(t, s.Migrate(ctx), "v31 must pre-clean NULL counters before adding CHECK constraints")

	var fanOutIndex sql.NullInt64
	require.NoError(t, db.WithContext(ctx).Raw(
		"SELECT fan_out_index FROM jobs WHERE id = ?", precleanJobID,
	).Scan(&fanOutIndex).Error)
	require.True(t, fanOutIndex.Valid)
	require.Equal(t, int64(0), fanOutIndex.Int64)

	var counts struct {
		Completed sql.NullInt64
		Failed    sql.NullInt64
		Cancelled sql.NullInt64
	}
	require.NoError(t, db.WithContext(ctx).Raw(`
		SELECT completed_count AS completed,
		       failed_count AS failed,
		       cancelled_count AS cancelled
		FROM fan_outs
		WHERE id = ?
	`, precleanFanOutID).Scan(&counts).Error)
	require.True(t, counts.Completed.Valid)
	require.True(t, counts.Failed.Valid)
	require.True(t, counts.Cancelled.Valid)
	require.Equal(t, int64(0), counts.Completed.Int64)
	require.Equal(t, int64(0), counts.Failed.Int64)
	require.Equal(t, int64(0), counts.Cancelled.Int64)
}

func insertFanOutIndexCheckJob(ctx context.Context, db *gorm.DB, id core.UUID, fanOutIndex any) error {
	return db.WithContext(ctx).Exec(`
		INSERT INTO jobs (id, type, status, queue, priority, attempt, max_retries, timeout, determinism, fan_out_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, "schema.fan_out_index", core.StatusPending, "default", 0, 0, 3, 0, 0, fanOutIndex).Error
}

func insertFanOutCounterCheckFanOut(
	ctx context.Context,
	db *gorm.DB,
	parentID core.UUID,
	fanOutID core.UUID,
	totalCount int,
	completedCount any,
	failedCount any,
	cancelledCount any,
) error {
	if err := insertFanOutIndexCheckJob(ctx, db, parentID, 0); err != nil {
		return err
	}
	return db.WithContext(ctx).Exec(`
		INSERT INTO fan_outs (
			id, parent_job_id, total_count, completed_count, failed_count, cancelled_count, strategy, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, fanOutID, parentID, totalCount, completedCount, failedCount, cancelledCount, core.StrategyFailFast, core.FanOutPending).Error
}

func dropFanOutCounterNullChecks(ctx context.Context, db *gorm.DB, dialect string) error {
	checks := []struct {
		table string
		name  string
	}{
		{table: "jobs", name: "chk_jobs_fan_out_index_not_null"},
		{table: "fan_outs", name: "chk_fan_outs_counts_not_null"},
	}
	for _, check := range checks {
		var err error
		switch dialect {
		case dialectPostgres:
			err = db.WithContext(ctx).Exec("ALTER TABLE " + check.table + " DROP CONSTRAINT IF EXISTS " + check.name).Error
		case dialectMySQL:
			err = db.WithContext(ctx).Exec("ALTER TABLE " + check.table + " DROP CHECK " + check.name).Error
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// TestRequeue_FanOutHandling verifies Requeue's fan-out rules: a sub-job is
// rejected (would double-count its parent), and requeuing a parent clears its
// old fan-out batch so the replay re-dispatches cleanly.
func TestRequeue_FanOutHandling(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// Parent → fo-1 → {sub-0, sub-1}; sub-0 is itself a nested fan-out parent
	// → fo-2 → {grand-0, grand-1}. Requeuing the parent must clear the WHOLE
	// subtree, including the grandchildren and their checkpoints.
	parentID := core.NewID()
	foID := core.NewID()
	subIDs := []core.UUID{core.NewID(), core.NewID()}
	fo2 := core.NewID()
	grandIDs := []core.UUID{core.NewID(), core.NewID()}
	otherID := core.NewID()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: parentID, Type: "wf", Queue: "default", Status: core.StatusFailed,
	}).Error)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID: foID, ParentJobID: parentID, TotalCount: 2, CompletedCount: 1, FailedCount: 1,
	}))
	for i, st := range []core.JobStatus{core.StatusCompleted, core.StatusFailed} {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: subIDs[i], Type: "wf.sub", Queue: "default",
			Status: st, FanOutID: &foID, FanOutIndex: i,
		}).Error)
	}
	// Nested level under sub-0.
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID: fo2, ParentJobID: subIDs[0], TotalCount: 2, CompletedCount: 2,
	}))
	// grand-0 failed (so the sub-job-rejection path is reachable), grand-1 done.
	for i, st := range []core.JobStatus{core.StatusFailed, core.StatusCompleted} {
		gid := grandIDs[i]
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: gid, Type: "wf.grand", Queue: "default",
			Status: st, FanOutID: &fo2, FanOutIndex: i,
		}).Error)
		require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
			JobID: gid, CallIndex: 0, CallType: "g", Result: []byte(`"ok"`),
		}))
	}

	// An unrelated parent that must be left untouched.
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: otherID, Type: "wf", Queue: "default", Status: core.StatusFailed,
	}).Error)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{ID: core.NewID(), ParentJobID: otherID, TotalCount: 1}))

	// A sub-job cannot be requeued directly.
	_, err := s.Requeue(ctx, subIDs[1])
	require.ErrorIs(t, err, core.ErrCannotRequeueSubJob)
	_, err = s.Requeue(ctx, grandIDs[0])
	require.ErrorIs(t, err, core.ErrCannotRequeueSubJob)

	// Requeuing the parent resets it and clears the whole fan-out subtree.
	requeued, err := s.Requeue(ctx, parentID)
	require.NoError(t, err)
	assert.True(t, requeued)

	parent, err := s.GetJob(ctx, parentID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, parent.Status)

	fos, err := s.GetFanOutsByParent(ctx, parentID)
	require.NoError(t, err)
	assert.Empty(t, fos, "level-1 fan-out records cleared")
	subs, err := s.GetSubJobs(ctx, foID)
	require.NoError(t, err)
	assert.Empty(t, subs, "level-1 sub-jobs cleared")

	// Nested level cleared too.
	nestedFOs, err := s.GetFanOutsByParent(ctx, subIDs[0])
	require.NoError(t, err)
	assert.Empty(t, nestedFOs, "nested fan-out records cleared")
	grands, err := s.GetSubJobs(ctx, fo2)
	require.NoError(t, err)
	assert.Empty(t, grands, "grandchild sub-jobs cleared")
	for i := 0; i < 2; i++ {
		gcps, err := s.GetCheckpoints(ctx, grandIDs[i])
		require.NoError(t, err)
		assert.Empty(t, gcps, "grandchild checkpoints cleared")
	}

	// The unrelated parent is untouched.
	otherFOs, err := s.GetFanOutsByParent(ctx, otherID)
	require.NoError(t, err)
	assert.Len(t, otherFOs, 1, "unrelated parent's fan-out must be left intact")
}

// TestRequeue_ClearsCheckpoints verifies Requeue resets the job AND drops its
// checkpoints (replay-from-scratch), so a requeued workflow can't resume into
// stale steps or false-trip Strict determinism.
func TestRequeue_ClearsCheckpoints(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	jobID := core.NewID()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: jobID, Type: "wf", Queue: "default", Status: core.StatusFailed, Attempt: 3, LastError: "boom",
	}).Error)
	for i := 0; i < 3; i++ {
		require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
			JobID: jobID, CallIndex: i, CallType: fmt.Sprintf("step-%d", i), Result: []byte(`"ok"`),
		}))
	}

	requeued, err := s.Requeue(ctx, jobID)
	require.NoError(t, err)
	assert.True(t, requeued)

	got, err := s.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, got.Status)
	assert.Equal(t, 0, got.Attempt)

	cps, err := s.GetCheckpoints(ctx, jobID)
	require.NoError(t, err)
	assert.Empty(t, cps, "Requeue should clear checkpoints for a fresh replay")

	// A non-terminal / missing job is not requeued.
	missing, err := s.Requeue(ctx, core.NewID())
	require.NoError(t, err)
	assert.False(t, missing)
}

// TestEnqueue_UniqueKeyDedup_AllBackends verifies that a plain Enqueue with a
// unique key dedupes identically on every backend. This is the MySQL parity fix
// (M4): before the generated active_unique_key column, MySQL silently accepted a
// duplicate because the ON CONFLICT target index did not exist there.
func TestEnqueue_UniqueKeyDedup_AllBackends(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	first := &core.Job{Type: "dedup", UniqueKey: "only-one"}
	require.NoError(t, s.Enqueue(ctx, first))

	second := &core.Job{Type: "dedup", UniqueKey: "only-one"}
	err := s.Enqueue(ctx, second)
	require.ErrorIs(t, err, core.ErrDuplicateJob, "second active job with same unique key must be rejected")

	// Once the first reaches a terminal status, the key frees up and a new job
	// with the same key is allowed (the active-unique semantics).
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", first.ID).Update("status", core.StatusCompleted).Error)

	third := &core.Job{Type: "dedup", UniqueKey: "only-one"}
	require.NoError(t, s.Enqueue(ctx, third), "key should be reusable after the holder is terminal")
}

// TestTryAcquireRecoveryLease verifies single-holder election, owner renewal,
// and failover after expiry (the H4 recovery-lease mechanism).
func TestTryAcquireRecoveryLease(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	const lease = "fanout-recovery"

	got, err := s.TryAcquireRecoveryLease(ctx, lease, "worker-A", time.Hour)
	require.NoError(t, err)
	assert.True(t, got, "first acquirer takes the lease")

	got, err = s.TryAcquireRecoveryLease(ctx, lease, "worker-B", time.Hour)
	require.NoError(t, err)
	assert.False(t, got, "a second worker cannot take a live lease")

	got, err = s.TryAcquireRecoveryLease(ctx, lease, "worker-A", time.Hour)
	require.NoError(t, err)
	assert.True(t, got, "the holder renews its own lease")

	// Failover after expiry: take a short-lived lease, let it lapse, then a
	// different worker must be able to take it over.
	const short = "fanout-recovery-short"
	got, err = s.TryAcquireRecoveryLease(ctx, short, "worker-A", 50*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, got)

	time.Sleep(150 * time.Millisecond)

	got, err = s.TryAcquireRecoveryLease(ctx, short, "worker-B", time.Hour)
	require.NoError(t, err)
	assert.True(t, got, "an expired lease can be taken over by another worker")
}

// TestSeedScheduledFire_InsertIfAbsent verifies the shared-anchor seeding the
// scheduler uses to prevent first-fire double-firing (H2): the first seed wins
// and a later seed is a no-op, so all workers read the same base.
func TestSeedScheduledFire_InsertIfAbsent(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	require.NoError(t, s.DB().Where("name = ?", "sched-A").Delete(&core.ScheduledFire{}).Error)

	base := time.Now().UTC().Truncate(time.Second)
	got, err := s.SeedScheduledFire(ctx, "sched-A", base)
	require.NoError(t, err)
	assert.WithinDuration(t, base, got, time.Second, "first seed becomes the anchor")

	var seeded core.ScheduledFire
	require.NoError(t, s.DB().First(&seeded, "name = ?", "sched-A").Error)
	assert.Nil(t, seeded.LastFiredAt, "seeding establishes only a boundary cursor, not a real fire")

	// A later seed with a different anchor must NOT advance the boundary.
	later := base.Add(10 * time.Second)
	got2, err := s.SeedScheduledFire(ctx, "sched-A", later)
	require.NoError(t, err)
	assert.WithinDuration(t, base, got2, time.Second, "subsequent seed is a no-op (insert-if-absent)")

	claimed, err := s.ClaimScheduledFire(ctx, "sched-A", later)
	require.NoError(t, err)
	require.True(t, claimed)
	var claimedFire core.ScheduledFire
	require.NoError(t, s.DB().First(&claimedFire, "name = ?", "sched-A").Error)
	require.NotNil(t, claimedFire.LastFiredAt)
	assert.True(t, claimedFire.LastFiredAt.Equal(later), "successful claim records the real fire time")
}

// TestSaveCheckpoint_PersistsErrorCause verifies the error_cause column is
// persisted and round-trips (M1), so error replay no longer relies on parsing
// the formatted message prefix.
func TestSaveCheckpoint_PersistsErrorCause(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	seedTestJob(t, ctx, s, testUUID("job-1"), core.StatusRunning)

	orig := core.NoRetry(assertErr("payment declined"))
	message, cause, kind, delay := core.CheckpointErrorFields(orig)

	cp := &core.Checkpoint{
		JobID:           testUUID("job-1"),
		CallIndex:       0,
		CallType:        "charge",
		Error:           message,
		ErrorCause:      cause,
		ErrorKind:       kind,
		ErrorDelayNanos: int64(delay),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp))

	loaded, err := s.GetCheckpoints(ctx, testUUID("job-1"))
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "payment declined", loaded[0].ErrorCause)
	assert.Equal(t, core.CheckpointErrorKindNoRetry, loaded[0].ErrorKind)

	rebuilt := core.RehydrateCheckpointErrorWithCause(
		loaded[0].Error, loaded[0].ErrorCause, loaded[0].ErrorKind,
		time.Duration(loaded[0].ErrorDelayNanos),
	)
	var noRetry *core.NoRetryError
	require.ErrorAs(t, rebuilt, &noRetry)
	assert.Equal(t, "payment declined", noRetry.Err.Error())
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
