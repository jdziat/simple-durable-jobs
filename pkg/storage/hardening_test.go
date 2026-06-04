package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

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
	assert.Equal(t, []int{1, 2}, versions, "every migration recorded exactly once")
	assert.True(t, s.db.Migrator().HasIndex(&core.Job{}, "idx_jobs_dequeue"), "reworked index present")

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

// TestRequeue_FanOutHandling verifies Requeue's fan-out rules: a sub-job is
// rejected (would double-count its parent), and requeuing a parent clears its
// old fan-out batch so the replay re-dispatches cleanly.
func TestRequeue_FanOutHandling(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// Parent + a fan-out batch with two terminal sub-jobs.
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: "parent", Type: "wf", Queue: "default", Status: core.StatusFailed,
	}).Error)
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID: "fo-1", ParentJobID: "parent", TotalCount: 2, CompletedCount: 1, FailedCount: 1,
	}))
	foID := "fo-1"
	for i, st := range []core.JobStatus{core.StatusCompleted, core.StatusFailed} {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: fmt.Sprintf("sub-%d", i), Type: "wf.sub", Queue: "default",
			Status: st, FanOutID: &foID, FanOutIndex: i,
		}).Error)
	}

	// A sub-job cannot be requeued directly.
	_, err := s.Requeue(ctx, "sub-1")
	require.ErrorIs(t, err, core.ErrCannotRequeueSubJob)

	// Requeuing the parent resets it and clears the old fan-out batch.
	requeued, err := s.Requeue(ctx, "parent")
	require.NoError(t, err)
	assert.True(t, requeued)

	parent, err := s.GetJob(ctx, "parent")
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, parent.Status)

	fos, err := s.GetFanOutsByParent(ctx, "parent")
	require.NoError(t, err)
	assert.Empty(t, fos, "old fan-out records cleared")
	subs, err := s.GetSubJobs(ctx, "fo-1")
	require.NoError(t, err)
	assert.Empty(t, subs, "old sub-jobs cleared")
}

// TestRequeue_ClearsCheckpoints verifies Requeue resets the job AND drops its
// checkpoints (replay-from-scratch), so a requeued workflow can't resume into
// stale steps or false-trip Strict determinism.
func TestRequeue_ClearsCheckpoints(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: "rq-1", Type: "wf", Queue: "default", Status: core.StatusFailed, Attempt: 3, LastError: "boom",
	}).Error)
	for i := 0; i < 3; i++ {
		require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
			JobID: "rq-1", CallIndex: i, CallType: fmt.Sprintf("step-%d", i), Result: []byte(`"ok"`),
		}))
	}

	requeued, err := s.Requeue(ctx, "rq-1")
	require.NoError(t, err)
	assert.True(t, requeued)

	got, err := s.GetJob(ctx, "rq-1")
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, got.Status)
	assert.Equal(t, 0, got.Attempt)

	cps, err := s.GetCheckpoints(ctx, "rq-1")
	require.NoError(t, err)
	assert.Empty(t, cps, "Requeue should clear checkpoints for a fresh replay")

	// A non-terminal / missing job is not requeued.
	missing, err := s.Requeue(ctx, "does-not-exist")
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

	base := time.Now().UTC().Truncate(time.Second)
	got, err := s.SeedScheduledFire(ctx, "sched-A", base)
	require.NoError(t, err)
	assert.WithinDuration(t, base, got, time.Second, "first seed becomes the anchor")

	// A later seed with a different anchor must NOT advance the boundary.
	later := base.Add(10 * time.Second)
	got2, err := s.SeedScheduledFire(ctx, "sched-A", later)
	require.NoError(t, err)
	assert.WithinDuration(t, base, got2, time.Second, "subsequent seed is a no-op (insert-if-absent)")
}

// TestSaveCheckpoint_PersistsErrorCause verifies the error_cause column is
// persisted and round-trips (M1), so error replay no longer relies on parsing
// the formatted message prefix.
func TestSaveCheckpoint_PersistsErrorCause(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	orig := core.NoRetry(assertErr("payment declined"))
	message, cause, kind, delay := core.CheckpointErrorFields(orig)

	cp := &core.Checkpoint{
		JobID:           "job-1",
		CallIndex:       0,
		CallType:        "charge",
		Error:           message,
		ErrorCause:      cause,
		ErrorKind:       kind,
		ErrorDelayNanos: int64(delay),
	}
	require.NoError(t, s.SaveCheckpoint(ctx, cp))

	loaded, err := s.GetCheckpoints(ctx, "job-1")
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
