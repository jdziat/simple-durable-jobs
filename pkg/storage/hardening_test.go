package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

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
