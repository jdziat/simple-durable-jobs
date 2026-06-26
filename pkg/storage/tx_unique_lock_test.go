package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Covers EnqueueWithUniqueLockTx, the caller-transaction form of the windowed
// (lease-based) unique-lock enqueue. The lock/steal/dedup logic itself is
// exercised via the non-tx EnqueueWithUniqueLock in unique_lock_test.go; here we
// pin the Tx wrapper's argument guard and that it honors the supplied tx
// (commit visibility + cross-tx dedup within the window).

func countAllJobs(t *testing.T, ctx context.Context, s *GormStorage) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).Count(&n).Error)
	return n
}

func TestEnqueueWithUniqueLockTx_EmptyScopeHashErrors(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	tx := s.DB().Begin()
	defer tx.Rollback()

	_, err := s.EnqueueWithUniqueLockTx(ctx, tx, newTestJob("default", "tx.lock"), "", time.Minute)
	require.ErrorIs(t, err, core.ErrStorageNoUniqueLocks)
}

func TestEnqueueWithUniqueLockTx_NonPositiveTTLErrors(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	tx := s.DB().Begin()
	defer tx.Rollback()

	_, err := s.EnqueueWithUniqueLockTx(ctx, tx, newTestJob("default", "tx.lock"), "scope-a", 0)
	require.ErrorIs(t, err, core.ErrStorageNoUniqueLocks)
}

func TestEnqueueWithUniqueLockTx_CommitsAndDedupesWithinWindow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	const scope = "scope-dedupe"

	// First enqueue under the lock, committed.
	tx1 := s.DB().Begin()
	id1, err := s.EnqueueWithUniqueLockTx(ctx, tx1, newTestJob("default", "tx.lock"), scope, time.Minute)
	require.NoError(t, err)
	require.NotEqual(t, core.NilUUID, id1)
	require.NoError(t, tx1.Commit().Error)

	require.Equal(t, int64(1), countAllJobs(t, ctx, s), "first enqueue created a job")

	// Second enqueue under the same scope, inside its window: deduped to the
	// existing job id, no new row, no error (windowed dedup, not ErrDuplicateJob).
	tx2 := s.DB().Begin()
	id2, err := s.EnqueueWithUniqueLockTx(ctx, tx2, newTestJob("default", "tx.lock"), scope, time.Minute)
	require.NoError(t, err)
	require.NoError(t, tx2.Commit().Error)

	assert.Equal(t, id1, id2, "second enqueue within the window dedupes to the first job")
	assert.Equal(t, int64(1), countAllJobs(t, ctx, s), "no second job is created")
}

func TestEnqueueWithUniqueLockTx_RollbackDiscardsJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	tx := s.DB().Begin()
	_, err := s.EnqueueWithUniqueLockTx(ctx, tx, newTestJob("default", "tx.lock"), "scope-rollback", time.Minute)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback().Error)

	assert.Equal(t, int64(0), countAllJobs(t, ctx, s), "a rolled-back enqueue leaves no job")
}
