package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// A job can carry BOTH a windowed dedup key (IdempotencyKey/UniqueFor, enforced by
// unique_locks) and an active-only one (queue.Unique, enforced by the partial
// unique index). Winning the windowed lock says nothing about the index, so the
// insert could still be refused — and it surfaced the RAW driver error, a
// dialect-specific 1062 / 23505 / "UNIQUE constraint failed" string, where the
// same collision on the ordinary enqueue path returns core.ErrDuplicateJob.
//
// A caller cannot errors.Is that, so a documented, expected condition looked like
// an unknown storage failure — and the natural reaction to an unknown storage
// failure is to retry it, forever.
func TestEnqueueWithUniqueLock_ActiveKeyCollisionReturnsErrDuplicateJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// First job: holds the ACTIVE unique key and is pending, so the partial index
	// covers it.
	first := &core.Job{
		ID: core.NewID(), Type: "wf", Queue: "default",
		Status: core.StatusPending, UniqueKey: "shared-active-key",
	}
	gotID, err := s.EnqueueWithUniqueLock(ctx, first, "window-scope-a", time.Hour)
	require.NoError(t, err)
	require.Equal(t, first.ID, gotID, "premise: the first job must actually be inserted")

	// Second job: a DIFFERENT windowed scope, so the lock is winnable, but the SAME
	// active unique key, so the partial index must refuse the insert.
	second := &core.Job{
		ID: core.NewID(), Type: "wf", Queue: "default",
		Status: core.StatusPending, UniqueKey: "shared-active-key",
	}
	_, err = s.EnqueueWithUniqueLock(ctx, second, "window-scope-b", time.Hour)

	require.ErrorIs(t, err, core.ErrDuplicateJob,
		"an active-unique-key collision must be reported as core.ErrDuplicateJob, the same as on the ordinary enqueue path; a raw driver error cannot be matched with errors.Is and reads as an unknown storage failure worth retrying forever (got: %v)", err)

	// And the refusal must not have inserted anything.
	var n int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", second.ID).Count(&n).Error)
	require.Equal(t, int64(0), n, "a refused insert must leave no row behind")
}

// The ordinary success path must be unaffected: a job whose windowed lock is free
// and whose active key does not collide is inserted and returned.
func TestEnqueueWithUniqueLock_StillInsertsWhenNothingCollides(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{
		ID: core.NewID(), Type: "wf", Queue: "default",
		Status: core.StatusPending, UniqueKey: "unique-key-1",
	}
	gotID, err := s.EnqueueWithUniqueLock(ctx, job, "scope-1", time.Hour)
	require.NoError(t, err)
	require.Equal(t, job.ID, gotID)

	var n int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", job.ID).Count(&n).Error)
	require.Equal(t, int64(1), n, "the OnConflict mapping must not suppress a legitimate insert")
}
