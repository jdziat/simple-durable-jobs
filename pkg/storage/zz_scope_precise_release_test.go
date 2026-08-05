package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestUniqueLockReleaseCannotClobberAnotherLiveWindow pins the scope-precise
// predicate in createUniqueLockedJob's failed-insert cleanup.
//
// When the windowed lock is won but the job insert then loses the SEPARATE
// active-unique index race, the window must be released — otherwise it guards
// work that never ran. The release used to be keyed on job_id alone, which
// deletes EVERY unique_locks row referencing that id. Nothing forces a job id to
// belong to only one scope: fillEnqueueDefaults mints an id only when the caller
// left it empty, so a caller-supplied id can sit under two windows. A losing
// enqueue would then destroy an unrelated LIVE window and admit a duplicate run
// of the work that window was protecting — the same double charge this change
// exists to prevent, arriving from a new direction.
//
// Mutation-tested: widening the predicate back to `job_id = ?` fails this test.
func TestUniqueLockReleaseCannotClobberAnotherLiveWindow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	shared := core.NewID()

	// An unrelated, live window for a DIFFERENT scope that happens to reference
	// the same job id.
	bystander := core.UniqueLock{
		ScopeHash: "scope-bystander",
		JobID:     shared,
		ExpiresAt: time.Now().UTC().Add(90 * 24 * time.Hour),
	}
	require.NoError(t, s.db.Create(&bystander).Error)

	// Occupy the active-unique key so the job insert below is guaranteed to lose.
	blocker := &core.Job{
		ID: core.NewID(), Type: "blocker", Queue: "default",
		Status: core.StatusPending, UniqueKey: "contended-active-key",
	}
	require.NoError(t, s.Enqueue(ctx, blocker))

	// Win a windowed lock for our own scope, then fail the job insert on the
	// active-unique index. The cleanup must release OUR scope only.
	loser := &core.Job{
		ID: shared, Type: "loser", Queue: "default",
		Status: core.StatusPending, UniqueKey: "contended-active-key",
	}
	// Use the caller-owned-transaction path and COMMIT despite the error. On the
	// self-contained EnqueueWithUniqueLock path the rollback would undo the stray
	// delete, so that version of this test cannot fail and proves nothing — the
	// damage only survives where the caller owns the commit, which is exactly the
	// case the production comment cites.
	tx := s.db.Begin()
	require.NoError(t, tx.Error)
	_, err := s.EnqueueWithUniqueLockTx(ctx, tx, loser, "scope-ours", time.Hour)
	require.Error(t, err, "the job insert must lose the active-unique race")
	require.NoError(t, tx.Commit().Error)

	var bystanderLeft int64
	require.NoError(t, s.db.Model(&core.UniqueLock{}).
		Where("scope_hash = ?", "scope-bystander").Count(&bystanderLeft).Error)
	require.Equalf(t, int64(1), bystanderLeft,
		"the failed enqueue destroyed an unrelated live window: releasing by job_id alone "+
			"deletes every scope referencing that id, so the bystander's 90-day guard is gone "+
			"and its protected work can run twice")
}
