package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// queue.Unique(key) is documented as releasing its guard "as soon as the existing
// job reaches completed, failed, or cancelled". The predicate matched only
// pending-or-running, so a holder parked in `waiting` (on a signal or a fan-out) or
// sitting in `paused` — neither of which is terminal — silently stopped
// deduplicating and a second job with the same key was admitted.
//
// The damage is not two handlers running at once: the partial unique index
// (pending/running only) is a hard backstop and refuses the SECOND row the moment
// either job becomes runnable. The damage is that it refuses the WRONG one. The
// interloper is admitted as `pending`, and when the original holder's signal
// finally arrives its waiting->pending resume collides with the index and fails —
// so the job that was doing the work is the one that cannot proceed, until the
// interloper it never asked for reaches a terminal status.
//
// Widening the app-side predicate fixes both halves at once: the interloper is
// never admitted, so the collision never arises. The DB index is deliberately left
// alone — it exists for Postgres's absent-row FOR UPDATE gap, and with the
// predicate widened there IS now a row to lock in the waiting/paused case, so that
// gap is narrower than before rather than wider.
func TestUniqueKey_HolderInWaitingOrPausedStillDeduplicates(t *testing.T) {
	ctx := context.Background()

	for _, holderStatus := range []core.JobStatus{
		core.StatusWaiting,
		core.StatusPaused,
		core.StatusRetrying,
	} {
		t.Run(string(holderStatus), func(t *testing.T) {
			s := newTestStorage(t)
			holder := &core.Job{
				ID: core.NewID(), Type: "wf", Queue: "default",
				Status: holderStatus, UniqueKey: "shared",
			}
			require.NoError(t, s.db.Create(holder).Error)

			interloper := &core.Job{
				ID: core.NewID(), Type: "wf", Queue: "default",
				Status: core.StatusPending, UniqueKey: "shared",
			}
			// EnqueueUnique is the method queue.Enqueue routes to for Unique(key)
			// (pkg/queue/queue.go). The low-level Enqueue consults only the partial
			// index and would not exercise the app-side predicate at all.
			err := s.EnqueueUnique(ctx, interloper, "shared")
			require.ErrorIs(t, err, core.ErrDuplicateJob,
				"a %s holder has not reached completed/failed/cancelled, so it still holds the Unique guard; admitting a second job lets the interloper block the holder's own resume with a unique-index collision (got err=%v)", holderStatus, err)

			var n int64
			require.NoError(t, s.db.Model(&core.Job{}).
				Where("unique_key = ?", "shared").Count(&n).Error)
			require.Equal(t, int64(1), n, "the refused enqueue must not have inserted a row")
		})
	}
}

// The guard must still RELEASE on the statuses the documentation names, or widening
// it would have turned a dedup guard into a permanent block.
func TestUniqueKey_TerminalHolderReleasesTheGuard(t *testing.T) {
	ctx := context.Background()

	for _, terminal := range core.TerminalJobStatuses {
		t.Run(string(terminal), func(t *testing.T) {
			s := newTestStorage(t)
			done := &core.Job{
				ID: core.NewID(), Type: "wf", Queue: "default",
				Status: terminal, UniqueKey: "shared",
			}
			require.NoError(t, s.db.Create(done).Error)

			next := &core.Job{
				ID: core.NewID(), Type: "wf", Queue: "default",
				Status: core.StatusPending, UniqueKey: "shared",
			}
			require.NoError(t, s.EnqueueUnique(ctx, next, "shared"),
				"a %s holder has released the guard, so the next job must be admitted; if this fails the widened predicate has become a permanent block", terminal)
		})
	}
}

// The consequence that actually hurt: with the interloper refused, the holder's own
// resume no longer collides with the partial unique index.
func TestUniqueKey_WaitingHolderCanStillResume(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	holder := &core.Job{
		ID: core.NewID(), Type: "wf", Queue: "default",
		Status: core.StatusWaiting, UniqueKey: "shared",
	}
	require.NoError(t, s.db.Create(holder).Error)

	// The interloper is refused, so nothing else holds the key.
	require.ErrorIs(t, s.EnqueueUnique(ctx, &core.Job{
		ID: core.NewID(), Type: "wf", Queue: "default",
		Status: core.StatusPending, UniqueKey: "shared",
	}, "shared"), core.ErrDuplicateJob)

	resumed, err := s.ResumeJob(ctx, holder.ID)
	require.NoError(t, err,
		"the waiting holder must be able to resume; an admitted interloper would have taken the pending slot and made this fail on the unique index")
	require.True(t, resumed)

	row, err := s.GetJob(ctx, holder.ID)
	require.NoError(t, err)
	require.Equal(t, core.StatusPending, row.Status)
}
