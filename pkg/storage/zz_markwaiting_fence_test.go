package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// MarkWaitingForSignal carries the same ownership+status fence as MarkWaiting:
// `id = ? AND locked_by = ? AND status = running`. Deleting it left all 23
// packages green, including the 310s tests/ integration suite — so the guard that
// stops a worker suspending a job it no longer owns was carrying no test weight at
// all. It is my own round-20 addition.
//
// What the fence prevents: a worker whose lease lapsed (its job already reclaimed
// by the reaper and running on a peer) calling MarkWaitingForSignal would otherwise
// yank the live job into `waiting` and stamp it with the stale worker's awaited
// signal name — suspending work another worker is actively executing.
func TestMarkWaitingForSignal_RefusesAJobThisWorkerDoesNotOwn(t *testing.T) {
	ctx := context.Background()

	t.Run("a different worker holds the lock", func(t *testing.T) {
		s := newTestStorage(t)
		id := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: id, Type: "wf", Queue: "default",
			Status: core.StatusRunning, LockedBy: "peer",
		}).Error)

		err := s.MarkWaitingForSignal(ctx, id, "stale-worker", "approval")
		require.ErrorIs(t, err, core.ErrJobNotOwned,
			"a worker that does not hold the lock must not be able to suspend the job; the peer is executing it right now")

		row, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, core.StatusRunning, row.Status, "the live job must not have been moved to waiting")
		require.Equal(t, "peer", row.LockedBy, "the peer's lock must survive")
		require.Empty(t, row.WaitingSignalName, "a refused suspend must not stamp an awaited name")
	})

	t.Run("the job is no longer running", func(t *testing.T) {
		s := newTestStorage(t)
		for _, status := range []core.JobStatus{
			core.StatusPending, core.StatusCompleted, core.StatusCancelled, core.StatusWaiting,
		} {
			id := core.NewID()
			require.NoError(t, s.db.Create(&core.Job{
				ID: id, Type: "wf", Queue: "default",
				Status: status, LockedBy: "w1",
			}).Error)

			err := s.MarkWaitingForSignal(ctx, id, "w1", "approval")
			require.ErrorIs(t, err, core.ErrJobNotOwned,
				"a %s job must not be suspendable; only a running job this worker owns can enter waiting", status)

			row, err := s.GetJob(ctx, id)
			require.NoError(t, err)
			require.Equal(t, status, row.Status, "the %s job must be unchanged", status)
		}
	})

	// The positive leg, so the fence cannot be "refuse everything".
	t.Run("the owning worker on a running job succeeds", func(t *testing.T) {
		s := newTestStorage(t)
		id := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: id, Type: "wf", Queue: "default",
			Status: core.StatusRunning, LockedBy: "w1",
		}).Error)

		require.NoError(t, s.MarkWaitingForSignal(ctx, id, "w1", "approval"))

		row, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, core.StatusWaiting, row.Status)
		require.Equal(t, "approval", row.WaitingSignalName)
		require.Empty(t, row.LockedBy, "a suspended job releases its lock")
	})
}
