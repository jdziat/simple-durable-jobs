package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestEnqueueBatchCorrectionsOnlyTouchRowsItCreated covers the reported defect and
// BOTH regressions that killed the two previous fixes.
func TestEnqueueBatchCorrectionsOnlyTouchRowsItCreated(t *testing.T) {
	t.Run("a stranger holding the id is not reconfigured", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		stranger := &core.Job{
			ID: core.NewID(), Type: "stranger", Queue: "default",
			Status: core.StatusPending, MaxRetries: 7,
		}
		require.NoError(t, s.Enqueue(ctx, stranger))

		intruder := &core.Job{
			ID: stranger.ID, Type: "intruder", Queue: "other",
			Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true,
		}
		require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{intruder}))

		var got core.Job
		require.NoError(t, s.db.Where("id = ?", stranger.ID).First(&got).Error)
		require.Equal(t, "stranger", got.Type, "the stranger's row was overwritten")
		require.Equal(t, 7, got.MaxRetries,
			"the corrective UPDATE applied the intruder's Retries(0) to a row EnqueueBatch did not create")
	})

	t.Run("two jobs sharing an id: the survivor keeps its OWN config", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		// What Queue.EnqueueBatch produces when in-batch Unique collapse gives the
		// loser the winner's id and middleware then rewrites their keys apart.
		shared := core.NewID()
		winner := &core.Job{
			ID: shared, Type: "winner", Queue: "default",
			Status: core.StatusPending, MaxRetries: 2, UniqueKey: "key-a",
		}
		loser := &core.Job{
			ID: shared, Type: "loser", Queue: "default",
			Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true, UniqueKey: "key-b",
		}
		require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{winner, loser}))

		var got core.Job
		require.NoError(t, s.db.Where("id = ?", shared).First(&got).Error)
		require.Equal(t, "winner", got.Type, "the first job must be the one that persists")
		require.Equal(t, 2, got.MaxRetries,
			"the suppressed job's Retries(0) landed on the surviving row")
	})

	t.Run("regression: the collapsed id still resolves (killed attempt 1)", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		shared := core.NewID()
		a := &core.Job{ID: shared, Type: "a", Queue: "default", Status: core.StatusPending, UniqueKey: "k"}
		b := &core.Job{ID: shared, Type: "b", Queue: "default", Status: core.StatusPending, UniqueKey: "k"}
		require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{a, b}))

		for _, j := range []*core.Job{a, b} {
			var n int64
			require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", j.ID).Count(&n).Error)
			require.Equalf(t, int64(1), n,
				"job %q came back with an id naming no row; attempt 1 dangled ids this way", j.Type)
		}
	})

	t.Run("regression: an uncontested job is never dropped (killed attempt 2)", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		// Two jobs share a primary key; the FIRST also collides on the active
		// unique key with a pre-existing row, so the id-claimer is itself
		// suppressed. Attempt 2 dropped the second job in advance and lost it.
		blocker := &core.Job{
			ID: core.NewID(), Type: "blocker", Queue: "default",
			Status: core.StatusPending, UniqueKey: "contended",
		}
		require.NoError(t, s.Enqueue(ctx, blocker))

		shared := core.NewID()
		claimer := &core.Job{
			ID: shared, Type: "claimer", Queue: "default",
			Status: core.StatusPending, UniqueKey: "contended",
		}
		uncontested := &core.Job{
			ID: shared, Type: "uncontested", Queue: "default",
			Status: core.StatusPending, UniqueKey: "free",
		}
		require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{claimer, uncontested}))

		var total int64
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", shared).Count(&total).Error)
		require.Equalf(t, int64(1), total,
			"the batch persisted no row for the shared id: attempt 2 lost the uncontested job this way")
	})

	t.Run("an at-least-once replay stays a silent no-op", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		job := &core.Job{
			ID: core.NewID(), Type: "outbox", Queue: "default",
			Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true,
		}
		require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{job}))
		require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{job}), "replay must not error")

		var n int64
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", job.ID).Count(&n).Error)
		require.Equal(t, int64(1), n, "replay must not duplicate the row")

		var got core.Job
		require.NoError(t, s.db.Where("id = ?", job.ID).First(&got).Error)
		require.Equal(t, 0, got.MaxRetries, "the replayed row must keep its explicit Retries(0)")
	})
}
