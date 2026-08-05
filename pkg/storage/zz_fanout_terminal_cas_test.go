package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestAccountTerminalWithFanOut_PendingCASIsExactlyOnce covers the
// `status = 'pending'` CAS on the fan-out advance inside the CHILD terminal
// transaction (accountTerminalWithFanOut), which its own comment calls "the
// exactly-once serializer" and which had no test on any dialect: replacing it with
// a bare `id = ?` left pkg/storage, pkg/fanout and pkg/worker all green.
//
// Note there are THREE pending-CAS sites on fan_outs and the other two ARE covered
// (UpdateFanOutStatus by TestUpdateFanOutStatus_IdempotentWhenAlreadyCompleted and
// _CompleteThenFail; the CancelJobTerminal one by
// TestCancelJobTerminal_PreFrozenFailFastFanOutReconcilesCounts). Only this one was
// free, so a mutation sweep that happens to pick a different site reports the guard
// as covered — measured, because I made exactly that mistake first.
//
// The CAS carries two distinct properties, and dropping it loses both:
//
//  1. DATA INTEGRITY. A fan-out an operator has already cancelled would be
//     re-labelled `completed` by the last sibling settling afterwards, silently
//     overwriting the cancellation in the record an operator reads.
//  2. EXACTLY-ONCE. RowsAffected == 1 is how a child learns it is the one that
//     made the fan-out terminal, and the caller uses the returned FanOut to decide
//     whether to resume the waiting parent. Ungated, a late child also reports
//     RowsAffected == 1 and the parent is resumed a second time.
//
// Reachable through the public API: cancel a workflow while a sub-job is still
// in flight, which is ordinary operator behaviour, then let that sub-job finish.
func TestAccountTerminalWithFanOut_PendingCASIsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := core.NewID()
	seedTestJob(t, ctx, s, parent, core.StatusWaiting)
	fanOutID := core.NewID()
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID: fanOutID, ParentJobID: parent, TotalCount: 2, Status: core.FanOutPending,
	}))

	child := func() core.UUID {
		id := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: id, Type: "sub", Queue: "default", Status: core.StatusRunning,
			LockedBy: "w1", FanOutID: &fanOutID, ParentJobID: &parent,
		}).Error)
		return id
	}
	c1, c2 := child(), child()

	// First child settles: 1 of 2, so the fan-out is not terminal yet.
	fo, err := s.CompleteWithResult(ctx, c1, "w1", []byte(`"r1"`))
	require.NoError(t, err)
	require.NotNil(t, fo)
	require.Equal(t, core.FanOutPending, fo.Status, "premise: one of two children cannot make it terminal")

	// An operator cancels the workflow while the second child is still running.
	// This is what freezes the fan-out terminal ahead of the last child.
	require.NoError(t, s.db.Model(&core.FanOut{}).Where("id = ?", fanOutID).
		Update("status", core.FanOutCancelled).Error)

	// The second child now settles. Its live counts say completed+failed+cancelled
	// == total_count, so TerminalStatus() reports done and it attempts the advance.
	fo2, err := s.CompleteWithResult(ctx, c2, "w1", []byte(`"r2"`))
	require.NoError(t, err)
	require.NotNil(t, fo2)

	require.NotEqual(t, core.FanOutCompleted, fo2.Status,
		"the last child reported itself as the one that completed the fan-out, but the fan-out was already terminal; the caller uses this to decide whether to resume the waiting parent, so the parent is resumed a second time")

	var persisted core.FanOut
	require.NoError(t, s.db.Where("id = ?", fanOutID).First(&persisted).Error)
	require.Equal(t, core.FanOutCancelled, persisted.Status,
		"a fan-out an operator cancelled was re-labelled %q by the last sibling settling afterwards, silently overwriting the cancellation", persisted.Status)

	// The ungated count reconciliation must still run, or a fan-out frozen early
	// keeps permanently short counts. This is the else-branch of the same CAS, so
	// asserting it here keeps a future "simplification" from deleting both.
	require.Equal(t, 2, persisted.CompletedCount,
		"counts must still reconcile once every child has settled, even though the status advance was correctly refused")
}

// A fan-out still pending when its last child settles MUST advance — otherwise the
// CAS could be "fixed" by never advancing at all and the test above would still
// pass. This is the positive leg, deliberately in the same file.
func TestAccountTerminalWithFanOut_StillAdvancesAPendingFanOut(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := core.NewID()
	seedTestJob(t, ctx, s, parent, core.StatusWaiting)
	fanOutID := core.NewID()
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID: fanOutID, ParentJobID: parent, TotalCount: 1, Status: core.FanOutPending,
	}))
	only := core.NewID()
	require.NoError(t, s.db.Create(&core.Job{
		ID: only, Type: "sub", Queue: "default", Status: core.StatusRunning,
		LockedBy: "w1", FanOutID: &fanOutID, ParentJobID: &parent,
	}).Error)

	fo, err := s.CompleteWithResult(ctx, only, "w1", []byte(`"r"`))
	require.NoError(t, err)
	require.NotNil(t, fo)
	require.Equal(t, core.FanOutCompleted, fo.Status,
		"the child that settles a pending fan-out must be told it made it terminal, or the waiting parent is never resumed")

	var persisted core.FanOut
	require.NoError(t, s.db.Where("id = ?", fanOutID).First(&persisted).Error)
	require.Equal(t, core.FanOutCompleted, persisted.Status)
	require.Equal(t, 1, persisted.CompletedCount)
	require.WithinDuration(t, time.Now(), persisted.UpdatedAt, time.Minute)
}
