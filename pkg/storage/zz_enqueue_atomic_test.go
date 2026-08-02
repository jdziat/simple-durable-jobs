package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestEnqueue_ZeroRetryRowIsNeverExternallyVisibleCarryingTheDefault pins the
// contract applyExplicitZeroRetries states in its own doc comment: it writes the
// intended 0 back "in the caller's transaction, so the row is never visible
// carrying a value its author did not ask for".
//
// Enqueue used to pass the ROOT handle instead of a transaction, so the INSERT
// (which GORM writes with the declared `default:3`) and the corrective UPDATE were
// two separate autocommits. Between them the row existed, was dq_ready, and was
// claimable — so a worker could dequeue a Retries(0) job carrying max_retries=3
// and run it three times. That is the exact defect Retries(0) exists to prevent,
// surviving in a window.
//
// HOW THIS CAN FAIL. A GORM after-create callback fires while Enqueue's work is
// still in flight, and reads the row back through a SEPARATE session that is not
// part of that transaction. Uncommitted work is invisible to it, so:
//
//	fixed  -> the outside reader finds NO row (the tx has not committed)
//	broken -> the outside reader finds the row carrying max_retries = 3
//
// It therefore fails loudly on the old code rather than being a seed-only guard,
// and it does not depend on winning a race.
func TestEnqueue_ZeroRetryRowIsNeverExternallyVisibleCarryingTheDefault(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	var (
		observed    bool
		observedMax int
	)

	// A separate session: callbacks run on the transaction's *gorm.DB, so reading
	// through s.db directly is the outside view we want.
	const cbName = "zztest:observe_uncommitted"
	require.NoError(t, s.db.Callback().Create().After("gorm:create").Register(cbName, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != "jobs" {
			return
		}
		var got []core.Job
		if err := s.db.Session(&gorm.Session{NewDB: true}).
			Model(&core.Job{}).Where("queue = ?", "atomicq").Find(&got).Error; err != nil {
			return
		}
		if len(got) > 0 {
			observed = true
			observedMax = got[0].MaxRetries
		}
	}))
	t.Cleanup(func() { _ = s.db.Callback().Create().Remove(cbName) })

	job := &core.Job{Type: "zero-retry", Queue: "atomicq", MaxRetries: 0}
	require.NoError(t, s.Enqueue(ctx, job))

	require.False(t, observed,
		"a job row was visible OUTSIDE Enqueue's transaction while it was still being written "+
			"(max_retries=%d at that moment). The INSERT carries GORM's substituted default and is "+
			"corrected by a following UPDATE, so a row visible in between can be claimed by a worker "+
			"and run with the wrong retry budget", observedMax)

	// And the committed row must carry what the author actually asked for.
	var stored core.Job
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", job.ID).First(&stored).Error)
	require.Equal(t, 0, stored.MaxRetries,
		"Retries(0) must persist max_retries = 0, not the column default")
}

// TestEnqueue_DuplicateUniqueKeyLeavesNothingBehind covers the second defect the
// transaction rewrite closed: the unique-key path ran the corrective UPDATE and
// the dq_ready restore even when OnConflict inserted nothing, i.e. against a row
// belonging to whoever won the race.
func TestEnqueue_DuplicateUniqueKeyLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	first := &core.Job{Type: "dedup", Queue: "dupq", UniqueKey: "same-key", MaxRetries: 5}
	require.NoError(t, s.Enqueue(ctx, first))

	// A second enqueue on the same key asks for ZERO retries. It must be refused
	// AND must not touch the winner's row.
	second := &core.Job{Type: "dedup", Queue: "dupq", UniqueKey: "same-key", MaxRetries: 0}
	require.ErrorIs(t, s.Enqueue(ctx, second), core.ErrDuplicateJob)

	var stored core.Job
	require.NoError(t, s.db.Model(&core.Job{}).Where("unique_key = ?", "same-key").First(&stored).Error)
	require.Equal(t, 5, stored.MaxRetries,
		"the losing enqueue's Retries(0) must not have been applied to the winning row")
	require.Equal(t, first.ID, stored.ID)
}
