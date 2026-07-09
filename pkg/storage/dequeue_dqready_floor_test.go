package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestDequeueBatch_DueJobClaimableBeforePromoter guards D2b: on Postgres/SQLite
// the dequeue no longer filters on the dq_ready hint, so a job that is DUE
// (COALESCE(run_at, created_at) <= now) is claimable the instant it is eligible —
// it does not have to wait for the promoter tick to flip dq_ready=true.
//
// The predicate `dq_ready = true` was a pure redundant filter on PG/SQLite (the
// dequeue index is the priority-first partial idx_jobs_dequeue_eligible WHERE
// status='pending', which does not contain dq_ready), and its only effect was a
// dequeue-visibility LATENCY FLOOR: a delayed job whose run_at just passed stayed
// unclaimable until the promoter caught up. eligExpr<=now is the real fence.
//
// MySQL KEEPS the predicate (idx_jobs_dq_ready leads with (status, dq_ready) so
// dropping it would force a priority filesort), so this behavior is PG/SQLite
// only and the test skips MySQL.
//
// Deterministic recreation of "eligible but not-yet-promoted": enqueue a due job
// (past run_at → Enqueue sets dq_ready=true because it is already eligible), then
// force dq_ready=false to model the window before the promoter runs. A ready
// sibling keeps the first dequeue pass non-empty so the empty-pass self-heal
// (PromoteReadyJobs) cannot mask the floor by promoting the delayed job for us.
func TestDequeueBatch_DueJobClaimableBeforePromoter(t *testing.T) {
	s := newTestStorage(t)
	if s.dialect() == dialectMySQL {
		t.Skip("MySQL keeps the dq_ready dequeue floor (idx_jobs_dq_ready requires it); the floor is removed only on PG/SQLite")
	}
	ctx := context.Background()

	ready := &core.Job{ID: core.NewID(), Type: "work", Queue: "q"}
	require.NoError(t, s.Enqueue(ctx, ready))

	past := time.Now().Add(-time.Minute)
	delayed := &core.Job{ID: core.NewID(), Type: "work", Queue: "q", RunAt: &past}
	require.NoError(t, s.Enqueue(ctx, delayed))
	// Model the pre-promoter window: due, but dq_ready still false.
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", delayed.ID).UpdateColumn("dq_ready", false).Error)

	got, err := s.DequeueBatch(ctx, []string{"q"}, "w1", 10)
	require.NoError(t, err)
	claimed := make(map[core.UUID]bool, len(got))
	for _, j := range got {
		claimed[j.ID] = true
	}
	require.Truef(t, claimed[delayed.ID],
		"a DUE job must be claimable before the promoter flips dq_ready (PG/SQLite floor removed); claimed %d job(s)", len(got))
	require.True(t, claimed[ready.ID], "the ready sibling must also be claimed")
}
