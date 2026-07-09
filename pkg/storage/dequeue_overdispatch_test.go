package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestDequeueBatch_NeverOverDispatchesUnderChurn guards against a Postgres
// over-dispatch that let DequeueBatch claim far more jobs than the caller's
// limit.
//
// Root cause: the RETURNING claim path (dequeueBatchReturning) once issued a
// bare `UPDATE jobs ... WHERE id IN (SELECT id ... FOR UPDATE SKIP LOCKED
// LIMIT n) RETURNING *`. Postgres is free to place that locking subquery on the
// INNER side of a nested-loop semi-join and RE-EVALUATE it per candidate row,
// applying LIMIT n on each iteration — a runaway where a LIMIT 5 claim updates
// 10+ rows (a worker over-dispatches its whole batch, exceeding its concurrency
// budget and locking far more work than intended if it crashes). The claim now
// wraps the locking subquery in a MATERIALIZED CTE, an optimization fence that
// forces single evaluation.
//
// The bad plan is stats/bloat dependent, so a single fresh-table claim almost
// never triggers it (~2/8000). We drive many seed->claim cycles to accumulate
// the churn that flips Postgres onto the pathological plan, and assert every
// claim honors the limit — both in the returned batch and in the DB row states.
//
// Postgres/SQLite only: MySQL uses the two-step dequeueBatchLocked path (pluck
// ids, then UPDATE the frozen id set), which never embedded the locking
// subquery and so never had this bug.
func TestDequeueBatch_NeverOverDispatchesUnderChurn(t *testing.T) {
	s := newTestStorage(t)
	if s.dialect() == dialectMySQL {
		t.Skip("MySQL uses the two-step claim path; the RETURNING over-dispatch is Postgres/SQLite-only")
	}
	ctx := context.Background()

	const (
		iters     = 600
		seedCount = 12
		limit     = 5
	)
	for i := 0; i < iters; i++ {
		require.NoError(t, s.db.Exec("DELETE FROM jobs").Error, "iter %d: reset", i)
		base := time.Now().Add(-time.Hour)
		for j := 0; j < seedCount; j++ {
			q := "alpha"
			if j%2 == 1 {
				q = "beta"
			}
			require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
				ID:        core.NewID(),
				Type:      "work",
				Queue:     q,
				Priority:  seedCount - j,
				Status:    core.StatusPending,
				CreatedAt: base.Add(time.Duration(j) * time.Second),
			}).Error, "iter %d: seed", i)
		}

		// Alternate the flat total-limit path and the per-queue-budget path: both
		// issue the same MATERIALIZED-CTE claim (per unit for the budget path), so
		// both must honor their limit. The bug only actually manifests on the
		// Postgres CI matrix — SQLite is single-writer with no FOR UPDATE re-lock,
		// so there this loop is a cheap correct-count sanity check, not a repro.
		var got []*core.Job
		var err error
		if i%2 == 0 {
			got, err = s.DequeueBatch(ctx, []string{"alpha", "beta"}, "w1", limit)
		} else {
			// alpha:2 + beta:3 = 5; a per-unit over-dispatch would blow past these.
			got, err = s.DequeueBatchPerQueue(ctx, "w1", map[string]int{"alpha": 2, "beta": 3})
		}
		require.NoError(t, err, "iter %d", i)
		require.LessOrEqualf(t, len(got), limit,
			"iter %d: claim OVER-DISPATCHED: returned %d jobs for limit %d — the "+
				"MATERIALIZED CTE fence regressed and Postgres re-evaluated the locking subquery",
			i, len(got), limit)

		var running int64
		require.NoError(t, s.db.Raw("SELECT count(*) FROM jobs WHERE status = ?", core.StatusRunning).Scan(&running).Error, "iter %d", i)
		require.LessOrEqualf(t, running, int64(limit),
			"iter %d: %d rows are running after a limit-%d claim — over-dispatch flipped extra rows to running",
			i, running, limit)
	}
}

// TestDequeueBatch_ClaimFencesLockingSubqueryInCTE is a fast, deterministic
// guard complementing the churn test above: it captures the SQL the RETURNING
// claim path actually executes and asserts the locking candidate subquery is
// wrapped in a MATERIALIZED CTE. That fence is what forces Postgres to evaluate
// the FOR UPDATE SKIP LOCKED ... LIMIT n subquery exactly once; dropping it (a
// bare `UPDATE ... WHERE id IN (SELECT ... LIMIT n FOR UPDATE SKIP LOCKED)`)
// reintroduces the per-row re-evaluation over-dispatch. Unlike the churn test,
// this catches a regression instantly and independent of planner/stats state.
// Postgres/SQLite only (the RETURNING path); MySQL uses dequeueBatchLocked.
func TestDequeueBatch_ClaimFencesLockingSubqueryInCTE(t *testing.T) {
	s := newTestStorage(t)
	if s.dialect() == dialectMySQL {
		t.Skip("MySQL uses the two-step claim path (no embedded locking subquery)")
	}
	ctx := context.Background()

	var claimSQL string
	capture := func(db *gorm.DB) {
		sql := db.Statement.SQL.String()
		if strings.Contains(sql, "UPDATE jobs") && strings.Contains(sql, "SELECT id FROM claimed") {
			claimSQL = sql
		}
	}
	// Raw(...).Scan on an UPDATE...RETURNING can surface through the raw, row, or
	// query pipeline depending on GORM internals — hook all three.
	require.NoError(t, s.db.Callback().Raw().After("gorm:raw").Register("test:capture_claim", capture))
	require.NoError(t, s.db.Callback().Row().After("gorm:row").Register("test:capture_claim_row", capture))
	require.NoError(t, s.db.Callback().Query().After("gorm:query").Register("test:capture_claim_q", capture))
	t.Cleanup(func() {
		_ = s.db.Callback().Raw().After("gorm:raw").Remove("test:capture_claim")
		_ = s.db.Callback().Row().After("gorm:row").Remove("test:capture_claim_row")
		_ = s.db.Callback().Query().After("gorm:query").Remove("test:capture_claim_q")
	})

	base := time.Now().Add(-time.Hour)
	for j := 0; j < 3; j++ {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: core.NewID(), Type: "work", Queue: "q", Priority: 3 - j,
			Status: core.StatusPending, CreatedAt: base.Add(time.Duration(j) * time.Second),
		}).Error)
	}

	got, err := s.DequeueBatch(ctx, []string{"q"}, "w1", 2)
	require.NoError(t, err)
	require.Len(t, got, 2, "sanity: the claim must honor the limit")

	require.NotEmpty(t, claimSQL, "did not capture the RETURNING claim statement")
	require.Containsf(t, claimSQL, "MATERIALIZED",
		"the claim must fence its locking subquery in a MATERIALIZED CTE to stop Postgres "+
			"re-evaluating it per row (over-dispatch); captured SQL:\n%s", claimSQL)
}
