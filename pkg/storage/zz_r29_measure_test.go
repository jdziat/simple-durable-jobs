package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestR29_JobsListOrderKeepsTheIndex is the plan half of the decision recorded on
// jobSortOrder and deadLetterOrderColumn: the ORDER BY stays a BARE column so
// SQLite can walk the index in order and stop at the LIMIT, and the single clock
// face is supplied on the WRITE side instead.
//
// A normalized ORDER BY (julianday(created_at) DESC) is instant-correct and
// measured 554x slower on this exact shape at 200k rows, because the index can no
// longer supply any of the order. The difference is visible in the plan at any row
// count — "RIGHT PART OF ORDER BY" means the index supplied the timestamp order
// and only the id tiebreak needed sorting, while a bare "USE TEMP B-TREE FOR ORDER
// BY" means the whole result set is read and sorted.
//
// Nothing else in this suite can tell the two apart: they return the same rows on
// a single-face table. So a change from one to the other must fail HERE, with the
// measurement re-done, rather than land silently.
func TestR29_JobsListOrderKeepsTheIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("EXPLAIN QUERY PLAN output is SQLite-specific")
	}

	anchor := time.Now()
	for i := 0; i < 40; i++ {
		id := seedJobCreatedAt(t, ctx, s, "planord", fmt.Sprintf("j%d", i),
			anchor.Add(-time.Duration(i)*time.Hour))
		if i%2 == 0 {
			require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
				Where("id = ?", id).
				Updates(map[string]any{
					"status":             core.StatusFailed,
					"dead_lettered_at":   anchor.Add(-time.Duration(i) * time.Hour),
					"dead_letter_reason": "seeded",
				}).Error)
		}
	}
	require.NoError(t, s.db.WithContext(ctx).Exec("ANALYZE").Error)

	for _, tc := range []struct {
		name  string
		build func() *gorm.Statement
		index string
	}{
		{
			name: "jobs list, queue filter",
			build: func() *gorm.Statement {
				var jobs []*core.Job
				return s.db.Session(&gorm.Session{DryRun: true}).Model(&core.Job{}).
					Where("queue = ?", "planord").
					Order(jobSortOrder(core.JobFilter{})).
					Limit(50).Find(&jobs).Statement
			},
			index: "idx_jobs_queue_created",
		},
		{
			name: "jobs list, status filter",
			build: func() *gorm.Statement {
				var jobs []*core.Job
				return s.db.Session(&gorm.Session{DryRun: true}).Model(&core.Job{}).
					Where("status = ?", core.StatusPending).
					Order(jobSortOrder(core.JobFilter{})).
					Limit(50).Find(&jobs).Statement
			},
			index: "idx_jobs_status_created",
		},
		{
			name: "dead-letter view",
			build: func() *gorm.Statement {
				var jobs []*core.Job
				return s.db.Session(&gorm.Session{DryRun: true}).Model(&core.Job{}).
					Where(deadLetterOrderColumn + " IS NOT NULL").
					Order(deadLetterDefaultOrder()).
					Limit(50).Find(&jobs).Statement
			},
			index: "idx_jobs_dead_lettered_at",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := explainQueryPlan(t, s, tc.build())
			t.Logf("plan: %s", plan)
			assert.Contains(t, plan, tc.index,
				"the ORDER BY must let this query keep its index")
			assert.NotContains(t, strings.ReplaceAll(plan, "RIGHT PART OF ORDER BY", ""),
				"USE TEMP B-TREE FOR ORDER BY",
				"the index must supply the timestamp order; a whole-result sort here means "+
					"the ORDER BY became a computed expression — re-measure before accepting it "+
					"(554x at 200k rows, see jobSortOrder)")
		})
	}
}
