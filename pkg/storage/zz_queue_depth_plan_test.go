package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestQueueDepthGroupByKeepsTheStatusIndex pins the GROUP BY column ORDER in
// queueDepthQueueOnlyQuery, which is the only thing standing between the queue
// depth sampler and a full walk of every retained job row.
//
// The two orders return byte-identical rows (the caller folds them into a map),
// so no behavioural test in this suite can tell them apart — this is the guard
// that can. Grouping queue-major asks the planner for queue-major order, which it
// supplies by scanning idx_jobs_queue_created end to end: EVERY row, including
// the 30 days of completed history the default retention keeps. Grouping
// status-major lets it seek the two live statuses instead.
//
// Measured on this exact shape at 300k jobs (3k live, 8 queues), ANALYZEd,
// best of 5: 121.2ms queue-major vs 1.3ms status-major.
func TestQueueDepthGroupByKeepsTheStatusIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("EXPLAIN QUERY PLAN output is SQLite-specific")
	}

	// A mixed population: a little live work spread over several queues, plus a
	// bulk of completed history. Without the history the planner has no reason to
	// prefer either index and the guard would pass on any grouping.
	anchor := time.Now()
	for i := 0; i < 40; i++ {
		id := seedJobCreatedAt(t, ctx, s, fmt.Sprintf("depthq%d", i%4), "plandepth",
			anchor.Add(-time.Duration(i)*time.Minute))
		if i%4 != 0 {
			require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
				Where("id = ?", id).
				Updates(map[string]any{
					"status":       core.StatusCompleted,
					"completed_at": anchor.Add(-time.Duration(i) * time.Minute),
				}).Error)
		}
	}
	require.NoError(t, s.db.WithContext(ctx).Exec("ANALYZE").Error)

	// Explain the SHIPPED query, not a lookalike: this is the same builder
	// GetQueueDepthQueueOnly runs.
	type row struct {
		Queue  string
		Status string
		Count  int64
	}
	var rows []row
	stmt := queueDepthQueueOnlyQuery(s.db.Session(&gorm.Session{DryRun: true})).
		Find(&rows).Statement
	plan := explainQueryPlan(t, s, stmt)
	t.Logf("plan: %s", plan)

	assert.Contains(t, plan, "idx_jobs_status_created",
		"the depth aggregate must seek the live statuses; grouping queue-major sends "+
			"it through idx_jobs_queue_created, which covers every retained row "+
			"(121.2ms vs 1.3ms at 300k, see queueDepthQueueOnlyQuery)")
	assert.NotContains(t, plan, "idx_jobs_queue_created",
		"a queue-major plan walks the whole table; re-measure before accepting it")
}

// TestQueueDepthQueueOnlyCountsPendingAndRunningPerQueue is the behavioural half:
// the plan guard above is free to pick any index as long as the numbers stay
// right, and this is what says what right is.
func TestQueueDepthQueueOnlyCountsPendingAndRunningPerQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	seed := func(queue string, status core.JobStatus, n int) {
		for i := 0; i < n; i++ {
			job := &core.Job{Type: "depth", Queue: queue}
			require.NoError(t, s.Enqueue(ctx, job))
			if status != core.StatusPending {
				require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
					Where("id = ?", job.ID).
					Update("status", status).Error)
			}
		}
	}
	seed("alpha", core.StatusPending, 3)
	seed("alpha", core.StatusRunning, 2)
	seed("beta", core.StatusPending, 1)
	seed("beta", core.StatusCompleted, 5) // terminal rows must not be counted
	seed("gamma", core.StatusRunning, 4)

	depth, err := s.GetQueueDepthQueueOnly(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string][2]int64{
		"alpha": {3, 2},
		"beta":  {1, 0},
		"gamma": {0, 4},
	}, depth)
}
