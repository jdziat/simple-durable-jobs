package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// seedLargeFanOutSubtree creates a parent with n children under one fan-out.
func seedLargeFanOutSubtree(t *testing.T, s *GormStorage, n int) (parentID, fanOutID core.UUID) {
	t.Helper()
	ctx := context.Background()
	parentID = core.NewID()
	fanOutID = core.NewID()
	require.NoError(t, s.Enqueue(ctx, &core.Job{
		ID: parentID, Type: "parent", Queue: "default", Status: core.StatusWaiting,
	}))
	require.NoError(t, s.db.Create(&core.FanOut{
		ID: fanOutID, ParentJobID: parentID, TotalCount: n,
		Status: "pending", Strategy: "collect_all",
	}).Error)

	now := time.Now().UTC()
	rows := make([]*core.Job, 0, 200)
	flush := func() {
		if len(rows) == 0 {
			return
		}
		require.NoError(t, s.db.Create(rows).Error)
		rows = rows[:0]
	}
	for i := 0; i < n; i++ {
		rows = append(rows, &core.Job{
			ID: core.NewID(), Type: fmt.Sprintf("child-%d", i), Queue: "default",
			Status: core.StatusPending, ParentJobID: &parentID, FanOutID: &fanOutID,
			CreatedAt: now, UpdatedAt: now,
		})
		// 200, not 1000: a batch INSERT binds every column of every row, so
		// 1000 rows x ~30 columns already exceeds SQLite's 32766 variable ceiling
		// and the FIXTURE fails before the code under test runs.
		if len(rows) == 200 {
			flush()
		}
	}
	flush()
	return parentID, fanOutID
}

// TestFanOutSubtreeOperationsAreBindParameterBounded pins that the whole-subtree
// operations chunk their literal IN-lists.
//
// deleteFanOutSubtree (reached by Requeue, the dashboard Retry button and
// DeleteWorkflowSubtree) binds every descendant id across five DELETEs, and
// CancelJobTerminal locks the whole subtree with one SELECT ... FOR UPDATE. Past
// the driver's bind-parameter ceiling (SQLite 32766, Postgres 65535) the statement
// fails outright, so a large workflow can be neither replayed nor cancelled — and
// the operator hits it exactly when the workflow is big enough to matter.
//
// This is the same defect class as the retention sweep fixed earlier on this
// branch; a skeptic flagged deleteFanOutSubtree as a residual at the time and it
// was recorded rather than fixed.
func TestFanOutSubtreeOperationsAreBindParameterBounded(t *testing.T) {
	const children = 33000 // above SQLite's 32766 ceiling

	t.Run("Requeue deletes the subtree", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		parentID, _ := seedLargeFanOutSubtree(t, s, children)
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", parentID).
			Update("status", core.StatusFailed).Error)

		_, err := s.Requeue(ctx, parentID)
		require.NoErrorf(t, err, "requeueing a workflow with %d descendants must not exceed the "+
			"driver's bind-parameter ceiling", children)
	})

	t.Run("CancelJobTerminal locks the subtree", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		parentID, _ := seedLargeFanOutSubtree(t, s, children)

		err := s.CancelJobTerminal(ctx, parentID)
		require.NoErrorf(t, err, "terminally cancelling a workflow with %d descendants must not "+
			"exceed the driver's bind-parameter ceiling", children)
	})
}
