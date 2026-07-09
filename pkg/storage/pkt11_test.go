package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestDeleteFanOutSubtree_FailsLoudOnCycle (PKT-11 / E3-E4) proves deleteFanOutSubtree
// aborts loudly instead of silently deleting a partial subtree when the fan-out
// tree is cyclically corrupt. A 2-node cycle (A's fan-out has sub-job B, B's
// fan-out has sub-job A) keeps the BFS frontier non-empty forever, so the walk
// hits maxFanOutTreeDepth — which must now return an error, not truncate.
func TestDeleteFanOutSubtree_FailsLoudOnCycle(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	a, b := core.NewID(), core.NewID()
	fa, fb := core.NewID(), core.NewID()
	// No FK on jobs.fan_out_id, so a job can reference a not-yet-created fan-out;
	// fan_outs.parent_job_id DOES have an FK, so create the parent jobs first.
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: a, Type: "work", Queue: "q", Status: core.StatusCompleted, FanOutID: &fb,
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: b, Type: "work", Queue: "q", Status: core.StatusCompleted, FanOutID: &fa,
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.FanOut{ID: fa, ParentJobID: a, TotalCount: 1}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.FanOut{ID: fb, ParentJobID: b, TotalCount: 1}).Error)

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.deleteFanOutSubtree(tx, a)
	})
	require.Error(t, err, "a cyclic fan-out tree must fail loud, not silently delete a partial subtree")
	require.Contains(t, err.Error(), "exceeds max depth")

	// The abort rolled back: nothing was deleted (no orphaned partial tree).
	var jobs int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).Where("id IN ?", []core.UUID{a, b}).Count(&jobs).Error)
	require.Equal(t, int64(2), jobs, "a failed subtree delete must not partially delete the tree")
}
