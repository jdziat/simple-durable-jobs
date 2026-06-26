package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetRunningJobsByWorker surfaces the pod→task ownership SDJ records on every
// running job (locked_by), for operator visibility and graceful-handoff tooling.
func TestGetRunningJobsByWorker(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	until := time.Now().Add(time.Minute)

	mine1 := core.NewID()
	mine2 := core.NewID()
	theirs := core.NewID()
	idle := core.NewID()
	require.NoError(t, s.db.WithContext(ctx).Create([]*core.Job{
		{ID: mine1, Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "pod-a", LockedUntil: &until},
		{ID: mine2, Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "pod-a", LockedUntil: &until},
		{ID: theirs, Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "pod-b", LockedUntil: &until},
		{ID: idle, Type: "t", Queue: "default", Status: core.StatusPending}, // not running
	}).Error)

	got, err := s.GetRunningJobsByWorker(ctx, "pod-a")
	require.NoError(t, err)
	ids := map[core.UUID]bool{}
	for _, j := range got {
		ids[j.ID] = true
		assert.Equal(t, core.StatusRunning, j.Status)
		assert.Equal(t, "pod-a", j.LockedBy)
	}
	assert.True(t, ids[mine1] && ids[mine2], "returns this worker's running jobs")
	assert.False(t, ids[theirs], "excludes another worker's jobs")
	assert.Len(t, got, 2)

	empty, err := s.GetRunningJobsByWorker(ctx, "pod-z")
	require.NoError(t, err)
	assert.Empty(t, empty)
}
