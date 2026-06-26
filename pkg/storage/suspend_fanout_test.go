package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SuspendForFanOut is the atomic fan-out first-execution suspend: fan-out record
// + checkpoint + parent→waiting + children commit in ONE transaction, closing the
// orphan-on-crash gap. These tests pin the all-or-nothing contract.

func seedRunningLockedParent(t *testing.T, ctx context.Context, s *GormStorage, workerID string) core.UUID {
	t.Helper()
	id := core.NewID()
	until := time.Now().Add(time.Minute)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: id, Type: "wf.run", Queue: "default", Status: core.StatusRunning,
		LockedBy: workerID, LockedUntil: &until,
	}).Error)
	return id
}

func buildFanOutFixtures(parentID core.UUID, n int) (*core.FanOut, *core.Checkpoint, []*core.Job) {
	foID := core.NewID()
	fanOut := &core.FanOut{ID: foID, ParentJobID: parentID, TotalCount: n}
	cp := &core.Checkpoint{JobID: parentID, CallIndex: 0, CallType: "fanout", Result: []byte(`{"fan_out_id":"x"}`)}
	subs := make([]*core.Job, n)
	for i := range subs {
		subs[i] = &core.Job{Type: "wf.sub", Queue: "default", FanOutID: &foID, FanOutIndex: i}
	}
	return fanOut, cp, subs
}

func TestSuspendForFanOut_CommitsAtomically(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	const worker = "w1"
	parentID := seedRunningLockedParent(t, ctx, s, worker)
	fanOut, cp, subs := buildFanOutFixtures(parentID, 3)

	require.NoError(t, s.SuspendForFanOut(ctx, parentID, worker, fanOut, cp, subs))

	// Parent suspended, lock cleared.
	parent, err := s.GetJob(ctx, parentID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, core.StatusWaiting, parent.Status)
	assert.Empty(t, parent.LockedBy)

	// Fan-out record present.
	fo, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, fo)
	assert.Equal(t, 3, fo.TotalCount)

	// All children persisted and dequeue-eligible.
	children, err := s.GetSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Len(t, children, 3)
	for _, c := range children {
		assert.Equal(t, core.StatusPending, c.Status)
	}

	// Resume checkpoint written.
	cps, err := s.GetCheckpoints(ctx, parentID)
	require.NoError(t, err)
	var hasFanoutCP bool
	for _, c := range cps {
		if c.CallType == "fanout" && c.CallIndex == 0 {
			hasFanoutCP = true
		}
	}
	assert.True(t, hasFanoutCP, "fan-out resume checkpoint must be persisted")
}

func TestSuspendForFanOut_AbortsAtomicallyWhenParentNotOwned(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	// Parent is running but locked by a DIFFERENT worker — the ownership-checked
	// status flip must abort the whole transaction, rolling back the fan-out
	// record and checkpoint created earlier in the same tx so nothing is orphaned.
	parentID := seedRunningLockedParent(t, ctx, s, "other-worker")
	fanOut, cp, subs := buildFanOutFixtures(parentID, 3)

	err := s.SuspendForFanOut(ctx, parentID, "w1", fanOut, cp, subs)
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	// Nothing persisted: parent unchanged, no fan-out, no children, no checkpoint.
	parent, err := s.GetJob(ctx, parentID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusRunning, parent.Status, "parent left running")
	assert.Equal(t, "other-worker", parent.LockedBy, "parent lock untouched")

	fo, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Nil(t, fo, "fan-out record rolled back")

	children, err := s.GetSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	assert.Empty(t, children, "no children persisted")

	cps, err := s.GetCheckpoints(ctx, parentID)
	require.NoError(t, err)
	assert.Empty(t, cps, "checkpoint rolled back")
}
