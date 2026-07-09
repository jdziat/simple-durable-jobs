package storage

import (
	"context"
	"sync"
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

// TestCancelJobTerminal_ConcurrentDescendantSuspend_NoOrphanedGrandchildren is
// the E4b regression: a descendant that calls SuspendForFanOut (creating a new
// fan-out + grandchildren) concurrently with a subtree terminal-cancel must not
// leave those grandchildren runnable under the cancelled root. SERIALIZABLE
// isolation (PG SSI / MySQL next-key locks) closes the phantom-insert window.
// Skipped on SQLite, which serializes writers so the interleaving cannot occur
// (and in-memory SQLite does not model concurrent writers faithfully). Runs
// several rounds to give the race a chance on PG/MySQL.
func TestCancelJobTerminal_ConcurrentDescendantSuspend_NoOrphanedGrandchildren(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if s.dialect() == dialectSQLite {
		t.Skip("concurrent-suspend race requires a real PG/MySQL backend")
	}

	for round := 0; round < 6; round++ {
		const worker = "w-desc"
		rootID := core.NewID()
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: rootID, Type: "wf.root", Queue: "default", Status: core.StatusWaiting,
		}).Error)
		fanAID := core.NewID()
		require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{ID: fanAID, ParentJobID: rootID, TotalCount: 1, Status: core.FanOutPending}))
		until := time.Now().Add(time.Minute)
		dID := core.NewID()
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: dID, Type: "wf.desc", Queue: "default", Status: core.StatusRunning,
			LockedBy: worker, LockedUntil: &until, ParentJobID: &rootID, RootJobID: &rootID, FanOutID: &fanAID,
		}).Error)

		fanBID := core.NewID()
		fanB := &core.FanOut{ID: fanBID, ParentJobID: dID, TotalCount: 1, Status: core.FanOutPending}
		cp := &core.Checkpoint{JobID: dID, CallIndex: 0, CallType: "fanout", Result: []byte(`{"fan_out_id":"x"}`)}
		grand := &core.Job{ID: core.NewID(), Type: "wf.grand", Queue: "default", ParentJobID: &dID, RootJobID: &rootID, FanOutID: &fanBID, FanOutIndex: 0}

		var wg sync.WaitGroup
		wg.Add(2)
		// Both may error under contention (serialization abort / ErrJobNotOwned);
		// the invariant below must hold regardless of who wins.
		go func() { defer wg.Done(); _ = s.CancelJobTerminal(ctx, rootID) }()
		go func() { defer wg.Done(); _ = s.SuspendForFanOut(ctx, dID, worker, fanB, cp, []*core.Job{grand}) }()
		wg.Wait()

		// The root is terminally cancelled (CancelJobTerminal retries to success).
		gotRoot, err := s.GetJob(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, core.StatusCancelled, gotRoot.Status, "round %d", round)

		// Invariant: no grandchild of the cancelled subtree survives runnable —
		// either the suspend rolled back (fanB/grand never committed) or the
		// cancel caught them (grand cancelled).
		var liveGrand int64
		require.NoError(t, s.DB().Model(&core.Job{}).
			Where("fan_out_id = ?", fanBID).
			Where("status NOT IN ?", []core.JobStatus{core.StatusCompleted, core.StatusFailed, core.StatusCancelled}).
			Count(&liveGrand).Error)
		assert.Zero(t, liveGrand, "round %d: a grandchild of the cancelled subtree survived runnable", round)
	}
}
