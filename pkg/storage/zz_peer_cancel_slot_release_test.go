package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestCancelSubJobs_ReleasesTheCancelledSiblingsConcurrencySlots covers the
// fleet-concurrency-slot release inside the CancelOnFail peer-cancel path, which
// was untested repo-wide: DELETING the statement outright left pkg/storage,
// pkg/fanout and pkg/worker all green.
//
// The release exists because these siblings are being made terminal by a PEER, not
// by their own worker. The comment above it states the reason — a worker that dies
// before its deferred release cannot orphan a live slot for a now-terminal job —
// and without it a cancelled sibling's fleet-wide slot survives until its TTL
// (concurrencySlotTTL, 45 minutes by default). Every cancelled sibling silently
// shrinks the fleet cap for three quarters of an hour, so a cap of 10 admits 10
// minus however many siblings a CancelOnFail fan-out just cancelled.
//
// The bystander leg is here for the reason documented on
// TestScopedMutations_DoNotTouchABystander: this deletes by an IN-list, and with
// only the cancelled jobs' rows present, "delete these jobs' slots" and "delete
// every slot in the table" are indistinguishable — which would release every cap
// in the fleet at once.
func TestCancelSubJobs_ReleasesTheCancelledSiblingsConcurrencySlots(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := core.NewID()
	seedTestJob(t, ctx, s, parent, core.StatusWaiting)
	fanOutID := core.NewID()
	require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
		ID: fanOutID, ParentJobID: parent, TotalCount: 2, Status: core.FanOutPending,
	}))

	sibling := func(name string) core.UUID {
		id := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: id, Type: "sub", Queue: "default", Status: core.StatusRunning,
			LockedBy: "w1", FanOutID: &fanOutID, ParentJobID: &parent,
		}).Error)
		acquireSlotFor(t, ctx, s, name+"-cap-"+string(id), id)
		return id
	}
	c1, c2 := sibling("sib1"), sibling("sib2")

	// An unrelated job holding its own fleet slot. It must be untouched.
	bystander := core.NewID()
	seedTestJob(t, ctx, s, bystander, core.StatusRunning)
	acquireSlotFor(t, ctx, s, "bystander-cap-"+string(bystander), bystander)

	cancelled, err := s.CancelSubJobs(ctx, fanOutID)
	require.NoError(t, err)
	require.ElementsMatch(t, []core.UUID{c1, c2}, cancelled,
		"premise: both running siblings must be cancelled, or the slot assertions below are vacuous")

	for _, id := range []core.UUID{c1, c2} {
		require.Equal(t, int64(0), countRowsForJob(t, s, &core.ConcurrencySlot{}, id),
			"a sibling cancelled by a peer still holds its fleet concurrency slot; nothing else releases it, so it shrinks the fleet cap until its %v TTL expires", 45*time.Minute)
	}
	require.Equal(t, int64(1), countRowsForJob(t, s, &core.ConcurrencySlot{}, bystander),
		"an unrelated job's fleet slot must survive; an unscoped delete here releases every cap in the fleet at once")
}
