package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// worker_id is written on every acquire and renew but was read in NO WHERE
// clause, so ReleaseConcurrencySlot keyed on (slot_name, job_id) alone — which
// does not identify a holder.
//
// The sequence that breaks the cap: worker-A holds a slot for job J; the
// stale-lock reaper reclaims J; worker-B re-acquires the SAME slot for the SAME
// job id; worker-A's deferred release finally runs and deletes B's live row.
// The cap is then under-counted and admits an extra concurrent job.
//
// FALSE-GREEN TRAP: releasing a slot the caller still owns succeeds with or
// without the worker_id predicate. The discriminating case is a release issued
// by a worker that has ALREADY lost the slot to a peer.
func TestReleaseConcurrencySlotOwned_DoesNotDeleteAPeersSlot(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	const slot = "cap:tenant-a"
	jobID := core.NewID()

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobID, "worker-A", 1, time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "worker-A must take the only slot")

	// The reaper hands the job to worker-B, which re-acquires the same slot for
	// the same job id. TryAcquire upserts worker_id, so the row is now B's.
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, jobID, "worker-B", 1, time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "worker-B re-acquires the slot after reclaim")

	// worker-A's deferred release finally runs. It cannot know it lost the job.
	require.NoError(t, s.ReleaseConcurrencySlotOwned(ctx, slot, jobID, "worker-A"))

	var held int64
	require.NoError(t, s.db.Model(&core.ConcurrencySlot{}).
		Where("slot_name = ? AND job_id = ?", slot, jobID).Count(&held).Error)
	assert.Equal(t, int64(1), held,
		"a stale worker's release must NOT delete the slot row its successor is holding — "+
			"deleting it under-counts the cap and admits an extra concurrent job")

	var owner string
	require.NoError(t, s.db.Model(&core.ConcurrencySlot{}).
		Where("slot_name = ? AND job_id = ?", slot, jobID).
		Select("worker_id").Scan(&owner).Error)
	assert.Equal(t, "worker-B", owner, "the surviving row must still belong to the live holder")
}

// TestReleaseConcurrencySlotOwned_ReleasesOwnSlot is the positive control: the
// fence must not be so tight that a legitimate holder cannot let go.
func TestReleaseConcurrencySlotOwned_ReleasesOwnSlot(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	const slot = "cap:tenant-b"
	jobID := core.NewID()

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobID, "worker-A", 1, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, s.ReleaseConcurrencySlotOwned(ctx, slot, jobID, "worker-A"))

	var held int64
	require.NoError(t, s.db.Model(&core.ConcurrencySlot{}).
		Where("slot_name = ? AND job_id = ?", slot, jobID).Count(&held).Error)
	assert.Zero(t, held, "the real holder must be able to release")

	// And the freed capacity is genuinely reusable.
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, core.NewID(), "worker-C", 1, time.Minute)
	require.NoError(t, err)
	assert.True(t, ok, "releasing must return capacity to the cap, not merely delete a row")
}

// TestConcurrencySlotsJobIDIndexExists pins migration v37. Both the slot release
// and the terminal job write delete by job_id; without this index each one
// scanned the whole (deliberately unbounded) table, and on MySQL under
// REPEATABLE READ the unindexed DELETE also took a next-key lock across the
// scanned range.
func TestConcurrencySlotsJobIDIndexExists(t *testing.T) {
	s := newTestStorage(t)
	assert.True(t, s.db.Migrator().HasIndex(&core.ConcurrencySlot{}, "idx_concurrency_slots_job_id"),
		"migration v37 must create idx_concurrency_slots_job_id")
}
