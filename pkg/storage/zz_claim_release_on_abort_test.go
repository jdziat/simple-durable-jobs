package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A claim is durably committed BEFORE the payload is decoded and before the
// batch is re-fetched. Every error exit after that point used to return without
// undoing the claim, leaving the row status='running', locked_by=us and
// undispatched — which reads as healthy work in progress to every queue-depth
// alert, and is reclaimable only by ReleaseStaleLocks (default StaleLockAge: 45
// minutes).
//
// The batch path has released poison rows since the teardown-g3 fix; the
// single-job paths and dequeueBatchLocked's outer loop had not.

// TestDequeue_ReleasesClaimOnPayloadDecodeFailure is the headline guard. Before
// the fix the job stays 'running' and locked forever.
//
// The RETURN contract changed with the poison-skip fix: a row this dequeue could
// not decode is reported (PoisonPayloadDrops) and skipped, not surfaced as the
// call's error — the same shape decodeClaimedBatch has always had, and what lets
// the next claim reach the job behind it. Nothing runnable was found here because
// the only row is poison, so (nil, nil) is the correct "nothing to do".
func TestDequeue_ReleasesClaimOnPayloadDecodeFailure(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	job := &core.Job{ID: core.NewID(), Type: "poison", Queue: "default", Status: core.StatusPending, Args: []byte(`{"a":1}`)}
	require.NoError(t, s.Enqueue(ctx, job))

	// Swap in a codec that fails on the way OUT, so the row is claimed and
	// committed and only then fails to decode.
	s.codec = failingDecodeCodec{}

	before := s.PoisonPayloadDrops()
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-A")
	require.NoError(t, err, "a poison row must not fail the dequeue that steps over it")
	assert.Nil(t, got)
	assert.Equal(t, before+1, s.PoisonPayloadDrops(),
		"the drop must be counted; the counter is what operators alert on")

	// The claim must have been undone.
	s.codec = nil
	var row core.Job
	require.NoError(t, s.db.First(&row, "id = ?", job.ID).Error)

	assert.Equal(t, core.StatusPending, row.Status,
		"a job whose payload failed to decode must be released, not parked as 'running' until the stale-lock reaper")
	assert.Empty(t, row.LockedBy, "locked_by must be cleared on release")
	assert.Nil(t, row.LockedUntil, "locked_until must be cleared on release")
	assert.Zero(t, row.Attempt,
		"the release must give the attempt back; a poison row that burns an attempt per claim dead-letters itself")
}

// TestReleaseClaimedOnAbort_OwnershipFenced proves the release cannot steal a
// row that now belongs to somebody else.
//
// FALSE-GREEN TRAP: a test that releases a row this worker still owns passes
// with or without the `locked_by = ?` predicate. The rows below are owned by a
// DIFFERENT worker and by nobody, which is the case that distinguishes them.
func TestReleaseClaimedOnAbort_OwnershipFenced(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mine := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-A"}
	theirs := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-B"}
	pendingAlready := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", Status: core.StatusPending}
	for _, j := range []*core.Job{mine, theirs, pendingAlready} {
		require.NoError(t, s.db.WithContext(ctx).Create(j).Error)
	}

	cause := errors.New("boom")
	err := s.releaseClaimedOnAbort([]core.UUID{mine.ID, theirs.ID, pendingAlready.ID}, "worker-A", cause)
	require.ErrorIs(t, err, cause, "the original cause must survive so callers can classify it")

	// A fresh destination per lookup: GORM folds a primary key already set on the
	// destination struct into the query as an extra condition, so reusing one
	// var makes the second First silently look for a row matching BOTH ids.
	var gotMine core.Job
	require.NoError(t, s.db.First(&gotMine, "id = ?", mine.ID).Error)
	assert.Equal(t, core.StatusPending, gotMine.Status, "our own claim must be released")

	var gotTheirs core.Job
	require.NoError(t, s.db.First(&gotTheirs, "id = ?", theirs.ID).Error)
	assert.Equal(t, core.StatusRunning, gotTheirs.Status, "another worker's running job must be untouched")
	assert.Equal(t, "worker-B", gotTheirs.LockedBy)
}

// TestReleaseClaimedOnAbort_PreservesContextCanceled pins the property
// pkg/worker/worker.go depends on: its dequeue loop suppresses shutdown noise
// with errors.Is(err, context.Canceled). If the release wrapped the cause in
// something opaque, every clean worker stop would start logging an error.
func TestReleaseClaimedOnAbort_PreservesContextCanceled(t *testing.T) {
	s := newTestStorage(t)

	job := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-A"}
	require.NoError(t, s.db.Create(job).Error)

	err := s.releaseClaimedOnAbort([]core.UUID{job.ID}, "worker-A", context.Canceled)
	assert.ErrorIs(t, err, context.Canceled,
		"worker.go's dequeue loop tests errors.Is(err, context.Canceled) to stay quiet during shutdown")

	var got core.Job
	require.NoError(t, s.db.First(&got, "id = ?", job.ID).Error)
	assert.Equal(t, core.StatusPending, got.Status,
		"a cancelled context is the MOST common way to reach this path (worker shutdown); the release "+
			"runs on a detached context precisely so it still happens")
}

// TestReleaseClaimedOnAbort_NoClaimsIsPassthrough keeps the common case free of
// a pointless UPDATE.
func TestReleaseClaimedOnAbort_NoClaimsIsPassthrough(t *testing.T) {
	s := newTestStorage(t)
	cause := errors.New("boom")
	assert.Equal(t, cause, s.releaseClaimedOnAbort(nil, "worker-A", cause))
}

// TestReleaseClaimedOnAbort_ChunksLargeBatches exercises the >200-id path so the
// chunking loop is not dead code. maxDequeueBatch is 1000, so a full batch
// spans several chunks.
func TestReleaseClaimedOnAbort_ChunksLargeBatches(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	const n = releaseOnAbortChunk*2 + 37
	ids := make([]core.UUID, 0, n)
	for range n {
		j := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-A"}
		require.NoError(t, s.db.WithContext(ctx).Create(j).Error)
		ids = append(ids, j.ID)
	}

	require.Error(t, s.releaseClaimedOnAbort(ids, "worker-A", errors.New("boom")))

	var stillRunning int64
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("id IN ? AND status = ?", ids, core.StatusRunning).Count(&stillRunning).Error)
	assert.Zero(t, stillRunning, "every chunk must be released, not just the first")
}
