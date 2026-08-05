package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// completeJobAt drives jobID to completed with an explicit completed_at so the
// retention sweep's age predicate matches deterministically.
func completeJobAt(t *testing.T, s *GormStorage, jobID core.UUID, at time.Time) {
	t.Helper()
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", jobID).
		Updates(map[string]any{
			"status":       core.StatusCompleted,
			"completed_at": at.UTC(),
		}).Error)
}

func countJobRows(t *testing.T, s *GormStorage, jobID core.UUID) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", jobID).Count(&n).Error)
	return n
}

func countLockRows(t *testing.T, s *GormStorage, scopeHash string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Model(&core.UniqueLock{}).Where("scope_hash = ?", scopeHash).Count(&n).Error)
	return n
}

// TestRetention_KeepsJobWhileIdempotencyWindowLive pins the documented contract
// for IdempotencyKey/UniqueFor: the window keeps deduplicating until its OWN
// expires_at, even after the original job completed and even when the retention
// window is shorter than the TTL. Before the fix the 30-day (or DefaultRetention's
// 7-day) sweep deleted the completed job row AND its unique_locks row, so a
// replayed request re-ran the guarded work — a double charge.
func TestRetention_KeepsJobWhileIdempotencyWindowLive(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := scope64("idem-live")

	firstID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, 90*24*time.Hour)
	require.NoError(t, err)

	// Job finished two hours ago; the retention window is one hour.
	completeJobAt(t, s, firstID, time.Now().Add(-2*time.Hour))

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	require.Equal(t, int64(0), deleted,
		"retention must not delete a job pinned by a still-live idempotency window")
	require.Equal(t, int64(1), countLockRows(t, s, scope), "the live window must survive retention")
	require.Equal(t, int64(1), countJobRows(t, s, firstID), "the deduped-to job row must survive too")

	// The replayed request must dedup to the ORIGINAL job, not run a second time.
	secondID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, 90*24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, firstID, secondID,
		"the windowed guard must keep deduplicating until its TTL expires")

	var jobRows int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("type = ?", "charge").Count(&jobRows).Error)
	require.Equal(t, int64(1), jobRows, "no second job row may be inserted inside the window")
}

// TestRetention_DeletesJobOnceUniqueLockExpired proves the pin is scoped to LIVE
// windows only: once the lock's own expires_at has passed, retention collects the
// job row and the lock row exactly as before. This is the property that stops the
// fix from turning a bounded table into an unbounded one.
func TestRetention_DeletesJobOnceUniqueLockExpired(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := scope64("idem-dead")

	jobID := core.NewID()
	require.NoError(t, s.db.Create(&core.Job{
		ID: jobID, Type: "charge", Queue: "q", Status: core.StatusCompleted,
		Args: []byte(`{}`),
	}).Error)
	completed := time.Now().Add(-2 * time.Hour).UTC()
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", jobID).
		Update("completed_at", completed).Error)
	require.NoError(t, s.db.Create(&core.UniqueLock{
		ScopeHash: scope, JobID: jobID,
		ExpiresAt: time.Now().Add(-time.Minute).UTC(), // already expired
	}).Error)

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "an EXPIRED window must not pin its job row")
	require.Equal(t, int64(0), countJobRows(t, s, jobID))
	require.Equal(t, int64(0), countLockRows(t, s, scope), "the expired lock is collected with the job")
}

// TestRetention_UnlockedTerminalJobsStillCollected is the bystander guard: a
// completed job with NO unique lock at all must still be swept. Without it the
// pin could be written as an unconditional skip and every test above would still
// pass.
func TestRetention_UnlockedTerminalJobsStillCollected(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// One pinned job (live window) and one plain completed job.
	pinnedID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope64("idem-bystander"), 90*24*time.Hour)
	require.NoError(t, err)
	completeJobAt(t, s, pinnedID, time.Now().Add(-2*time.Hour))

	plainID := core.NewID()
	require.NoError(t, s.db.Create(&core.Job{
		ID: plainID, Type: "plain", Queue: "q", Status: core.StatusCompleted, Args: []byte(`{}`),
	}).Error)
	completeJobAt(t, s, plainID, time.Now().Add(-2*time.Hour))

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.Equal(t, int64(0), countJobRows(t, s, plainID), "unpinned terminal jobs are still collected")
	require.Equal(t, int64(1), countJobRows(t, s, pinnedID), "only the pinned job is retained")
}

// TestUniqueLock_LiveWindowWithMissingJobIsNotStolen closes the interaction the
// finder flagged: stealTerminalUniqueLock used to treat "the referenced job row
// is gone" as proof the guarded work never ran, and stole a still-live window —
// so merely retaining the lock row would not have stopped the double charge.
// Releasing a window is now an explicit act of whoever deletes the job, never an
// inference from a missing row.
func TestUniqueLock_LiveWindowWithMissingJobIsNotStolen(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := scope64("idem-missing")

	firstID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, 90*24*time.Hour)
	require.NoError(t, err)
	completeJobAt(t, s, firstID, time.Now().Add(-2*time.Hour))

	// Delete ONLY the job row, leaving the live lock behind (what any future or
	// third-party sweep that forgets the pin would do).
	require.NoError(t, s.db.Where("id = ?", firstID).Delete(&core.Job{}).Error)
	require.Equal(t, int64(1), countLockRows(t, s, scope))

	secondID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, 90*24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, firstID, secondID,
		"a live window whose job row vanished must keep deduplicating, not be stolen")

	var jobRows int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("type = ?", "charge").Count(&jobRows).Error)
	require.Equal(t, int64(0), jobRows, "no duplicate job may be admitted inside a live window")
}

// TestUniqueLock_ExpiredWindowWithMissingJobIsReacquired proves the previous
// test did not simply wedge the scope forever: once the window expires, the
// ordinary acquire path takes over and fresh work is admitted.
func TestUniqueLock_ExpiredWindowWithMissingJobIsReacquired(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := scope64("idem-expired-missing")

	gone := core.NewID()
	require.NoError(t, s.db.Create(&core.UniqueLock{
		ScopeHash: scope, JobID: gone, ExpiresAt: time.Now().Add(-time.Minute).UTC(),
	}).Error)

	newID := core.NewID()
	got, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: newID, Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, time.Hour)
	require.NoError(t, err)
	require.Equal(t, newID, got, "an EXPIRED window must admit fresh work")
	require.Equal(t, int64(1), countJobRows(t, s, newID))
}

// TestUniqueLock_FailedJobStillStealable pins the pre-existing self-healing that
// the missing-row change must not regress: a window whose job FAILED or was
// CANCELLED is still stolen, because that work will never complete.
func TestUniqueLock_FailedJobStillStealable(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for _, status := range []core.JobStatus{core.StatusFailed, core.StatusCancelled} {
		scope := scope64("steal-" + string(status))
		firstID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
			ID: core.NewID(), Type: "charge-" + string(status), Queue: "q", Args: []byte(`{}`),
		}, scope, 90*24*time.Hour)
		require.NoError(t, err)
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", firstID).
			Update("status", status).Error)

		secondID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
			ID: core.NewID(), Type: "charge-" + string(status), Queue: "q", Args: []byte(`{}`),
		}, scope, 90*24*time.Hour)
		require.NoError(t, err)
		require.NotEqual(t, firstID, secondID,
			"a %s job's window is dead work and must still be stealable", status)
	}
}

// TestUniqueLock_ActiveUniqueCollisionReleasesWindow covers the one case that
// USED to depend on "a missing job row is stealable": winning the windowed lock
// and then losing the separate active-unique index race, so the lock references a
// job that was never inserted. Driven through enqueueWithUniqueLockDB on the bare
// handle (no enclosing transaction) because that is what the caller-owned-tx path
// looks like when the caller commits despite the error. The window must be
// released explicitly, so the scope is immediately reusable instead of wedged for
// the whole TTL.
func TestUniqueLock_ActiveUniqueCollisionReleasesWindow(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := scope64("idem-activedup")

	// An ACTIVE job already holds the active-only unique key.
	require.NoError(t, s.EnqueueUnique(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, "active-k"))

	loser := &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
		UniqueKey: "active-k",
	}
	fillEnqueueDefaults(loser)
	_, err := s.enqueueWithUniqueLockDB(ctx, s.db, loser, scope, 90*24*time.Hour)
	require.ErrorIs(t, err, core.ErrDuplicateJob)
	require.Equal(t, int64(0), countJobRows(t, s, loser.ID), "the losing job row is not inserted")
	require.Equal(t, int64(0), countLockRows(t, s, scope),
		"a window guarding work that was never inserted must be released, not left to expire")
}

// TestDeleteWorkflowSubtree_ReleasesUniqueLocks covers the other two explicit
// deletion sites: the root's own window (DeleteWorkflowSubtree) and the
// descendants' (deleteFanOutSubtree, shared with Requeue's replay reset). Without
// them a removed workflow would leave live windows pointing at rows that no
// longer exist, and — since a missing job row no longer steals — those scopes
// would refuse re-enqueue for the rest of their TTL.
func TestDeleteWorkflowSubtree_ReleasesUniqueLocks(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	parentID, subIDs := seedFanOutTree(t, s, "q", core.StatusCompleted, 3)
	require.NotEmpty(t, subIDs)

	rootScope := scope64("subtree-root")
	require.NoError(t, s.db.Create(&core.UniqueLock{
		ScopeHash: rootScope, JobID: parentID, ExpiresAt: time.Now().Add(90 * 24 * time.Hour),
	}).Error)
	childScope := scope64("subtree-child")
	require.NoError(t, s.db.Create(&core.UniqueLock{
		ScopeHash: childScope, JobID: subIDs[0], ExpiresAt: time.Now().Add(90 * 24 * time.Hour),
	}).Error)

	require.NoError(t, s.DeleteWorkflowSubtree(ctx, parentID))

	require.Equal(t, int64(0), countLockRows(t, s, rootScope), "root window released")
	require.Equal(t, int64(0), countLockRows(t, s, childScope), "sub-job window released")
}

// TestDeleteJob_ReleasesUniqueLock proves an operator's explicit delete releases
// the window with the row, so re-enqueue works without relying on the removed
// missing-row inference.
func TestDeleteJob_ReleasesUniqueLock(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := scope64("idem-deletejob")

	firstID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, 90*24*time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", firstID).
		Update("status", core.StatusCompleted).Error)

	require.NoError(t, s.DeleteJob(ctx, firstID))
	require.Equal(t, int64(0), countLockRows(t, s, scope),
		"DeleteJob must release the deleted job's window, not strand it")

	newID := core.NewID()
	got, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: newID, Type: "charge", Queue: "q", Args: []byte(`{}`),
	}, scope, 90*24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, newID, got, "re-enqueue after an explicit delete admits fresh work")
}
