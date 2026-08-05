package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestReleaseStaleLocks_OnlyReclaimsRunningJobs covers the `status = 'running'`
// scope on the reaper, which had no coverage on ANY dialect.
//
// ReleaseStaleLocks asserts the predicate twice — once on the locking pluck and
// again on the UPDATE as documented defense-in-depth. Before this test, removing
// EITHER copy left the whole pkg/storage suite green, and so did removing BOTH:
// no test contained a stale non-running job at all.
//
// Measured, so the next reader knows what this test does and does not pin:
//
//	pluck copy removed            -> this test FAILS (it is the load-bearing one)
//	UPDATE re-assert removed      -> still passes, and correctly so: the pluck has
//	                                 already excluded non-running rows, so the
//	                                 re-assert is unobservable while its twin holds
//	both copies removed           -> this test FAILS
//
// The re-assert is therefore genuine defense-in-depth rather than coverage this
// test is missing; it can only matter if a future change drops the row lock or
// the pluck predicate, which is exactly the scenario it exists for.
//
// What the guard prevents is severe. The reaper's other predicate selects purely
// on staleness — COALESCE(last_heartbeat_at, started_at, locked_until) < cutoff —
// and a terminal job's timestamps only ever recede, so every completed, failed
// and cancelled job in the database eventually satisfies it. Only the status
// scope stops the reaper from flipping those rows back to `pending` with
// dq_ready set, which is a queue that resurrects and re-executes finished work,
// and silently unpauses paused jobs.
//
// The positive leg runs in the same test on the same storage so this cannot go
// vacuous: if a future change stopped the reaper reclaiming anything at all, the
// stale RUNNING job would fail to be released and the test would fail rather
// than passing for the wrong reason.
func TestReleaseStaleLocks_OnlyReclaimsRunningJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	ancient := time.Now().Add(-72 * time.Hour)

	// Every non-running status, each as stale as the reaper's cutoff can see.
	// A terminal job's timestamps only recede, so in a long-lived deployment
	// these are the normal state of the table, not a contrived one.
	spared := map[core.JobStatus]core.UUID{}
	for _, status := range []core.JobStatus{
		core.StatusCompleted,
		core.StatusFailed,
		core.StatusCancelled,
		core.StatusPending,
		core.StatusWaiting,
		core.StatusPaused,
	} {
		id := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: id, Type: "wf", Queue: "default", Status: status,
			LockedBy: "worker-dead", StartedAt: &ancient, LockedUntil: &ancient,
		}).Error)
		spared[status] = id
	}

	// Positive leg: a genuinely stale RUNNING job, which the reaper must reclaim.
	// This keeps the assertions below from passing merely because the reaper
	// stopped working.
	stuck := core.NewID()
	require.NoError(t, s.db.Create(&core.Job{
		ID: stuck, Type: "wf", Queue: "default", Status: core.StatusRunning,
		LockedBy: "worker-dead", StartedAt: &ancient, LockedUntil: &ancient,
	}).Error)

	released, err := s.ReleaseStaleLocks(ctx, time.Hour)
	require.NoError(t, err)

	require.Equal(t, []core.UUID{stuck}, released,
		"the reaper must reclaim exactly the stale RUNNING job; any other id here is a row it had no business touching")

	for status, id := range spared {
		row, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, status, row.Status,
			"the reaper changed the status of a stale %s job to %s; without the status scope it selects on staleness alone, so it resurrects finished work to pending and re-executes it, and unpauses paused jobs",
			status, row.Status)
	}

	row, err := s.GetJob(ctx, stuck)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, core.StatusPending, row.Status, "the stale running job should have been reclaimed")
	require.Empty(t, row.LockedBy)
}
