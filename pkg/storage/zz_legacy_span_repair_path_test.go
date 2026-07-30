package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestLegacyCallSpanRepairPath pins what an operator can actually DO about a
// listed job, because the runbook's stated repair cannot run on one.
//
// FindLegacyCallSpanJobs excludes completed, cancelled and failed — every listed
// job is non-terminal by construction. Requeue (gorm.go, and the dashboard's
// RetryJob) returns early unless the status IS failed or cancelled. The two sets
// are disjoint, so "requeueing a listed job is the repair" is inert on EVERY
// listed job, true positives included — and the per-run WARN says the same thing.
//
// This is not an over-approximation artifact that a narrower predicate fixes. It
// is a gap in the runbook: the detector deliberately lists live work, and the
// only checkpoint-clearing operation deliberately refuses live work.
//
// The path that does work is cancel-then-requeue, and it restarts the workflow
// from scratch rather than resuming it, so it is only appropriate for work whose
// steps are safe to re-run. Both legs are asserted here so the guidance in
// UPGRADE.md is executable rather than plausible.
func TestLegacyCallSpanRepairPath(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// A listed job: non-terminal, two legacy Call checkpoints.
	suspect := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, suspect, 0, "child", 0)
	seedCheckpoint(t, s, suspect, 1, "leaf", 0)

	listed, err := s.FindLegacyCallSpanJobs(ctx, 100)
	require.NoError(t, err)
	found := false
	for _, l := range listed {
		if l.JobID == suspect {
			found = true
		}
	}
	require.True(t, found, "premise: the job must be listed, or the rest of this test proves nothing")

	// LEG 1 — the documented repair is inert on a listed job.
	requeued, err := s.Requeue(ctx, suspect)
	require.NoError(t, err)
	require.False(t, requeued,
		"Requeue now succeeds on a listed (non-terminal) job. If that is intentional, UPGRADE.md's repair instruction is finally executable as written and this test should assert the new behaviour; until then the runbook must not tell operators to requeue a listed job")
	require.Equal(t, int64(2), countRowsForJob(t, s, &core.Checkpoint{}, suspect),
		"an inert Requeue must not have cleared checkpoints either")

	// LEG 2 — cancel first, then requeue. This is the path UPGRADE.md documents.
	require.NoError(t, s.CancelJobTerminal(ctx, suspect))
	requeued, err = s.Requeue(ctx, suspect)
	require.NoError(t, err)
	require.True(t, requeued,
		"cancel-then-requeue is the repair path UPGRADE.md now documents; if it stops working the documentation is wrong again")
	require.Equal(t, int64(0), countRowsForJob(t, s, &core.Checkpoint{}, suspect),
		"the point of the repair is to clear the legacy checkpoints")

	row, err := s.GetJob(ctx, suspect)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, core.StatusPending, row.Status, "the repaired job should be runnable again")
}
