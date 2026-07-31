package ui

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
	"github.com/stretchr/testify/require"
)

// densePathologicalSchedule advances by ONE NANOSECOND per step and counts how
// many times the UI asks. A real schedule is far coarser; this is the shape the
// cap exists for — a schedule dense enough that walking from an old last-run to
// "now" is effectively unbounded.
type densePathologicalSchedule struct{ calls atomic.Int64 }

func (d *densePathologicalSchedule) Next(from time.Time) time.Time {
	d.calls.Add(1)
	return from.Add(time.Nanosecond)
}

func TestListScheduledJobs_ForwardWalkIsBounded(t *testing.T) {
	const name = "dense"
	ctx := context.Background()
	sched := &densePathologicalSchedule{}

	svc, q := setupServiceWithQueue(t)
	registerScheduledTestHandler(q, name)
	require.NoError(t, q.Schedule(name, nil, sched))

	// A persisted last fire an hour back. At one nanosecond per boundary that is
	// 3.6e12 boundaries to walk, which is the point.
	lastRun := time.Now().UTC().Add(-time.Hour)
	_, err := svc.storage.(interface {
		SeedScheduledFire(context.Context, string, time.Time) (time.Time, error)
	}).SeedScheduledFire(ctx, name, lastRun.Add(-time.Hour))
	require.NoError(t, err)
	claimed, err := svc.storage.ClaimScheduledFire(ctx, name, lastRun)
	require.NoError(t, err)
	require.True(t, claimed, "premise: the last fire must be persisted, or there is no backlog to walk")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = svc.ListScheduledJobs(ctx, connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("ListScheduledJobs did not return; the forward walk made %d Schedule.Next calls and is unbounded, so the dashboard request never completes", sched.calls.Load())
	}

	calls := sched.calls.Load()
	require.LessOrEqual(t, calls, int64(nextRunCatchUpCap)+10,
		"the forward walk made %d Schedule.Next calls against a cap of %d; without the bound a dense schedule with an old last run spins the request handler", calls, nextRunCatchUpCap)
	require.Positive(t, calls, "premise: the walk must actually have run, or this asserts nothing")
}
