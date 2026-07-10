package ui

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
)

// TestListScheduledJobs_NextRunIsFutureAfterMissedBoundaries guards C-03: the
// UI's NextRun must be the next FUTURE fire. It anchors on the persisted last run
// like the scheduler, but a single Schedule.Next(lastRun) step returns a boundary
// already in the PAST when the last run is more than one interval behind now
// (worker down / schedule paused). The UI must walk forward past the missed
// boundaries — mirroring the scheduler's seedLastRun catch-up — so an operator
// viewing the dashboard during an outage never sees a stale past timestamp
// labeled as the next run.
func TestListScheduledJobs_NextRunIsFutureAfterMissedBoundaries(t *testing.T) {
	ctx := context.Background()
	svc, q := setupServiceWithQueue(t)
	registerScheduledTestHandler(q, "hourly")
	sched := schedule.Every(time.Hour)
	require.NoError(t, q.Schedule("hourly", nil, sched))

	// Claim a fire 90m in the past: a single Next(lastRun) step lands on a
	// boundary ~30m in the past — the missed-boundary case the old one-step code
	// reported verbatim as "next".
	lastRun := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Second)
	require.False(t, sched.Next(lastRun).After(time.Now()),
		"test premise: a single Next(lastRun) step must land in the past")
	_, err := svc.storage.(interface {
		SeedScheduledFire(context.Context, string, time.Time) (time.Time, error)
	}).SeedScheduledFire(ctx, "hourly", lastRun.Add(-time.Hour))
	require.NoError(t, err)
	claimed, err := svc.storage.ClaimScheduledFire(ctx, "hourly", lastRun)
	require.NoError(t, err)
	require.True(t, claimed)

	callTime := time.Now()
	resp, err := svc.ListScheduledJobs(ctx, connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	job := resp.Msg.Jobs[0]
	require.NotNil(t, job.LastRun, "LastRun must remain the persisted last fire")
	assert.Equal(t, lastRun.Unix(), job.LastRun.AsTime().Unix())
	require.NotNil(t, job.NextRun)

	nextRun := job.NextRun.AsTime()
	assert.True(t, nextRun.After(callTime),
		"NextRun must be the next FUTURE fire, not a stale past boundary")
	assert.LessOrEqual(t, nextRun.Sub(callTime), time.Hour+2*time.Second,
		"NextRun must be the immediate next boundary (within one interval), not skipped ahead")
}

// phaseSchedule fires at a fixed offset relative to its anchor, WITHOUT epoch
// truncation, so its Next depends on the anchor's phase. Anchoring on
// max(lastRun, now) would shift that phase; a forward walk from lastRun preserves
// it. It exists to lock in that the UI walks (matching the scheduler) rather than
// re-phasing from now.
type phaseSchedule struct{ interval time.Duration }

func (p phaseSchedule) Next(from time.Time) time.Time { return from.Add(p.interval) }

// TestListScheduledJobs_NextRunPreservesSchedulePhase guards that the forward walk
// preserves the last-run phase for an anchor-relative schedule — the reason to
// walk rather than anchor on max(lastRun, now).
func TestListScheduledJobs_NextRunPreservesSchedulePhase(t *testing.T) {
	ctx := context.Background()
	svc, q := setupServiceWithQueue(t)
	registerScheduledTestHandler(q, "phased")
	sched := phaseSchedule{interval: time.Hour}
	require.NoError(t, q.Schedule("phased", nil, sched))

	// lastRun at an off-boundary phase 137m ago, so its boundaries (lastRun +
	// k*1h) straddle now with clear margin (nearest ~43m ahead / ~17m behind) —
	// no boundary sits within clock-skew of now.
	lastRun := time.Now().UTC().Add(-137 * time.Minute).Truncate(time.Second)
	_, err := svc.storage.(interface {
		SeedScheduledFire(context.Context, string, time.Time) (time.Time, error)
	}).SeedScheduledFire(ctx, "phased", lastRun.Add(-time.Hour))
	require.NoError(t, err)
	claimed, err := svc.storage.ClaimScheduledFire(ctx, "phased", lastRun)
	require.NoError(t, err)
	require.True(t, claimed)

	callTime := time.Now()
	resp, err := svc.ListScheduledJobs(ctx, connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	require.NotNil(t, resp.Msg.Jobs[0].NextRun)
	nextRun := resp.Msg.Jobs[0].NextRun.AsTime()

	// Expected: the first lastRun + k*interval strictly after now — phase preserved.
	want := sched.Next(lastRun)
	for !want.After(callTime) {
		want = sched.Next(want)
	}
	assert.Equal(t, want.Unix(), nextRun.Unix(),
		"NextRun must preserve the last-run phase (lastRun + k*interval), not re-phase from now")
	assert.True(t, nextRun.After(callTime), "NextRun must be in the future")
	// max(lastRun, now) would give now+1h, a materially different instant, so this
	// guards the walk against a future 'simplification' to Next(max(lastRun, now)).
	assert.NotEqual(t, callTime.Add(time.Hour).Unix(), nextRun.Unix())
}

func TestListScheduledJobs_ScheduleHealth(t *testing.T) {
	ctx := context.Background()
	interval := 10 * time.Minute
	now := time.Now().UTC()
	boundary := now.Truncate(interval)
	thresholdWithinCurrentBoundary := now.Sub(boundary) + interval/2

	cases := []struct {
		name             string
		jobName          string
		lastRun          *time.Time
		threshold        time.Duration
		wantOverdue      bool
		wantMissedFires  int64
		wantExpectedLast *time.Time
		wantExpectedNil  bool
	}{
		{
			name:            "healthy within grace",
			jobName:         "healthyWithinGrace",
			lastRun:         timePtr(boundary.Add(-interval)),
			threshold:       thresholdWithinCurrentBoundary,
			wantOverdue:     false,
			wantMissedFires: 0,
		},
		{
			name:             "overdue after three missed fires",
			jobName:          "overdueAfterMissed",
			lastRun:          timePtr(boundary.Add(-4 * interval)),
			threshold:        thresholdWithinCurrentBoundary,
			wantOverdue:      true,
			wantMissedFires:  3,
			wantExpectedLast: timePtr(boundary),
		},
		{
			name:            "never fired",
			jobName:         "neverFired",
			threshold:       thresholdWithinCurrentBoundary,
			wantOverdue:     false,
			wantMissedFires: 0,
			wantExpectedNil: true,
		},
		{
			name:            "threshold disabled",
			jobName:         "thresholdDisabled",
			lastRun:         timePtr(boundary.Add(-4 * interval)),
			threshold:       0,
			wantOverdue:     false,
			wantMissedFires: 0,
			wantExpectedNil: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, q := setupServiceWithQueue(t)
			svc.scheduleOverdueThreshold = tc.threshold
			registerScheduledTestHandler(q, tc.jobName)
			require.NoError(t, q.Schedule(tc.jobName, nil, schedule.Every(interval)))

			if tc.lastRun != nil {
				seedScheduledTestFire(t, ctx, svc, tc.jobName, tc.lastRun.Add(-interval))
				claimed, err := svc.storage.ClaimScheduledFire(ctx, tc.jobName, *tc.lastRun)
				require.NoError(t, err)
				require.True(t, claimed)
			}

			callTime := time.Now()
			resp, err := svc.ListScheduledJobs(ctx, connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
			require.NoError(t, err)
			require.Len(t, resp.Msg.Jobs, 1)
			job := resp.Msg.Jobs[0]

			assert.Equal(t, tc.wantOverdue, job.Overdue)
			assert.Equal(t, tc.wantMissedFires, job.MissedFires)
			require.NotNil(t, job.NextRun)
			assert.True(t, job.NextRun.AsTime().After(callTime), "NextRun must remain the next future boundary")

			if tc.wantExpectedNil {
				assert.Nil(t, job.ExpectedLastRun)
			}
			if tc.wantExpectedLast != nil {
				require.NotNil(t, job.ExpectedLastRun)
				assert.Equal(t, tc.wantExpectedLast.Unix(), job.ExpectedLastRun.AsTime().Unix())
			}
		})
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func seedScheduledTestFire(t *testing.T, ctx context.Context, svc *jobsService, name string, fireTime time.Time) {
	t.Helper()
	_, err := svc.storage.(interface {
		SeedScheduledFire(context.Context, string, time.Time) (time.Time, error)
	}).SeedScheduledFire(ctx, name, fireTime)
	require.NoError(t, err)
}

// TestAggregateStrategy guards C-04: a workflow whose fan-outs do not all share a
// strategy is reported as "mixed" rather than silently taking the first fan-out's
// strategy.
func TestAggregateStrategy(t *testing.T) {
	assert.Equal(t, "collect_all", aggregateStrategy([]*core.FanOut{
		{Strategy: core.StrategyCollectAll},
		{Strategy: core.StrategyCollectAll},
	}), "uniform strategy is reported as-is")

	assert.Equal(t, "threshold", aggregateStrategy([]*core.FanOut{
		{Strategy: core.StrategyThreshold},
	}), "a single fan-out reports its own strategy")

	assert.Equal(t, strategyMixed, aggregateStrategy([]*core.FanOut{
		{Strategy: core.StrategyCollectAll},
		{Strategy: core.StrategyFailFast},
	}), "differing strategies aggregate to mixed")

	assert.Equal(t, strategyMixed, aggregateStrategy([]*core.FanOut{
		{Strategy: core.StrategyFailFast},
		{Strategy: core.StrategyFailFast},
		{Strategy: core.StrategyThreshold},
	}), "a late-differing strategy still aggregates to mixed")
}
