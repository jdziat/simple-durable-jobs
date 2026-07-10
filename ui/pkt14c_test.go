package ui

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
)

// TestListScheduledJobs_NextRunAnchoredOnLastRun guards C-03: the UI's NextRun
// must be computed from the persisted last run (Schedule.Next(lastRun)), the same
// anchor the scheduler uses, not Schedule.Next(now). Anchoring on now makes the UI
// disagree with when the job will actually fire for any schedule that has run.
func TestListScheduledJobs_NextRunAnchoredOnLastRun(t *testing.T) {
	ctx := context.Background()
	svc, q := setupServiceWithQueue(t)
	registerScheduledTestHandler(q, "hourly")
	sched := schedule.Every(time.Hour)
	require.NoError(t, q.Schedule("hourly", nil, sched))

	// Claim a fire well in the past so lastRun is clearly distinct from now.
	lastRun := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Second)
	_, err := svc.storage.(interface {
		SeedScheduledFire(context.Context, string, time.Time) (time.Time, error)
	}).SeedScheduledFire(ctx, "hourly", lastRun.Add(-time.Hour))
	require.NoError(t, err)
	claimed, err := svc.storage.ClaimScheduledFire(ctx, "hourly", lastRun)
	require.NoError(t, err)
	require.True(t, claimed)

	resp, err := svc.ListScheduledJobs(ctx, connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	job := resp.Msg.Jobs[0]
	require.NotNil(t, job.LastRun)
	require.NotNil(t, job.NextRun)

	want := sched.Next(lastRun)
	assert.Equal(t, want.Unix(), job.NextRun.AsTime().Unix(),
		"NextRun must be Schedule.Next(lastRun), matching the scheduler — not Schedule.Next(now)")
	// Sanity: with a lastRun 90m ago and an hourly schedule, the now-anchored value
	// the old code produced is a different instant, so this test is meaningful.
	require.NotEqual(t, sched.Next(time.Now()).Unix(), want.Unix(),
		"test setup must make lastRun-anchored and now-anchored NextRun differ")
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
