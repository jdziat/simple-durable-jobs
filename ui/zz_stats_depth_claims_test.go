package ui

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exists because three behaviours the depth-collector diff STATES in
// its comments had no test at all: reverting each one left the whole ui package
// green. A comment asserting a property, with nothing checking it, is how the
// property quietly stops being true.

// scanCapStorage has no aggregate capability and always returns exactly the scan
// cap, which is what a queue deeper than the cap looks like to the collector.
type scanCapStorage struct {
	core.Storage
	scanCalls atomic.Int64
}

func (s *scanCapStorage) GetJobsByStatus(_ context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	s.scanCalls.Add(1)
	if status != core.StatusPending {
		return nil, nil
	}
	jobs := make([]*core.Job, limit)
	for i := range jobs {
		jobs[i] = &core.Job{ID: core.NewID(), Queue: "deep", Type: "work", Status: core.StatusPending}
	}
	return jobs, nil
}

// TestStatsCollector_LogsWhenTheScanTruncates covers the claim that "the
// truncation is now logged rather than silent".
//
// FALSE-GREEN TRAP: every existing collector test uses a storage with far fewer
// jobs than the cap, so the branch is never entered — gating the whole WARN off
// with `if false` left ./ui green. The discriminating input is a page that comes
// back exactly AT the cap, which is indistinguishable from "there are more".
//
// This matters because the number the dashboard then shows is wrong during
// exactly the incident an operator is looking at it for. Wrong-and-loud is
// recoverable; wrong-and-silent is not.
func TestStatsCollector_LogsWhenTheScanTruncates(t *testing.T) {
	_, statsStore, _ := setupCollectorTest(t)
	ctx := context.Background()

	store := &scanCapStorage{}
	_, isAggregator := core.Storage(store).(queueDepthOnlyStorage)
	require.False(t, isAggregator, "this test must exercise the row-scan fallback")

	var logs bytes.Buffer
	NewStatsCollector(queue.New(store), statsStore,
		WithStatsCollectorLogger(slog.New(slog.NewTextHandler(&logs, nil)))).snapshot(ctx)

	assert.Contains(t, strings.ToLower(logs.String()), "truncat",
		"a depth scan that comes back exactly at the cap is very likely truncated, and the "+
			"number the dashboard is about to show is wrong — that has to be said out loud")
}

// aggregateFailsStorage satisfies the aggregate capability but errors, and records
// whether the collector then fell back to scanning job rows.
type aggregateFailsStorage struct {
	core.Storage
	scanCalls atomic.Int64
}

func (s *aggregateFailsStorage) GetQueueDepthQueueOnly(context.Context) (map[string][2]int64, error) {
	return nil, errors.New("aggregate failed")
}

func (s *aggregateFailsStorage) GetJobsByStatus(context.Context, core.JobStatus, int) ([]*core.Job, error) {
	s.scanCalls.Add(1)
	return nil, nil
}

// TestStatsCollector_DoesNotFallBackToTheScanWhenTheAggregateFails covers the
// stated decision in snapshot(): "Do NOT fall back to the scan when the aggregate
// fails."
//
// FALSE-GREEN TRAP: re-adding the fallback left ./ui green, because no test had a
// storage that both advertises the aggregate AND fails it. The reasoning in the
// comment is the thing worth pinning — both read `jobs` and only `jobs`, so a
// fallback pays the 10,000-row scan to fail for the same reason, on a table that
// is already unhappy.
func TestStatsCollector_DoesNotFallBackToTheScanWhenTheAggregateFails(t *testing.T) {
	_, statsStore, _ := setupCollectorTest(t)
	ctx := context.Background()

	store := &aggregateFailsStorage{}
	_, isAggregator := core.Storage(store).(queueDepthOnlyStorage)
	require.True(t, isAggregator, "this test must exercise the aggregate path")

	var logs bytes.Buffer
	NewStatsCollector(queue.New(store), statsStore,
		WithStatsCollectorLogger(slog.New(slog.NewTextHandler(&logs, nil)))).snapshot(ctx)

	assert.Zero(t, store.scanCalls.Load(),
		"the aggregate and the scan read the same table; falling back just pays a 10,000-row "+
			"page to fail the same way, against a database that is already struggling")

	all, err := statsStore.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.Empty(t, all, "a failed depth read must persist nothing rather than a fabricated zero")
	assert.Contains(t, logs.String(), "failed to read queue depth",
		"and it must say why the minute is missing")
}

// zeroDepthStorage is the third-party core.Storage the zero-skip exists for: an
// aggregate that reports a queue with no pending and no running work.
type zeroDepthStorage struct{ core.Storage }

func (zeroDepthStorage) GetQueueDepthQueueOnly(context.Context) (map[string][2]int64, error) {
	return map[string][2]int64{"drained": {0, 0}, "live": {3, 1}}, nil
}

// TestStatsCollector_SkipsQueuesWithNoLiveWork covers aggregateQueueDepth's zero
// check, which was the one surviving mutant in a 31-hunk sweep: deleting it left
// the entire ui package green.
//
// The comment above it is honest that GetQueueDepthQueueOnly cannot produce such a
// row, so the branch is defence against a THIRD-PARTY core.Storage — which is
// exactly why no test using the real GormStorage could ever reach it. Covering it
// needs a stub that does what the real query will not, and without one the
// branch's only defence was that nobody had deleted it yet.
//
// The property is that the sampled queue SET matches the scan's: a queue with no
// live work is not sampled at all, rather than sampled as a zero. A zero row makes
// a fully-drained queue draw a flat line on the depth chart forever.
func TestStatsCollector_SkipsQueuesWithNoLiveWork(t *testing.T) {
	_, statsStore, _ := setupCollectorTest(t)
	ctx := context.Background()

	NewStatsCollector(queue.New(zeroDepthStorage{}), statsStore).snapshot(ctx)

	all, err := statsStore.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)

	sampled := make(map[string]bool, len(all))
	for _, s := range all {
		sampled[s.Queue] = true
	}
	assert.True(t, sampled["live"], "a queue with live work must be sampled")
	assert.False(t, sampled["drained"],
		"a queue with neither pending nor running work must not be sampled at all — a zero row "+
			"draws a permanent flat line on the depth chart for a queue that is simply done")
}
