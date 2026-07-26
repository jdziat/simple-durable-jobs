package ui

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The collector counted queue depth by PAGING FULL JOB ROWS — payload columns
// included and codec-decoded — and truncating at 10,000 per status. Past the cap
// the dashboard silently showed a number that was simply wrong, during exactly
// the backlog incident an operator opens it for.
//
// FALSE-GREEN TRAP: a test with a handful of jobs passes identically before and
// after, because under the cap the scan and the aggregate agree — that is the
// whole point of the fallback. The count must EXCEED the cap for the assertion
// to discriminate, so this seeds cap+1 rows and asserts the exact total.
func TestStatsCollector_SnapshotCountsBeyondTheScanCap(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	const seeded = 10001 // one past the old scan cap
	// Chunked: SQLite caps bound parameters per statement, and core.Job is wide.
	store := q.Storage()
	const chunk = 25
	rows := make([]*core.Job, 0, chunk)
	flush := func() {
		require.NoError(t, store.EnqueueBatch(ctx, rows))
		rows = rows[:0]
	}
	for i := range seeded {
		rows = append(rows, &core.Job{
			ID: core.NewID(), Queue: "deep", Type: "work", Status: core.StatusPending,
			Args: []byte(`{"n":` + strconv.Itoa(i) + `}`),
		})
		if len(rows) == chunk {
			flush()
		}
	}
	if len(rows) > 0 {
		flush()
	}

	collector.snapshot(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "deep", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, int64(seeded), stats[0].Pending,
		"queue depth must be counted, not paged — a truncated count is a wrong number on the "+
			"dashboard during exactly the incident it exists for")
}

// TestStatsCollector_SnapshotReportsTheSameQueueSet pins the compatibility edge
// the aggregate introduces. GetQueueDepthStats groups over EVERY status, so a
// queue whose jobs are all terminal appears in it while the old scan never saw
// it. Writing those would add a zero-depth row every minute, forever, for every
// queue that has ever run a job.
func TestStatsCollector_SnapshotReportsTheSameQueueSet(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	store := q.Storage()
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID: core.NewID(), Queue: "live", Type: "work", Status: core.StatusPending,
	}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID: core.NewID(), Queue: "drained", Type: "work", Status: core.StatusCompleted,
	}))

	collector.snapshot(ctx)

	ts := time.Now().Truncate(time.Minute)
	live, err := statsStore.GetStatsHistory(ctx, "live", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	assert.Len(t, live, 1, "a queue with pending work is sampled, as before")

	drained, err := statsStore.GetStatsHistory(ctx, "drained", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	assert.Empty(t, drained,
		"a queue with no pending or running work must NOT be sampled — the aggregate sees every "+
			"status, and writing those adds a zero row per queue per minute forever")
}

// TestStatsCollector_SnapshotFallsBackWithoutTheAggregate keeps the fallback
// honest: a custom core.Storage that does not implement GetQueueDepthStats must
// still produce depth samples via the row scan.
func TestStatsCollector_SnapshotFallsBackWithoutTheAggregate(t *testing.T) {
	_, statsStore, _ := setupCollectorTest(t)
	ctx := context.Background()

	scanOnly := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			return []*core.Job{{ID: core.NewID(), Queue: "fallback", Type: "work", Status: status}}, nil
		},
	}
	_, isAggregator := core.Storage(scanOnly).(queueDepthOnlyStorage)
	require.False(t, isAggregator, "this storage must NOT satisfy the aggregate capability")

	collector := NewStatsCollector(queue.New(scanOnly), statsStore)
	collector.snapshot(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "fallback", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, int64(1), stats[0].Pending)
	assert.Equal(t, int64(1), stats[0].Running)
}
