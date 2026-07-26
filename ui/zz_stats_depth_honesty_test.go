package ui

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowScanSpy wraps a real GormStorage and records every job-row page. Promotion
// keeps GetQueueDepthStats reachable, so the spy still satisfies the aggregate
// capability — only the scan is observable.
type rowScanSpy struct {
	*storage.GormStorage
	pages atomic.Int64
}

func (s *rowScanSpy) GetJobsByStatus(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	s.pages.Add(1)
	return s.GormStorage.GetJobsByStatus(ctx, status, limit)
}

// TestStatsCollector_SnapshotPagesNoJobRows pins the MECHANISM, not just the number.
//
// FALSE-GREEN TRAP: TestStatsCollector_SnapshotCountsBeyondTheScanCap asserts only
// that the count is right at cap+1. Raising the scan cap to 20,000 makes that test
// pass again while the collector goes right back to paging 20,000 full job rows —
// payloads included and codec-decoded — once a minute, and starts lying again at
// 20,001. The count is a proxy; "no rows were fetched" is the property.
func TestStatsCollector_SnapshotPagesNoJobRows(t *testing.T) {
	_, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	gs, ok := q.Storage().(*storage.GormStorage)
	require.True(t, ok)
	spy := &rowScanSpy{GormStorage: gs}
	require.NoError(t, spy.Enqueue(ctx, &core.Job{
		ID: core.NewID(), Queue: "q", Type: "work", Status: core.StatusPending,
	}))

	NewStatsCollector(queue.New(spy), statsStore).snapshot(ctx)

	assert.Zero(t, spy.pages.Load(),
		"a storage that can aggregate must never be asked for job rows to count them")
}

// halfBrokenStorage fails the pending page and succeeds the running page.
type halfBrokenStorage struct{ core.Storage }

func (halfBrokenStorage) GetJobsByStatus(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
	if status == core.StatusPending {
		return nil, errors.New("pending page failed")
	}
	return []*core.Job{{ID: core.NewID(), Queue: "half", Type: "work", Status: core.StatusRunning}}, nil
}

// TestStatsCollector_SnapshotPersistsNothingOnPartialScanFailure covers the
// fallback path's own honesty.
//
// FALSE-GREEN TRAP: a storage that fails BOTH pages passes with the old `continue`
// still in place — both pages fail, the map stays empty, and nothing is written for
// a reason that has nothing to do with the fix. Only a PARTIAL failure
// discriminates: the old code wrote {pending: 0, running: 1} for queue "half",
// where that 0 did not mean "no pending jobs", it meant "the pending query errored".
func TestStatsCollector_SnapshotPersistsNothingOnPartialScanFailure(t *testing.T) {
	_, statsStore, _ := setupCollectorTest(t)
	ctx := context.Background()

	_, isAggregator := core.Storage(halfBrokenStorage{}).(queueDepthStatsStorage)
	require.False(t, isAggregator, "this test must exercise the row-scan fallback")

	NewStatsCollector(queue.New(halfBrokenStorage{}), statsStore,
		WithStatsCollectorLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))).snapshot(ctx)

	all, err := statsStore.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.Empty(t, all,
		"a half-failed depth read must persist nothing: a zero that means \"the query errored\" "+
			"is a fabricated number on the dashboard, and a missing minute is the honest answer")
}
