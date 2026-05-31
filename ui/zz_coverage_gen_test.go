package ui

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	storagepackage "github.com/jdziat/simple-durable-jobs/pkg/storage"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1/jobsv1connect"
)

// startWatchTestServer wires the JobsService Connect handler backed by a real
// queue over an httptest server, and returns a Connect client plus the queue so
// the caller can Emit events and observe them flow through WatchEvents.
//
// We register the Connect handler directly (rather than via ui.Handler) so the
// httptest HTTP/1.1 server speaks the Connect protocol without an h2c wrapper;
// Connect server-streaming works fine over HTTP/1.1 with chunked responses.
func startWatchTestServer(t *testing.T) (jobsv1connect.JobsServiceClient, *queue.Queue, func()) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{}))

	store := storagepackage.NewGormStorage(db)
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	mux := http.NewServeMux()
	path, handler := jobsv1connect.NewJobsServiceHandler(svc)
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)

	client := jobsv1connect.NewJobsServiceClient(srv.Client(), srv.URL)
	return client, q, srv.Close
}

// TestWatchEvents_StreamsAndFilters drives the full WatchEvents receive/filter/
// send loop (service.go lines 456-489) via a real Connect streaming client.
// It exercises both the unfiltered path and the queue-filter accept path.
func TestWatchEvents_StreamsAndFilters(t *testing.T) {
	client, q, closeFn := startWatchTestServer(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job := &core.Job{ID: "watch-1", Queue: "emails", Type: "send"}

	// The server subscribes asynchronously only after the streaming request
	// reaches the handler, and over HTTP/1.1 the client's WatchEvents call blocks
	// until the server flushes its first message. So we must emit continuously in
	// the background BEFORE opening the stream: once the handler subscribes, the
	// next emit drives stream.Send, which flushes headers and unblocks the client.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// A job.completed event carries the Job field, so the filter
				// reads pbEvent.Job.Queue ("emails") and accepts it.
				q.Emit(&core.JobCompleted{Job: job, Timestamp: time.Now()})
			}
		}
	}()
	defer close(stop)

	// Filter to the "emails" queue so the filter-accept branch is hit.
	stream, err := client.WatchEvents(ctx, connect.NewRequest(&jobsv1.WatchEventsRequest{
		Queues: []string{"emails"},
	}))
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	require.True(t, stream.Receive(), "expected to receive at least one streamed event: %v", stream.Err())
	ev := stream.Msg()
	assert.Equal(t, "job.completed", ev.Type)
	require.NotNil(t, ev.Job)
	assert.Equal(t, "emails", ev.Job.Queue)
}

// TestWatchEvents_FilterRejectsAndQueueEventBranch exercises the filter-reject
// path (an event whose queue is not in the filter set is skipped) as well as
// the branch that reads pbEvent.Queue for events that have no Job field.
func TestWatchEvents_FilterRejectsAndQueueEventBranch(t *testing.T) {
	client, q, closeFn := startWatchTestServer(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Emit a mix of events continuously in the background before opening the
	// stream: one on a non-matching queue (rejected), a queue-level event with no
	// Job field on a non-matching queue (rejected via the pbEvent.Queue branch),
	// and a queue-level event on the matching queue (accepted).
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Rejected: job on a different queue.
				q.Emit(&core.JobCompleted{Job: &core.Job{ID: "x", Queue: "other", Type: "t"}, Timestamp: time.Now()})
				// Rejected: queue event with no Job field on a non-matching queue.
				q.Emit(&core.QueuePaused{Queue: "other", Timestamp: time.Now()})
				// Accepted: queue event with no Job field on the matching queue,
				// exercising the `eventQueue = pbEvent.Queue` branch.
				q.Emit(&core.QueueResumed{Queue: "wanted", Timestamp: time.Now()})
			}
		}
	}()
	defer close(stop)

	// Filter to "wanted".
	stream, err := client.WatchEvents(ctx, connect.NewRequest(&jobsv1.WatchEventsRequest{
		Queues: []string{"wanted"},
	}))
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	require.True(t, stream.Receive(), "expected a matching queue event: %v", stream.Err())
	ev := stream.Msg()
	assert.Equal(t, "queue.resumed", ev.Type)
	assert.Nil(t, ev.Job)
	assert.Equal(t, "wanted", ev.Queue)
}

// ---------------------------------------------------------------------------
// PauseJob / ResumeJob GetJob error branches
// ---------------------------------------------------------------------------

func TestPauseJob_GetJobError(t *testing.T) {
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, errors.New("get job boom")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.PauseJob(context.Background(), connect.NewRequest(&jobsv1.PauseJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestResumeJob_GetJobError(t *testing.T) {
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, errors.New("get job boom")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.ResumeJob(context.Background(), connect.NewRequest(&jobsv1.ResumeJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

// ---------------------------------------------------------------------------
// StatsCollector.Flush: zero-counter queues are skipped (stats_collector.go 140-141)
// ---------------------------------------------------------------------------

func TestStatsCollector_FlushSkipsZeroCounterQueues(t *testing.T) {
	collector, statsStore, _ := setupCollectorTest(t)
	ctx := context.Background()

	// Insert a queue entry whose counters are all zero. Flush must skip it,
	// writing nothing to storage.
	collector.mu.Lock()
	collector.counters["idle-queue"] = &statCounters{}
	collector.mu.Unlock()

	collector.Flush(ctx)

	all, err := statsStore.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.Empty(t, all, "zero-counter queue should not produce a stats row")
}

// ---------------------------------------------------------------------------
// StatsCollector.snapshot: running-status counting + SnapshotQueueDepth error
// ---------------------------------------------------------------------------

func TestStatsCollector_SnapshotCountsRunning(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)
	ctx := context.Background()

	store := q.Storage()
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID: "run-1", Queue: "run-queue", Type: "work", Status: core.StatusRunning,
	}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID: "pend-1", Queue: "run-queue", Type: "work", Status: core.StatusPending,
	}))

	collector.snapshot(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "run-queue", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, int64(1), stats[0].Pending)
	assert.Equal(t, int64(1), stats[0].Running)
}

// snapshotErrStatsStorage fails SnapshotQueueDepth so the error-log branch
// (stats_collector.go 194-201) is exercised.
type snapshotErrStatsStorage struct {
	nopStatsStorage
}

func (*snapshotErrStatsStorage) SnapshotQueueDepth(_ context.Context, _ string, _ time.Time, _, _ int64) error {
	return errors.New("snapshot boom")
}

func TestStatsCollector_SnapshotDepthError(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{}))

	store := storagepackage.NewGormStorage(db)
	q := queue.New(store)
	ctx := context.Background()
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID: "p-1", Queue: "errq", Type: "work", Status: core.StatusPending,
	}))

	logr := slog.New(slog.NewTextHandler(io.Discard, nil))
	collector := NewStatsCollector(q, &snapshotErrStatsStorage{}, WithStatsCollectorLogger(logr))

	// Must not panic even though SnapshotQueueDepth errors for every queue.
	assert.NotPanics(t, func() { collector.snapshot(ctx) })
}

// ---------------------------------------------------------------------------
// StatsCollector.prune: error branch (stats_collector.go 207-209)
// ---------------------------------------------------------------------------

// pruneErrStatsStorage fails PruneStats so the prune error-log branch runs.
type pruneErrStatsStorage struct {
	nopStatsStorage
}

func (*pruneErrStatsStorage) PruneStats(_ context.Context, _ time.Time) (int64, error) {
	return 0, errors.New("prune boom")
}

func TestStatsCollector_PruneError(t *testing.T) {
	q := queue.New(&mockStorage{})
	logr := slog.New(slog.NewTextHandler(io.Discard, nil))
	collector := NewStatsCollector(q, &pruneErrStatsStorage{},
		WithStatsCollectorRetention(time.Hour),
		WithStatsCollectorLogger(logr),
	)

	// retention > 0 so PruneStats is called; it errors and is logged.
	assert.NotPanics(t, func() { collector.prune(context.Background()) })
}

// ---------------------------------------------------------------------------
// gormStatsStorage error paths via a closed DB (stats_gorm.go 37-39, 51-53,
// 59, 78-80).
// ---------------------------------------------------------------------------

func closedStatsStorage(t *testing.T) *gormStatsStorage {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := &gormStatsStorage{db: db}
	require.NoError(t, s.MigrateStats(context.Background()))

	// Close the underlying connection so subsequent queries fail.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	return s
}

func TestGormStatsStorage_UpsertErrorOnClosedDB(t *testing.T) {
	s := closedStatsStorage(t)
	ts := time.Now().Truncate(time.Minute)

	// The Updates query fails immediately on the closed DB, returning an error
	// from the first branch (stats_gorm.go 37-39).
	err := s.UpsertStatCounters(context.Background(), "default", ts, 1, 0, 0)
	require.Error(t, err)
}

func TestGormStatsStorage_SnapshotErrorOnClosedDB(t *testing.T) {
	s := closedStatsStorage(t)
	ts := time.Now().Truncate(time.Minute)

	// The First() lookup fails with a non-RecordNotFound error on the closed DB,
	// hitting stats_gorm.go 78-80.
	err := s.SnapshotQueueDepth(context.Background(), "default", ts, 1, 1)
	require.Error(t, err)
}
