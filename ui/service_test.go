package ui

import (
	"context"
	"errors"
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
	"github.com/jdziat/simple-durable-jobs/pkg/schedule"
	storagepackage "github.com/jdziat/simple-durable-jobs/pkg/storage"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
)

// ---------------------------------------------------------------------------
// mockStorage implements core.Storage with minimal no-op stubs.
// Tests override the fields they care about via func fields.
// ---------------------------------------------------------------------------

type mockStorage struct {
	getJobFn          func(ctx context.Context, id string) (*core.Job, error)
	getJobsByStatusFn func(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error)
	getCheckpointsFn  func(ctx context.Context, jobID string) ([]core.Checkpoint, error)
}

// Implement the full core.Storage interface; methods not under test return zero values.
func (m *mockStorage) Migrate(_ context.Context) error                              { return nil }
func (m *mockStorage) Enqueue(_ context.Context, _ *core.Job) error                { return nil }
func (m *mockStorage) Dequeue(_ context.Context, _ []string, _ string) (*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) Complete(_ context.Context, _, _ string) error                    { return nil }
func (m *mockStorage) Fail(_ context.Context, _, _, _ string, _ *time.Time) error       { return nil }
func (m *mockStorage) EnqueueUnique(_ context.Context, _ *core.Job, _ string) error     { return nil }
func (m *mockStorage) SaveCheckpoint(_ context.Context, _ *core.Checkpoint) error       { return nil }
func (m *mockStorage) DeleteCheckpoints(_ context.Context, _ string) error              { return nil }
func (m *mockStorage) GetDueJobs(_ context.Context, _ []string, _ int) ([]*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) Heartbeat(_ context.Context, _, _ string) error                   { return nil }
func (m *mockStorage) ReleaseStaleLocks(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *mockStorage) CreateFanOut(_ context.Context, _ *core.FanOut) error             { return nil }
func (m *mockStorage) GetFanOut(_ context.Context, _ string) (*core.FanOut, error)      { return nil, nil }
func (m *mockStorage) IncrementFanOutCompleted(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}
func (m *mockStorage) IncrementFanOutFailed(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}
func (m *mockStorage) IncrementFanOutCancelled(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}
func (m *mockStorage) UpdateFanOutStatus(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
	return false, nil
}
func (m *mockStorage) GetFanOutsByParent(_ context.Context, _ string) ([]*core.FanOut, error) {
	return nil, nil
}
func (m *mockStorage) EnqueueBatch(_ context.Context, _ []*core.Job) error               { return nil }
func (m *mockStorage) GetSubJobs(_ context.Context, _ string) ([]*core.Job, error)       { return nil, nil }
func (m *mockStorage) GetSubJobResults(_ context.Context, _ string) ([]*core.Job, error) { return nil, nil }
func (m *mockStorage) CancelSubJobs(_ context.Context, _ string) (int64, error)          { return 0, nil }
func (m *mockStorage) CancelSubJob(_ context.Context, _ string) (*core.FanOut, error)    { return nil, nil }
func (m *mockStorage) SuspendJob(_ context.Context, _, _ string) error                   { return nil }
func (m *mockStorage) ResumeJob(_ context.Context, _ string) (bool, error)               { return false, nil }
func (m *mockStorage) GetWaitingJobsToResume(_ context.Context) ([]*core.Job, error)     { return nil, nil }
func (m *mockStorage) SaveJobResult(_ context.Context, _, _ string, _ []byte) error      { return nil }
func (m *mockStorage) PauseJob(_ context.Context, _ string) error                        { return nil }
func (m *mockStorage) UnpauseJob(_ context.Context, _ string) error                      { return nil }
func (m *mockStorage) GetPausedJobs(_ context.Context, _ string) ([]*core.Job, error)    { return nil, nil }
func (m *mockStorage) IsJobPaused(_ context.Context, _ string) (bool, error)             { return false, nil }
func (m *mockStorage) PauseQueue(_ context.Context, _ string) error                      { return nil }
func (m *mockStorage) UnpauseQueue(_ context.Context, _ string) error                    { return nil }
func (m *mockStorage) GetPausedQueues(_ context.Context) ([]string, error)               { return nil, nil }
func (m *mockStorage) IsQueuePaused(_ context.Context, _ string) (bool, error)           { return false, nil }
func (m *mockStorage) RefreshQueueStates(_ context.Context) (map[string]bool, error) {
	return nil, nil
}

func (m *mockStorage) GetJob(ctx context.Context, id string) (*core.Job, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, id)
	}
	return nil, nil
}

func (m *mockStorage) GetJobsByStatus(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	if m.getJobsByStatusFn != nil {
		return m.getJobsByStatusFn(ctx, status, limit)
	}
	return nil, nil
}

func (m *mockStorage) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
	if m.getCheckpointsFn != nil {
		return m.getCheckpointsFn(ctx, jobID)
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// mockUIStorage extends mockStorage with the UIStorage extra methods.
// ---------------------------------------------------------------------------

type mockUIStorage struct {
	mockStorage
	getQueueStatsFn func(ctx context.Context) ([]*jobsv1.QueueStats, error)
	searchJobsFn    func(ctx context.Context, f JobFilter) ([]*core.Job, int64, error)
	retryJobFn      func(ctx context.Context, id string) (*core.Job, error)
	deleteJobFn     func(ctx context.Context, id string) error
	purgeJobsFn     func(ctx context.Context, queue string, status core.JobStatus) (int64, error)
}

func (m *mockUIStorage) GetQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	if m.getQueueStatsFn != nil {
		return m.getQueueStatsFn(ctx)
	}
	return nil, nil
}

func (m *mockUIStorage) SearchJobs(ctx context.Context, f JobFilter) ([]*core.Job, int64, error) {
	if m.searchJobsFn != nil {
		return m.searchJobsFn(ctx, f)
	}
	return nil, 0, nil
}

func (m *mockUIStorage) RetryJob(ctx context.Context, id string) (*core.Job, error) {
	if m.retryJobFn != nil {
		return m.retryJobFn(ctx, id)
	}
	return nil, nil
}

func (m *mockUIStorage) DeleteJob(ctx context.Context, id string) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, id)
	}
	return nil
}

func (m *mockUIStorage) PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error) {
	if m.purgeJobsFn != nil {
		return m.purgeJobsFn(ctx, queue, status)
	}
	return 0, nil
}

// Verify the interface is satisfied at compile time.
var _ UIStorage = (*mockUIStorage)(nil)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newServiceWithUIStorage(s *mockUIStorage) *jobsService {
	return newJobsService(s, nil, nil)
}

func newServiceWithBaseStorage(s *mockStorage) *jobsService {
	return newJobsService(s, nil, nil)
}

func sampleJob(id, queue, jobType string, status core.JobStatus) *core.Job {
	return &core.Job{
		ID:         id,
		Queue:      queue,
		Type:       jobType,
		Status:     status,
		Priority:   1,
		Attempt:    0,
		MaxRetries: 3,
		Args:       []byte(`{"key":"val"}`),
		CreatedAt:  time.Now(),
	}
}

// ---------------------------------------------------------------------------
// GetStats tests
// ---------------------------------------------------------------------------

func TestGetStats_WithUIStorage(t *testing.T) {
	qs := []*jobsv1.QueueStats{
		{Name: "default", Pending: 5, Running: 2, Completed: 10, Failed: 1},
	}
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return qs, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int64(5), resp.Msg.TotalPending)
	assert.Equal(t, int64(2), resp.Msg.TotalRunning)
	assert.Equal(t, int64(10), resp.Msg.TotalCompleted)
	assert.Equal(t, int64(1), resp.Msg.TotalFailed)
	require.Len(t, resp.Msg.Queues, 1)
	assert.Equal(t, "default", resp.Msg.Queues[0].Name)
}

func TestGetStats_WithUIStorage_Error(t *testing.T) {
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return nil, errors.New("db down")
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestGetStats_FallbackToGetJobsByStatus(t *testing.T) {
	// Use base storage (not UIStorage) to exercise the fallback path.
	jobs := []*core.Job{
		sampleJob("j1", "emails", "send", core.StatusPending),
		sampleJob("j2", "emails", "send", core.StatusPending),
	}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPending {
				return jobs, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Msg.TotalPending)
}

func TestGetStats_FallbackStorageError(t *testing.T) {
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, _ core.JobStatus, _ int) ([]*core.Job, error) {
			return nil, errors.New("storage error")
		},
	}
	svc := newServiceWithBaseStorage(store)
	_, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.Error(t, err)
}

func TestGetStats_TotalsAggregatedAcrossQueues(t *testing.T) {
	qs := []*jobsv1.QueueStats{
		{Name: "q1", Pending: 3, Running: 1, Completed: 5, Failed: 2},
		{Name: "q2", Pending: 7, Running: 0, Completed: 2, Failed: 0},
	}
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return qs, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int64(10), resp.Msg.TotalPending)
	assert.Equal(t, int64(1), resp.Msg.TotalRunning)
	assert.Equal(t, int64(7), resp.Msg.TotalCompleted)
	assert.Equal(t, int64(2), resp.Msg.TotalFailed)
}

// ---------------------------------------------------------------------------
// GetStatsHistory tests
// ---------------------------------------------------------------------------

func TestGetStatsHistory_NilStatsStorage(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	resp, err := svc.GetStatsHistory(context.Background(), connect.NewRequest(&jobsv1.GetStatsHistoryRequest{Period: "1h"}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.Completed)
	assert.Empty(t, resp.Msg.Failed)
}

func TestGetStatsHistory_WithStats(t *testing.T) {
	ts := time.Now().Truncate(time.Minute)
	statsRows := []JobStat{
		{Queue: "default", Timestamp: ts, Completed: 5, Failed: 2},
		{Queue: "default", Timestamp: ts.Add(time.Minute), Completed: 3, Failed: 1},
	}

	mockStats := &mockStatsStorage{
		getStatsHistoryFn: func(_ context.Context, _ string, _, _ time.Time) ([]JobStat, error) {
			return statsRows, nil
		},
	}

	svc := newJobsService(&mockStorage{}, nil, mockStats)
	resp, err := svc.GetStatsHistory(context.Background(), connect.NewRequest(&jobsv1.GetStatsHistoryRequest{Period: "1h"}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.Completed, 2)
	assert.Len(t, resp.Msg.Failed, 2)
}

func TestGetStatsHistory_StorageError(t *testing.T) {
	mockStats := &mockStatsStorage{
		getStatsHistoryFn: func(_ context.Context, _ string, _, _ time.Time) ([]JobStat, error) {
			return nil, errors.New("db error")
		},
	}
	svc := newJobsService(&mockStorage{}, nil, mockStats)
	_, err := svc.GetStatsHistory(context.Background(), connect.NewRequest(&jobsv1.GetStatsHistoryRequest{Period: "1h"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestGetStatsHistory_SortedByTimestamp(t *testing.T) {
	ts := time.Now().Truncate(time.Minute)
	// Provide stats out of order; response should be sorted ascending.
	statsRows := []JobStat{
		{Queue: "default", Timestamp: ts.Add(2 * time.Minute), Completed: 30},
		{Queue: "default", Timestamp: ts, Completed: 10},
		{Queue: "default", Timestamp: ts.Add(time.Minute), Completed: 20},
	}
	mockStats := &mockStatsStorage{
		getStatsHistoryFn: func(_ context.Context, _ string, _, _ time.Time) ([]JobStat, error) {
			return statsRows, nil
		},
	}
	svc := newJobsService(&mockStorage{}, nil, mockStats)
	resp, err := svc.GetStatsHistory(context.Background(), connect.NewRequest(&jobsv1.GetStatsHistoryRequest{Period: "1h"}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Completed, 3)
	// Verify ascending timestamp order.
	assert.True(t, resp.Msg.Completed[0].Timestamp.AsTime().Before(resp.Msg.Completed[1].Timestamp.AsTime()))
	assert.True(t, resp.Msg.Completed[1].Timestamp.AsTime().Before(resp.Msg.Completed[2].Timestamp.AsTime()))
}

func TestGetStatsHistory_AllPeriods(t *testing.T) {
	periods := []string{"1h", "24h", "7d", "", "bad"}
	for _, p := range periods {
		mockStats := &mockStatsStorage{
			getStatsHistoryFn: func(_ context.Context, _ string, _, _ time.Time) ([]JobStat, error) {
				return nil, nil
			},
		}
		svc := newJobsService(&mockStorage{}, nil, mockStats)
		resp, err := svc.GetStatsHistory(context.Background(), connect.NewRequest(&jobsv1.GetStatsHistoryRequest{Period: p}))
		require.NoError(t, err, "period=%q", p)
		assert.Empty(t, resp.Msg.Completed, "period=%q", p)
	}
}

// mockStatsStorage is a test double for StatsStorage.
type mockStatsStorage struct {
	getStatsHistoryFn func(ctx context.Context, queue string, since, until time.Time) ([]JobStat, error)
}

func (m *mockStatsStorage) MigrateStats(_ context.Context) error { return nil }
func (m *mockStatsStorage) UpsertStatCounters(_ context.Context, _ string, _ time.Time, _, _, _ int64) error {
	return nil
}
func (m *mockStatsStorage) SnapshotQueueDepth(_ context.Context, _ string, _ time.Time, _, _ int64) error {
	return nil
}
func (m *mockStatsStorage) PruneStats(_ context.Context, _ time.Time) (int64, error) { return 0, nil }
func (m *mockStatsStorage) GetStatsHistory(ctx context.Context, queue string, since, until time.Time) ([]JobStat, error) {
	if m.getStatsHistoryFn != nil {
		return m.getStatsHistoryFn(ctx, queue, since, until)
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// ListJobs tests
// ---------------------------------------------------------------------------

func TestListJobs_WithUIStorage(t *testing.T) {
	jobs := []*core.Job{
		sampleJob("j1", "default", "send-email", core.StatusPending),
		sampleJob("j2", "default", "send-email", core.StatusCompleted),
	}
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, _ JobFilter) ([]*core.Job, int64, error) {
			return jobs, 2, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{Limit: 10, Page: 1}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.Jobs, 2)
	assert.Equal(t, int64(2), resp.Msg.Total)
	assert.Equal(t, int32(1), resp.Msg.Page)
}

func TestListJobs_DefaultsApplied(t *testing.T) {
	var capturedFilter JobFilter
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, f JobFilter) ([]*core.Job, int64, error) {
			capturedFilter = f
			return nil, 0, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	// Limit 0 should default to 50, Page 0 should default to 1.
	_, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{Limit: 0, Page: 0}))
	require.NoError(t, err)
	assert.Equal(t, 50, capturedFilter.Limit)
	assert.Equal(t, 0, capturedFilter.Offset) // page 1, offset 0
}

func TestListJobs_LimitCappedAt100(t *testing.T) {
	var capturedFilter JobFilter
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, f JobFilter) ([]*core.Job, int64, error) {
			capturedFilter = f
			return nil, 0, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{Limit: 9999}))
	require.NoError(t, err)
	assert.Equal(t, 50, capturedFilter.Limit)
}

func TestListJobs_FilterForwarded(t *testing.T) {
	var capturedFilter JobFilter
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, f JobFilter) ([]*core.Job, int64, error) {
			capturedFilter = f
			return nil, 0, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Limit:  20,
		Page:   3,
		Status: "failed",
		Queue:  "emails",
		Type:   "send-email",
		Search: "foo",
	}))
	require.NoError(t, err)
	assert.Equal(t, "failed", capturedFilter.Status)
	assert.Equal(t, "emails", capturedFilter.Queue)
	assert.Equal(t, "send-email", capturedFilter.Type)
	assert.Equal(t, "foo", capturedFilter.Search)
	assert.Equal(t, 40, capturedFilter.Offset) // page 3, limit 20 → offset 40
}

func TestListJobs_UIStorageError(t *testing.T) {
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, _ JobFilter) ([]*core.Job, int64, error) {
			return nil, 0, errors.New("db error")
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestListJobs_FallbackWithStatusFilter(t *testing.T) {
	jobs := []*core.Job{sampleJob("j1", "default", "work", core.StatusFailed)}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, _ core.JobStatus, _ int) ([]*core.Job, error) {
			return jobs, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Status: "failed",
	}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.Jobs, 1)
}

func TestListJobs_FallbackWithQueueFilter(t *testing.T) {
	jobs := []*core.Job{
		sampleJob("j1", "emails", "work", core.StatusPending),
		sampleJob("j2", "payments", "work", core.StatusPending),
	}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPending {
				return jobs, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Queue: "emails",
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "emails", resp.Msg.Jobs[0].Queue)
}

func TestListJobs_FallbackWithTypeFilter(t *testing.T) {
	jobs := []*core.Job{
		sampleJob("j1", "default", "send-email", core.StatusPending),
		sampleJob("j2", "default", "charge", core.StatusPending),
	}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPending {
				return jobs, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Type: "send-email",
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "send-email", resp.Msg.Jobs[0].Type)
}

func TestListJobs_FallbackWithSearchFilter(t *testing.T) {
	j1 := sampleJob("match-id", "default", "work", core.StatusPending)
	j2 := sampleJob("other-id", "default", "work", core.StatusPending)
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPending {
				return []*core.Job{j1, j2}, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Search: "match",
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "match-id", resp.Msg.Jobs[0].Id)
}

func TestListJobs_FallbackPagination(t *testing.T) {
	// Build 10 jobs, request page 2 with limit 3.
	var jobs []*core.Job
	for i := 0; i < 10; i++ {
		j := sampleJob("id", "default", "work", core.StatusPending)
		j.ID = string(rune('a' + i))
		j.CreatedAt = time.Now().Add(time.Duration(i) * time.Second)
		jobs = append(jobs, j)
	}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPending {
				return jobs, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Limit: 3,
		Page:  2,
	}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.Jobs, 3)
	assert.Equal(t, int64(10), resp.Msg.Total)
}

func TestListJobs_FallbackPaginationBeyondEnd(t *testing.T) {
	jobs := []*core.Job{sampleJob("j1", "default", "work", core.StatusPending)}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPending {
				return jobs, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{
		Limit: 10,
		Page:  99,
	}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.Jobs)
}

func TestListJobs_FallbackStorageError(t *testing.T) {
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, _ core.JobStatus, _ int) ([]*core.Job, error) {
			return nil, errors.New("storage error")
		},
	}
	svc := newServiceWithBaseStorage(store)
	_, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{}))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// GetJob tests
// ---------------------------------------------------------------------------

func TestGetJob_Found(t *testing.T) {
	job := sampleJob("j1", "default", "send-email", core.StatusCompleted)
	now := time.Now()
	job.StartedAt = &now
	job.CompletedAt = &now
	job.RunAt = &now

	cps := []core.Checkpoint{
		{ID: "cp1", JobID: "j1", CallIndex: 0, CallType: "http", Result: []byte(`"ok"`), CreatedAt: time.Now()},
	}

	store := &mockStorage{
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			assert.Equal(t, "j1", id)
			return job, nil
		},
		getCheckpointsFn: func(_ context.Context, jobID string) ([]core.Checkpoint, error) {
			assert.Equal(t, "j1", jobID)
			return cps, nil
		},
	}

	svc := newServiceWithBaseStorage(store)
	resp, err := svc.GetJob(context.Background(), connect.NewRequest(&jobsv1.GetJobRequest{Id: "j1"}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Job)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
	assert.Equal(t, "default", resp.Msg.Job.Queue)
	require.Len(t, resp.Msg.Checkpoints, 1)
	assert.Equal(t, "cp1", resp.Msg.Checkpoints[0].Id)
	// Verify optional timestamp fields are populated.
	assert.NotNil(t, resp.Msg.Job.StartedAt)
	assert.NotNil(t, resp.Msg.Job.CompletedAt)
	assert.NotNil(t, resp.Msg.Job.RunAt)
}

func TestGetJob_NotFound(t *testing.T) {
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	_, err := svc.GetJob(context.Background(), connect.NewRequest(&jobsv1.GetJobRequest{Id: "missing"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeNotFound, connectErr.Code())
}

func TestGetJob_StorageError(t *testing.T) {
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, errors.New("db error")
		},
	}
	svc := newServiceWithBaseStorage(store)
	_, err := svc.GetJob(context.Background(), connect.NewRequest(&jobsv1.GetJobRequest{Id: "j1"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestGetJob_CheckpointError(t *testing.T) {
	job := sampleJob("j1", "default", "work", core.StatusRunning)
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) { return job, nil },
		getCheckpointsFn: func(_ context.Context, _ string) ([]core.Checkpoint, error) {
			return nil, errors.New("checkpoint db error")
		},
	}
	svc := newServiceWithBaseStorage(store)
	_, err := svc.GetJob(context.Background(), connect.NewRequest(&jobsv1.GetJobRequest{Id: "j1"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

// ---------------------------------------------------------------------------
// RetryJob tests
// ---------------------------------------------------------------------------

func TestRetryJob_Success(t *testing.T) {
	retried := sampleJob("j1", "default", "work", core.StatusPending)
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, id string) (*core.Job, error) {
			assert.Equal(t, "j1", id)
			return retried, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))
	require.NoError(t, err)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
}

func TestRetryJob_NotFound(t *testing.T) {
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeNotFound, connectErr.Code())
}

func TestRetryJob_StorageError(t *testing.T) {
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, errors.New("db error")
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestRetryJob_FallbackWithoutUIStorage(t *testing.T) {
	svc := newServiceWithBaseStorage(&mockStorage{})
	_, err := svc.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

// ---------------------------------------------------------------------------
// DeleteJob tests
// ---------------------------------------------------------------------------

func TestDeleteJob_Success(t *testing.T) {
	store := &mockUIStorage{
		deleteJobFn: func(_ context.Context, id string) error {
			assert.Equal(t, "j1", id)
			return nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.DeleteJob(context.Background(), connect.NewRequest(&jobsv1.DeleteJobRequest{Id: "j1"}))
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestDeleteJob_StorageError(t *testing.T) {
	store := &mockUIStorage{
		deleteJobFn: func(_ context.Context, _ string) error {
			return errors.New("db error")
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.DeleteJob(context.Background(), connect.NewRequest(&jobsv1.DeleteJobRequest{Id: "j1"}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestDeleteJob_FallbackWithoutUIStorage(t *testing.T) {
	svc := newServiceWithBaseStorage(&mockStorage{})
	_, err := svc.DeleteJob(context.Background(), connect.NewRequest(&jobsv1.DeleteJobRequest{Id: "j1"}))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// BulkRetryJobs tests
// ---------------------------------------------------------------------------

func TestBulkRetryJobs_AllSucceed(t *testing.T) {
	retried := sampleJob("j1", "default", "work", core.StatusPending)
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return retried, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.BulkRetryJobs(context.Background(), connect.NewRequest(&jobsv1.BulkRetryJobsRequest{
		Ids: []string{"j1", "j2", "j3"},
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(3), resp.Msg.Count)
}

func TestBulkRetryJobs_PartialFailure(t *testing.T) {
	calls := 0
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			calls++
			if calls%2 == 0 {
				return nil, errors.New("db error")
			}
			return sampleJob("j", "default", "work", core.StatusPending), nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.BulkRetryJobs(context.Background(), connect.NewRequest(&jobsv1.BulkRetryJobsRequest{
		Ids: []string{"j1", "j2", "j3", "j4"},
	}))
	require.NoError(t, err)
	// 4 calls: 1 ok, 2 fail, 3 ok, 4 fail → 2 successes
	assert.Equal(t, int32(2), resp.Msg.Count)
}

func TestBulkRetryJobs_EmptyList(t *testing.T) {
	svc := newServiceWithUIStorage(&mockUIStorage{})
	resp, err := svc.BulkRetryJobs(context.Background(), connect.NewRequest(&jobsv1.BulkRetryJobsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.Msg.Count)
}

// ---------------------------------------------------------------------------
// BulkDeleteJobs tests
// ---------------------------------------------------------------------------

func TestBulkDeleteJobs_AllSucceed(t *testing.T) {
	store := &mockUIStorage{
		deleteJobFn: func(_ context.Context, _ string) error { return nil },
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.BulkDeleteJobs(context.Background(), connect.NewRequest(&jobsv1.BulkDeleteJobsRequest{
		Ids: []string{"j1", "j2"},
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.Msg.Count)
}

func TestBulkDeleteJobs_PartialFailure(t *testing.T) {
	calls := 0
	store := &mockUIStorage{
		deleteJobFn: func(_ context.Context, _ string) error {
			calls++
			if calls == 2 {
				return errors.New("db error")
			}
			return nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.BulkDeleteJobs(context.Background(), connect.NewRequest(&jobsv1.BulkDeleteJobsRequest{
		Ids: []string{"j1", "j2", "j3"},
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.Msg.Count)
}

func TestBulkDeleteJobs_EmptyList(t *testing.T) {
	svc := newServiceWithUIStorage(&mockUIStorage{})
	resp, err := svc.BulkDeleteJobs(context.Background(), connect.NewRequest(&jobsv1.BulkDeleteJobsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.Msg.Count)
}

// ---------------------------------------------------------------------------
// ListQueues tests
// ---------------------------------------------------------------------------

func TestListQueues_WithUIStorage(t *testing.T) {
	qs := []*jobsv1.QueueStats{
		{Name: "default", Pending: 3},
		{Name: "emails", Pending: 1},
	}
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return qs, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.ListQueues(context.Background(), connect.NewRequest(&jobsv1.ListQueuesRequest{}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.Queues, 2)
}

func TestListQueues_StorageError(t *testing.T) {
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return nil, errors.New("db error")
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.ListQueues(context.Background(), connect.NewRequest(&jobsv1.ListQueuesRequest{}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

// ---------------------------------------------------------------------------
// PurgeQueue tests
// ---------------------------------------------------------------------------

func TestPurgeQueue_Success(t *testing.T) {
	store := &mockUIStorage{
		purgeJobsFn: func(_ context.Context, q string, s core.JobStatus) (int64, error) {
			assert.Equal(t, "emails", q)
			assert.Equal(t, core.StatusFailed, s)
			return 7, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.PurgeQueue(context.Background(), connect.NewRequest(&jobsv1.PurgeQueueRequest{
		Name:   "emails",
		Status: "failed",
	}))
	require.NoError(t, err)
	assert.Equal(t, int64(7), resp.Msg.Deleted)
}

func TestPurgeQueue_StorageError(t *testing.T) {
	store := &mockUIStorage{
		purgeJobsFn: func(_ context.Context, _ string, _ core.JobStatus) (int64, error) {
			return 0, errors.New("db error")
		},
	}
	svc := newServiceWithUIStorage(store)
	_, err := svc.PurgeQueue(context.Background(), connect.NewRequest(&jobsv1.PurgeQueueRequest{
		Name:   "emails",
		Status: "failed",
	}))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeInternal, connectErr.Code())
}

func TestPurgeQueue_FallbackWithoutUIStorage(t *testing.T) {
	svc := newServiceWithBaseStorage(&mockStorage{})
	_, err := svc.PurgeQueue(context.Background(), connect.NewRequest(&jobsv1.PurgeQueueRequest{Name: "emails", Status: "failed"}))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// ListScheduledJobs tests
// ---------------------------------------------------------------------------

func TestListScheduledJobs_NilQueue(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	resp, err := svc.ListScheduledJobs(context.Background(), connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.Jobs)
}

// ---------------------------------------------------------------------------
// checkpointToProto tests
// ---------------------------------------------------------------------------

func TestCheckpointToProto(t *testing.T) {
	now := time.Now()
	cp := &core.Checkpoint{
		ID:        "cp1",
		JobID:     "j1",
		CallIndex: 2,
		CallType:  "http.get",
		Result:    []byte(`"result"`),
		Error:     "some error",
		CreatedAt: now,
	}
	pb := checkpointToProto(cp)
	assert.Equal(t, "cp1", pb.Id)
	assert.Equal(t, "j1", pb.JobId)
	assert.Equal(t, int32(2), pb.CallIndex)
	assert.Equal(t, "http.get", pb.CallType)
	assert.Equal(t, []byte(`"result"`), pb.Result)
	assert.Equal(t, "some error", pb.Error)
	assert.NotNil(t, pb.CreatedAt)
}

// ---------------------------------------------------------------------------
// jobToProto field coverage
// ---------------------------------------------------------------------------

func TestJobToProto_OptionalFieldsNil(t *testing.T) {
	job := sampleJob("j1", "default", "work", core.StatusPending)
	// StartedAt, CompletedAt, RunAt are nil
	pb := jobToProto(job)
	assert.Nil(t, pb.StartedAt)
	assert.Nil(t, pb.CompletedAt)
	assert.Nil(t, pb.RunAt)
	assert.Equal(t, "j1", pb.Id)
}

func TestJobToProto_OptionalFieldsSet(t *testing.T) {
	now := time.Now()
	job := sampleJob("j1", "default", "work", core.StatusCompleted)
	job.StartedAt = &now
	job.CompletedAt = &now
	job.RunAt = &now
	job.LastError = "oops"
	job.Args = []byte(`{"x":1}`)
	pb := jobToProto(job)
	assert.NotNil(t, pb.StartedAt)
	assert.NotNil(t, pb.CompletedAt)
	assert.NotNil(t, pb.RunAt)
	assert.Equal(t, "oops", pb.LastError)
	assert.Equal(t, []byte(`{"x":1}`), pb.Args)
}

// ---------------------------------------------------------------------------
// getQueueStats fallback: multiple statuses in one call
// ---------------------------------------------------------------------------

func TestGetQueueStats_FallbackMultipleStatuses(t *testing.T) {
	jobsByStatus := map[core.JobStatus][]*core.Job{
		core.StatusPending:   {sampleJob("j1", "q1", "work", core.StatusPending)},
		core.StatusRunning:   {sampleJob("j2", "q1", "work", core.StatusRunning)},
		core.StatusCompleted: {sampleJob("j3", "q1", "work", core.StatusCompleted)},
		core.StatusFailed:    {sampleJob("j4", "q1", "work", core.StatusFailed)},
	}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			return jobsByStatus[status], nil
		},
	}
	svc := newServiceWithBaseStorage(store)
	resp, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Queues, 1)
	qs := resp.Msg.Queues[0]
	assert.Equal(t, int64(1), qs.Pending)
	assert.Equal(t, int64(1), qs.Running)
	assert.Equal(t, int64(1), qs.Completed)
	assert.Equal(t, int64(1), qs.Failed)
}

// ---------------------------------------------------------------------------
// ListScheduledJobs with a real queue
// ---------------------------------------------------------------------------

func setupServiceWithQueue(t *testing.T) (*jobsService, *queue.Queue) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{}))

	store := storagepackage.NewGormStorage(db)
	q := queue.New(store)
	svc := newJobsService(store, q, nil)
	return svc, q
}

func TestListScheduledJobs_WithScheduledJobs(t *testing.T) {
	svc, q := setupServiceWithQueue(t)

	// Register a scheduled job using the every-schedule (no String() method).
	q.Schedule("daily-report", schedule.Every(24*time.Hour))

	resp, err := svc.ListScheduledJobs(context.Background(), connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "daily-report", resp.Msg.Jobs[0].Name)
}

// stringerSchedule is a schedule that also implements fmt.Stringer.
type stringerSchedule struct{ label string }

func (s *stringerSchedule) Next(from time.Time) time.Time { return from.Add(time.Hour) }
func (s *stringerSchedule) String() string                 { return s.label }

func TestListScheduledJobs_WithStringerSchedule(t *testing.T) {
	svc, q := setupServiceWithQueue(t)

	q.Schedule("hourly-sync", &stringerSchedule{label: "every 1h"})

	resp, err := svc.ListScheduledJobs(context.Background(), connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "hourly-sync", resp.Msg.Jobs[0].Name)
	// The schedule string should come from the Stringer, not the name.
	assert.Equal(t, "every 1h", resp.Msg.Jobs[0].Schedule)
}

// ---------------------------------------------------------------------------
// NewGormStatsStorage constructor coverage
// ---------------------------------------------------------------------------

func TestNewGormStatsStorage_Constructor(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := NewGormStatsStorage(db)
	require.NotNil(t, s)

	// Verify it is usable.
	err = s.MigrateStats(context.Background())
	require.NoError(t, err)
}
