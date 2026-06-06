package ui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
	"github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1/jobsv1connect"
)

// ---------------------------------------------------------------------------
// mockStorage implements core.Storage with minimal no-op stubs.
// Tests override the fields they care about via func fields.
// ---------------------------------------------------------------------------

type mockStorage struct {
	getJobFn             func(ctx context.Context, id string) (*core.Job, error)
	getJobsByStatusFn    func(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error)
	getCheckpointsFn     func(ctx context.Context, jobID string) ([]core.Checkpoint, error)
	getFanOutsByParentFn func(ctx context.Context, parentID string) ([]*core.FanOut, error)
	getSubJobsFn         func(ctx context.Context, fanOutID string) ([]*core.Job, error)
	pauseJobFn           func(ctx context.Context, id string) error
	unpauseJobFn         func(ctx context.Context, id string) error
	isJobPausedFn        func(ctx context.Context, id string) (bool, error)
	getPausedJobsFn      func(ctx context.Context, queue string) ([]*core.Job, error)
	pauseQueueFn         func(ctx context.Context, name string) error
	unpauseQueueFn       func(ctx context.Context, name string) error
	isQueuePausedFn      func(ctx context.Context, name string) (bool, error)
	getPausedQueuesFn    func(ctx context.Context) ([]string, error)
}

// Implement the full core.Storage interface; methods not under test return zero values.
func (m *mockStorage) Migrate(_ context.Context) error              { return nil }
func (m *mockStorage) Enqueue(_ context.Context, _ *core.Job) error { return nil }
func (m *mockStorage) Dequeue(_ context.Context, _ []string, _ string) (*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) Complete(_ context.Context, _, _ string) error                { return nil }
func (m *mockStorage) Release(_ context.Context, _, _ string) error                 { return nil }
func (m *mockStorage) Fail(_ context.Context, _, _, _ string, _ *time.Time) error   { return nil }
func (m *mockStorage) EnqueueUnique(_ context.Context, _ *core.Job, _ string) error { return nil }
func (m *mockStorage) SaveCheckpoint(_ context.Context, _ *core.Checkpoint) error   { return nil }
func (m *mockStorage) DeleteCheckpoints(_ context.Context, _ string) error          { return nil }
func (m *mockStorage) GetDueJobs(_ context.Context, _ []string, _ int) ([]*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) ClaimScheduledFire(_ context.Context, _ string, _ time.Time) (bool, error) {
	return true, nil
}
func (m *mockStorage) Heartbeat(_ context.Context, _, _ string) error { return nil }
func (m *mockStorage) ReleaseStaleLocks(_ context.Context, _ time.Duration) ([]string, error) {
	return nil, nil
}

func (m *mockStorage) FindOrphanedJobs(_ context.Context, _ []string, _ string) ([]string, error) {
	return nil, nil
}
func (m *mockStorage) CreateFanOut(_ context.Context, _ *core.FanOut) error        { return nil }
func (m *mockStorage) GetFanOut(_ context.Context, _ string) (*core.FanOut, error) { return nil, nil }
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
func (m *mockStorage) GetFanOutsByParent(ctx context.Context, parentID string) ([]*core.FanOut, error) {
	if m.getFanOutsByParentFn != nil {
		return m.getFanOutsByParentFn(ctx, parentID)
	}
	return nil, nil
}
func (m *mockStorage) EnqueueBatch(_ context.Context, _ []*core.Job) error { return nil }
func (m *mockStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	if m.getSubJobsFn != nil {
		return m.getSubJobsFn(ctx, fanOutID)
	}
	return nil, nil
}
func (m *mockStorage) GetSubJobResults(_ context.Context, _ string) ([]*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) CancelSubJobs(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (m *mockStorage) CancelSubJob(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}
func (m *mockStorage) SuspendJob(_ context.Context, _, _ string) error               { return nil }
func (m *mockStorage) ResumeJob(_ context.Context, _ string) (bool, error)           { return false, nil }
func (m *mockStorage) GetWaitingJobsToResume(_ context.Context) ([]*core.Job, error) { return nil, nil }
func (m *mockStorage) GetStalledFanOutParents(_ context.Context, _ time.Time) ([]*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) SaveJobResult(_ context.Context, _, _ string, _ []byte) error { return nil }
func (m *mockStorage) PauseJob(ctx context.Context, id string) error {
	if m.pauseJobFn != nil {
		return m.pauseJobFn(ctx, id)
	}
	return nil
}
func (m *mockStorage) UnpauseJob(ctx context.Context, id string) error {
	if m.unpauseJobFn != nil {
		return m.unpauseJobFn(ctx, id)
	}
	return nil
}
func (m *mockStorage) GetPausedJobs(ctx context.Context, queue string) ([]*core.Job, error) {
	if m.getPausedJobsFn != nil {
		return m.getPausedJobsFn(ctx, queue)
	}
	return nil, nil
}
func (m *mockStorage) IsJobPaused(ctx context.Context, id string) (bool, error) {
	if m.isJobPausedFn != nil {
		return m.isJobPausedFn(ctx, id)
	}
	return false, nil
}
func (m *mockStorage) PauseQueue(ctx context.Context, name string) error {
	if m.pauseQueueFn != nil {
		return m.pauseQueueFn(ctx, name)
	}
	return nil
}
func (m *mockStorage) UnpauseQueue(ctx context.Context, name string) error {
	if m.unpauseQueueFn != nil {
		return m.unpauseQueueFn(ctx, name)
	}
	return nil
}
func (m *mockStorage) GetPausedQueues(ctx context.Context) ([]string, error) {
	if m.getPausedQueuesFn != nil {
		return m.getPausedQueuesFn(ctx)
	}
	return nil, nil
}
func (m *mockStorage) IsQueuePaused(ctx context.Context, name string) (bool, error) {
	if m.isQueuePausedFn != nil {
		return m.isQueuePausedFn(ctx, name)
	}
	return false, nil
}
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
	getQueueStatsFn    func(ctx context.Context) ([]*jobsv1.QueueStats, error)
	searchJobsFn       func(ctx context.Context, f JobFilter) ([]*core.Job, int64, error)
	retryJobFn         func(ctx context.Context, id string) (*core.Job, error)
	deleteJobFn        func(ctx context.Context, id string) error
	purgeJobsFn        func(ctx context.Context, queue string, status core.JobStatus) (int64, error)
	getWorkflowRootsFn func(ctx context.Context, status string, limit, offset int) ([]*core.Job, int64, error)
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

func (m *mockUIStorage) GetWorkflowRoots(ctx context.Context, status string, limit, offset int) ([]*core.Job, int64, error) {
	if m.getWorkflowRootsFn != nil {
		return m.getWorkflowRootsFn(ctx, status, limit, offset)
	}
	return nil, 0, nil
}

// Verify the interface is satisfied at compile time.
var _ UIStorage = (*mockUIStorage)(nil)

type mockWorkflowBatchStorage struct {
	mockStorage
	getFanOutsByParentsFn func(ctx context.Context, parentIDs []string) ([]*core.FanOut, error)
	getSubJobsByFanOutsFn func(ctx context.Context, fanOutIDs []string) ([]*core.Job, error)
}

func (m *mockWorkflowBatchStorage) GetFanOutsByParents(ctx context.Context, parentIDs []string) ([]*core.FanOut, error) {
	if m.getFanOutsByParentsFn != nil {
		return m.getFanOutsByParentsFn(ctx, parentIDs)
	}
	return nil, nil
}

func (m *mockWorkflowBatchStorage) GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []string) ([]*core.Job, error) {
	if m.getSubJobsByFanOutsFn != nil {
		return m.getSubJobsByFanOutsFn(ctx, fanOutIDs)
	}
	return nil, nil
}

type mockUIWorkflowBatchStorage struct {
	mockUIStorage
	getFanOutsByParentsFn func(ctx context.Context, parentIDs []string) ([]*core.FanOut, error)
	getSubJobsByFanOutsFn func(ctx context.Context, fanOutIDs []string) ([]*core.Job, error)
}

func (m *mockUIWorkflowBatchStorage) GetFanOutsByParents(ctx context.Context, parentIDs []string) ([]*core.FanOut, error) {
	if m.getFanOutsByParentsFn != nil {
		return m.getFanOutsByParentsFn(ctx, parentIDs)
	}
	return nil, nil
}

func (m *mockUIWorkflowBatchStorage) GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []string) ([]*core.Job, error) {
	if m.getSubJobsByFanOutsFn != nil {
		return m.getSubJobsByFanOutsFn(ctx, fanOutIDs)
	}
	return nil, nil
}

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

func jobIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "job"
	}
	return ids
}

// ---------------------------------------------------------------------------
// Handler write authorization tests
// ---------------------------------------------------------------------------

type authorizerFunc func(context.Context, Action) error

func (f authorizerFunc) Authorize(ctx context.Context, action Action) error {
	return f(ctx, action)
}

func TestHandler_MutatingRPCDeniedWithoutAuthOrOptIn(t *testing.T) {
	called := false
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			called = true
			return sampleJob("j1", "default", "work", core.StatusPending), nil
		},
	}
	server := httptest.NewServer(Handler(store))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	_, err := client.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))

	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	assert.False(t, called)
}

func TestHandler_MutatingRPCAllowedWithInsecureOptIn(t *testing.T) {
	called := false
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			called = true
			return sampleJob("j1", "default", "work", core.StatusPending), nil
		},
	}
	server := httptest.NewServer(Handler(store, WithInsecureAllowUnauthenticatedWrites()))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	resp, err := client.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))

	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Job)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
	assert.True(t, called)
}

func TestHandler_CancelJobRPCRequiresWriteAuth(t *testing.T) {
	called := false
	store := &mockStorage{
		pauseJobFn: func(_ context.Context, _ string) error {
			called = true
			return nil
		},
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return sampleJob("j1", "default", "work", core.StatusCancelled), nil
		},
	}
	q := queue.New(store)
	server := httptest.NewServer(Handler(store, WithQueue(q)))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	_, err := client.CancelJob(context.Background(), connect.NewRequest(&jobsv1.CancelJobRequest{Id: "j1"}))

	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	assert.False(t, called)
}

func TestHandler_CancelJobRPCAllowedWithInsecureOptIn(t *testing.T) {
	called := false
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return sampleJob("j1", "default", "work", core.StatusCancelled), nil
		},
		pauseJobFn: func(_ context.Context, id string) error {
			assert.Equal(t, "j1", id)
			called = true
			return nil
		},
	}
	q := queue.New(store)
	server := httptest.NewServer(Handler(store, WithQueue(q), WithInsecureAllowUnauthenticatedWrites()))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	resp, err := client.CancelJob(context.Background(), connect.NewRequest(&jobsv1.CancelJobRequest{Id: "j1"}))

	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Job)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
	assert.True(t, called)
}

func TestHandler_MutatingRPCAllowedWithMiddleware(t *testing.T) {
	called := false
	store := &mockUIStorage{
		deleteJobFn: func(_ context.Context, _ string) error {
			called = true
			return nil
		},
	}
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
		})
	}
	server := httptest.NewServer(Handler(store, WithMiddleware(middleware)))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	_, err := client.DeleteJob(context.Background(), connect.NewRequest(&jobsv1.DeleteJobRequest{Id: "j1"}))

	require.NoError(t, err)
	assert.True(t, called)
}

func TestHandler_AuthorizerAllowsAndDeniesPerAction(t *testing.T) {
	retryCalled := false
	deleteCalled := false
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			retryCalled = true
			return sampleJob("j1", "default", "work", core.StatusPending), nil
		},
		deleteJobFn: func(_ context.Context, _ string) error {
			deleteCalled = true
			return nil
		},
	}
	authorizer := authorizerFunc(func(_ context.Context, action Action) error {
		if action == ActionRetryJob {
			return nil
		}
		return errors.New("not allowed")
	})
	server := httptest.NewServer(Handler(store, WithAuthorizer(authorizer)))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	retryResp, err := client.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))
	require.NoError(t, err)
	require.NotNil(t, retryResp.Msg.Job)
	assert.Equal(t, "j1", retryResp.Msg.Job.Id)
	assert.True(t, retryCalled)

	_, err = client.DeleteJob(context.Background(), connect.NewRequest(&jobsv1.DeleteJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	assert.False(t, deleteCalled)
}

func TestHandler_AuthorizerUsesPrincipalFromMiddleware(t *testing.T) {
	type principal struct {
		Role string
	}
	called := false
	store := &mockUIStorage{
		deleteJobFn: func(_ context.Context, _ string) error {
			called = true
			return nil
		},
	}
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := WithPrincipal(r.Context(), principal{Role: "admin"})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	authorizer := authorizerFunc(func(ctx context.Context, action Action) error {
		if action != ActionDeleteJob {
			return errors.New("unexpected action")
		}
		p, ok := PrincipalFromContext(ctx)
		if !ok {
			return errors.New("missing principal")
		}
		user, ok := p.(principal)
		if !ok || user.Role != "admin" {
			return errors.New("not an admin")
		}
		return nil
	})
	server := httptest.NewServer(Handler(store, WithMiddleware(middleware), WithAuthorizer(authorizer)))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	_, err := client.DeleteJob(context.Background(), connect.NewRequest(&jobsv1.DeleteJobRequest{Id: "j1"}))

	require.NoError(t, err)
	assert.True(t, called)
}

func TestHandler_ReadOnlyRPCNeverCallsAuthorizer(t *testing.T) {
	authorized := false
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, _ JobFilter) ([]*core.Job, int64, error) {
			return []*core.Job{sampleJob("j1", "default", "work", core.StatusPending)}, 1, nil
		},
	}
	authorizer := authorizerFunc(func(_ context.Context, _ Action) error {
		authorized = true
		return errors.New("deny everything")
	})
	server := httptest.NewServer(Handler(store, WithAuthorizer(authorizer)))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	resp, err := client.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{}))

	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "j1", resp.Msg.Jobs[0].Id)
	assert.False(t, authorized)
}

func TestHandler_AuthorizerPreservesConnectErrorCode(t *testing.T) {
	called := false
	store := &mockUIStorage{
		retryJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			called = true
			return sampleJob("j1", "default", "work", core.StatusPending), nil
		},
	}
	authorizer := authorizerFunc(func(_ context.Context, _ Action) error {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("login required"))
	})
	server := httptest.NewServer(Handler(store, WithAuthorizer(authorizer)))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	_, err := client.RetryJob(context.Background(), connect.NewRequest(&jobsv1.RetryJobRequest{Id: "j1"}))

	require.Error(t, err)
	assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
	assert.False(t, called)
}

func TestHandler_ReadOnlyRPCAllowedWithoutAuth(t *testing.T) {
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, _ JobFilter) ([]*core.Job, int64, error) {
			return []*core.Job{sampleJob("j1", "default", "work", core.StatusPending)}, 1, nil
		},
	}
	server := httptest.NewServer(Handler(store))
	defer server.Close()

	client := jobsv1connect.NewJobsServiceClient(server.Client(), server.URL)
	resp, err := client.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{}))

	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "j1", resp.Msg.Jobs[0].Id)
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

func TestListJobs_TotalCanExceedPageSize(t *testing.T) {
	jobs := []*core.Job{
		sampleJob("j1", "default", "send-email", core.StatusPending),
		sampleJob("j2", "default", "send-email", core.StatusPending),
	}
	store := &mockUIStorage{
		searchJobsFn: func(_ context.Context, f JobFilter) ([]*core.Job, int64, error) {
			assert.Equal(t, 2, f.Limit)
			return jobs, 5, nil
		},
	}
	svc := newServiceWithUIStorage(store)
	resp, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{Limit: 2, Page: 1}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.Jobs, 2)
	assert.Equal(t, int64(5), resp.Msg.Total)
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

func TestListJobs_FallbackWithoutUIStorageUnimplemented(t *testing.T) {
	svc := newServiceWithBaseStorage(&mockStorage{})
	_, err := svc.ListJobs(context.Background(), connect.NewRequest(&jobsv1.ListJobsRequest{}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
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

func TestBulkRetryJobs_TooManyIDs(t *testing.T) {
	svc := newServiceWithUIStorage(&mockUIStorage{})
	_, err := svc.BulkRetryJobs(context.Background(), connect.NewRequest(&jobsv1.BulkRetryJobsRequest{
		Ids: jobIDs(maxBulkJobIDs + 1),
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
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

func TestBulkDeleteJobs_TooManyIDs(t *testing.T) {
	svc := newServiceWithUIStorage(&mockUIStorage{})
	_, err := svc.BulkDeleteJobs(context.Background(), connect.NewRequest(&jobsv1.BulkDeleteJobsRequest{
		Ids: jobIDs(maxBulkJobIDs + 1),
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
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

func TestPurgeQueue_RejectsEmptyNameAndStatus(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupServiceWithQueue(t)
	store := svc.storage.(UIStorage)
	now := time.Now()
	jobs := []*core.Job{
		{ID: "q-failed-1", Type: "work", Queue: "q", Status: core.StatusFailed, Args: []byte(`{}`), CreatedAt: now},
		{ID: "q-failed-2", Type: "work", Queue: "q", Status: core.StatusFailed, Args: []byte(`{}`), CreatedAt: now.Add(time.Second)},
		{ID: "other-failed", Type: "work", Queue: "other", Status: core.StatusFailed, Args: []byte(`{}`), CreatedAt: now.Add(2 * time.Second)},
		{ID: "q-completed", Type: "work", Queue: "q", Status: core.StatusCompleted, Args: []byte(`{}`), CreatedAt: now.Add(3 * time.Second)},
	}
	for _, job := range jobs {
		require.NoError(t, svc.storage.Enqueue(ctx, job))
	}

	_, err := svc.PurgeQueue(ctx, connect.NewRequest(&jobsv1.PurgeQueueRequest{Name: "", Status: "failed"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	_, total, err := store.SearchJobs(ctx, JobFilter{Status: string(core.StatusFailed), Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)

	_, err = svc.PurgeQueue(ctx, connect.NewRequest(&jobsv1.PurgeQueueRequest{Name: "q", Status: "bogus"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	_, total, err = store.SearchJobs(ctx, JobFilter{Status: string(core.StatusFailed), Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)

	// Non-terminal statuses must be rejected: purging a running/retrying job
	// would orphan a worker's lock, and purging a waiting fan-out parent would
	// corrupt fan-out accounting.
	for _, badStatus := range []string{"running", "waiting", "retrying"} {
		_, err = svc.PurgeQueue(ctx, connect.NewRequest(&jobsv1.PurgeQueueRequest{Name: "q", Status: badStatus}))
		require.Error(t, err, "status %q must be rejected", badStatus)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "status %q", badStatus)
	}
	_, total, err = store.SearchJobs(ctx, JobFilter{Status: string(core.StatusFailed), Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)

	resp, err := svc.PurgeQueue(ctx, connect.NewRequest(&jobsv1.PurgeQueueRequest{Name: "q", Status: "failed"}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Msg.Deleted)

	remainingFailed, total, err := store.SearchJobs(ctx, JobFilter{Status: string(core.StatusFailed), Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, remainingFailed, 1)
	assert.Equal(t, "other", remainingFailed[0].Queue)

	_, total, err = store.SearchJobs(ctx, JobFilter{Queue: "q", Status: string(core.StatusCompleted), Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
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

func TestCheckpointToProto_RedactsResultWithoutTruncating(t *testing.T) {
	secret := "sk_live_1234567890abcdef"
	sentinel := "CHECKPOINT_TAIL_SENTINEL"
	cp := &core.Checkpoint{
		ID:        "cp1",
		JobID:     "j1",
		CallIndex: 1,
		CallType:  "http.get",
		Result:    []byte(strings.Repeat("c", 5000) + " " + secret + " " + sentinel),
		CreatedAt: time.Now(),
	}

	pb := checkpointToProto(cp)

	assert.NotContains(t, string(pb.Result), secret)
	assert.Contains(t, string(pb.Result), "[REDACTED]")
	assert.Contains(t, string(pb.Result), sentinel)
	assert.Greater(t, len(pb.Result), 5000)
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

func TestJobToProto_RedactsPayloadsWithoutTruncating(t *testing.T) {
	argsSecret := "ghp_1234567890abcdefghij"
	resultSecret := "sk_live_1234567890abcdef"
	sentinel := "RESULT_TAIL_SENTINEL"
	job := sampleJob("j1", "default", "work", core.StatusCompleted)
	job.Args = []byte(`{"token":"` + argsSecret + `"}`)
	job.Result = []byte(strings.Repeat("r", 5000) + " " + resultSecret + " " + sentinel)
	job.LastError = "request failed bearer abcdefghijklmnopqrstuvwxyz012345"

	pb := jobToProto(job)

	assert.NotContains(t, string(pb.Args), argsSecret)
	assert.Contains(t, string(pb.Args), "[REDACTED]")
	assert.NotContains(t, string(pb.Result), resultSecret)
	assert.Contains(t, string(pb.Result), "[REDACTED]")
	assert.Contains(t, string(pb.Result), sentinel)
	assert.Greater(t, len(pb.Result), 5000)
	assert.NotContains(t, pb.LastError, "abcdefghijklmnopqrstuvwxyz012345")
	assert.Contains(t, pb.LastError, "bearer [REDACTED]")
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
	q.Schedule("daily-report", nil, schedule.Every(24*time.Hour))

	resp, err := svc.ListScheduledJobs(context.Background(), connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Jobs, 1)
	assert.Equal(t, "daily-report", resp.Msg.Jobs[0].Name)
}

// stringerSchedule is a schedule that also implements fmt.Stringer.
type stringerSchedule struct{ label string }

func (s *stringerSchedule) Next(from time.Time) time.Time { return from.Add(time.Hour) }
func (s *stringerSchedule) String() string                { return s.label }

func TestListScheduledJobs_WithStringerSchedule(t *testing.T) {
	svc, q := setupServiceWithQueue(t)

	q.Schedule("hourly-sync", nil, &stringerSchedule{label: "every 1h"})

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

// ---------------------------------------------------------------------------
// GetWorkflow tests
// ---------------------------------------------------------------------------

func TestGetWorkflow_NotFound(t *testing.T) {
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "nope"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestGetWorkflow_StorageError(t *testing.T) {
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, errors.New("db down")
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestGetWorkflow_SimpleRootNoChildren(t *testing.T) {
	root := sampleJob("root-1", "default", "my-job", core.StatusCompleted)
	store := &mockStorage{
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			if id == "root-1" {
				return root, nil
			}
			return nil, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "root-1"}))
	require.NoError(t, err)
	assert.Equal(t, "root-1", resp.Msg.Root.Id)
	assert.Empty(t, resp.Msg.FanOuts)
	assert.Empty(t, resp.Msg.Children)
}

func TestGetWorkflow_WalksParentChain(t *testing.T) {
	parentID := "parent-1"
	child := &core.Job{ID: "child-1", Queue: "default", Type: "t", Status: core.StatusCompleted,
		ParentJobID: &parentID, CreatedAt: time.Now()}
	parent := &core.Job{ID: "parent-1", Queue: "default", Type: "t", Status: core.StatusCompleted,
		CreatedAt: time.Now()}

	store := &mockStorage{
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			switch id {
			case "child-1":
				return child, nil
			case "parent-1":
				return parent, nil
			}
			return nil, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "child-1"}))
	require.NoError(t, err)
	// Root should be the parent, not the child.
	assert.Equal(t, "parent-1", resp.Msg.Root.Id)
}

func TestGetWorkflow_ParentLookupError(t *testing.T) {
	parentID := "parent-1"
	child := &core.Job{ID: "child-1", Queue: "default", Type: "t", Status: core.StatusRunning,
		ParentJobID: &parentID, CreatedAt: time.Now()}

	store := &mockStorage{
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			if id == "child-1" {
				return child, nil
			}
			return nil, errors.New("db fail")
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "child-1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestGetWorkflow_WithFanOutsAndChildren(t *testing.T) {
	now := time.Now()
	root := &core.Job{ID: "root", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	childA := &core.Job{ID: "child-a", Queue: "default", Type: "step", Status: core.StatusCompleted, CreatedAt: now}
	childB := &core.Job{ID: "child-b", Queue: "default", Type: "step", Status: core.StatusFailed, CreatedAt: now}

	fo := &core.FanOut{
		ID: "fo-1", ParentJobID: "root", TotalCount: 2, CompletedCount: 1, FailedCount: 1,
		Strategy: core.StrategyCollectAll, Status: core.FanOutCompleted, CreatedAt: now, UpdatedAt: now,
	}

	store := &mockStorage{
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			if id == "root" {
				return root, nil
			}
			return nil, nil
		},
		getFanOutsByParentFn: func(_ context.Context, parentID string) ([]*core.FanOut, error) {
			if parentID == "root" {
				return []*core.FanOut{fo}, nil
			}
			return nil, nil
		},
		getSubJobsFn: func(_ context.Context, fanOutID string) ([]*core.Job, error) {
			if fanOutID == "fo-1" {
				return []*core.Job{childA, childB}, nil
			}
			return nil, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "root"}))
	require.NoError(t, err)
	assert.Equal(t, "root", resp.Msg.Root.Id)
	assert.Len(t, resp.Msg.FanOuts, 1)
	assert.Equal(t, "fo-1", resp.Msg.FanOuts[0].Id)
	assert.Len(t, resp.Msg.Children, 2)
}

func TestGetWorkflow_BatchedWorkflowTree(t *testing.T) {
	now := time.Now()
	root := &core.Job{ID: "root", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	childA := &core.Job{ID: "child-a", Queue: "default", Type: "step", Status: core.StatusCompleted, CreatedAt: now}
	childB := &core.Job{ID: "child-b", Queue: "default", Type: "step", Status: core.StatusCompleted, CreatedAt: now}
	grandchild := &core.Job{ID: "grandchild", Queue: "default", Type: "step", Status: core.StatusCompleted, CreatedAt: now}
	fo1 := &core.FanOut{ID: "fo-1", ParentJobID: "root", TotalCount: 2, CompletedCount: 2, Strategy: core.StrategyCollectAll, Status: core.FanOutCompleted, CreatedAt: now, UpdatedAt: now}
	fo2 := &core.FanOut{ID: "fo-2", ParentJobID: "child-a", TotalCount: 1, CompletedCount: 1, Strategy: core.StrategyFailFast, Status: core.FanOutCompleted, CreatedAt: now, UpdatedAt: now}

	var parentBatchCalls, subBatchCalls int
	store := &mockWorkflowBatchStorage{
		mockStorage: mockStorage{
			getJobFn: func(_ context.Context, id string) (*core.Job, error) {
				if id == "root" {
					return root, nil
				}
				return nil, nil
			},
			getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
				t.Fatal("serial fan-out lookup should not be used")
				return nil, nil
			},
			getSubJobsFn: func(_ context.Context, _ string) ([]*core.Job, error) {
				t.Fatal("serial sub-job lookup should not be used")
				return nil, nil
			},
		},
		getFanOutsByParentsFn: func(_ context.Context, parentIDs []string) ([]*core.FanOut, error) {
			parentBatchCalls++
			switch parentBatchCalls {
			case 1:
				assert.Equal(t, []string{"root"}, parentIDs)
				return []*core.FanOut{fo1}, nil
			case 2:
				assert.Equal(t, []string{"child-a", "child-b"}, parentIDs)
				return []*core.FanOut{fo2}, nil
			case 3:
				assert.Equal(t, []string{"grandchild"}, parentIDs)
				return nil, nil
			default:
				t.Fatalf("unexpected parent batch call %d", parentBatchCalls)
			}
			return nil, nil
		},
		getSubJobsByFanOutsFn: func(_ context.Context, fanOutIDs []string) ([]*core.Job, error) {
			subBatchCalls++
			switch subBatchCalls {
			case 1:
				assert.Equal(t, []string{"fo-1"}, fanOutIDs)
				fanOutID := "fo-1"
				childA.FanOutID = &fanOutID
				childB.FanOutID = &fanOutID
				return []*core.Job{childA, childB}, nil
			case 2:
				assert.Equal(t, []string{"fo-2"}, fanOutIDs)
				fanOutID := "fo-2"
				grandchild.FanOutID = &fanOutID
				return []*core.Job{grandchild}, nil
			case 3:
				assert.Empty(t, fanOutIDs)
				return nil, nil
			default:
				t.Fatalf("unexpected sub-job batch call %d", subBatchCalls)
			}
			return nil, nil
		},
	}

	svc := newJobsService(store, nil, nil)
	resp, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "root"}))
	require.NoError(t, err)
	assert.Equal(t, "root", resp.Msg.Root.Id)
	require.Len(t, resp.Msg.FanOuts, 2)
	assert.Equal(t, "fo-1", resp.Msg.FanOuts[0].Id)
	assert.Equal(t, "fo-2", resp.Msg.FanOuts[1].Id)
	require.Len(t, resp.Msg.Children, 3)
	assert.Equal(t, "child-a", resp.Msg.Children[0].Id)
	assert.Equal(t, "child-b", resp.Msg.Children[1].Id)
	assert.Equal(t, "grandchild", resp.Msg.Children[2].Id)
	assert.Equal(t, 3, parentBatchCalls)
	assert.Equal(t, 3, subBatchCalls)
}

func TestGetWorkflow_FanOutsByParentError(t *testing.T) {
	root := sampleJob("root", "default", "wf", core.StatusCompleted)
	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return root, nil
		},
		getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
			return nil, errors.New("fanout query failed")
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "root"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestGetWorkflow_GetSubJobsError(t *testing.T) {
	now := time.Now()
	root := sampleJob("root", "default", "wf", core.StatusCompleted)
	fo := &core.FanOut{ID: "fo-1", ParentJobID: "root", TotalCount: 1, Strategy: core.StrategyFailFast,
		Status: core.FanOutPending, CreatedAt: now, UpdatedAt: now}

	store := &mockStorage{
		getJobFn: func(_ context.Context, _ string) (*core.Job, error) {
			return root, nil
		},
		getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
			return []*core.FanOut{fo}, nil
		},
		getSubJobsFn: func(_ context.Context, _ string) ([]*core.Job, error) {
			return nil, errors.New("subjob query failed")
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "root"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestGetWorkflow_ParentDeletedMidWalk(t *testing.T) {
	parentID := "gone-parent"
	child := &core.Job{ID: "child-1", Queue: "default", Type: "t", Status: core.StatusCompleted,
		ParentJobID: &parentID, CreatedAt: time.Now()}

	store := &mockStorage{
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			if id == "child-1" {
				return child, nil
			}
			// Parent no longer exists.
			return nil, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.GetWorkflow(context.Background(), connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: "child-1"}))
	require.NoError(t, err)
	// Child becomes the root since parent is gone.
	assert.Equal(t, "child-1", resp.Msg.Root.Id)
}

// ---------------------------------------------------------------------------
// ListWorkflows tests
// ---------------------------------------------------------------------------

func TestListWorkflows_NoUIStorage(t *testing.T) {
	store := &mockStorage{}
	svc := newJobsService(store, nil, nil)
	_, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestListWorkflows_Success(t *testing.T) {
	now := time.Now()
	root := &core.Job{ID: "root-1", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	fo := &core.FanOut{
		ID: "fo-1", ParentJobID: "root-1", TotalCount: 10, CompletedCount: 7, FailedCount: 2,
		Strategy: core.StrategyThreshold, Status: core.FanOutCompleted, CreatedAt: now, UpdatedAt: now,
	}

	store := &mockUIStorage{
		mockStorage: mockStorage{
			getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
				return []*core.FanOut{fo}, nil
			},
		},
		getWorkflowRootsFn: func(_ context.Context, _ string, _, _ int) ([]*core.Job, int64, error) {
			return []*core.Job{root}, 1, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{Limit: 10, Page: 1}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Workflows, 1)
	assert.Equal(t, "root-1", resp.Msg.Workflows[0].RootJob.Id)
	assert.Equal(t, "threshold", resp.Msg.Workflows[0].Strategy)
	assert.Equal(t, int32(10), resp.Msg.Workflows[0].TotalJobs)
	assert.Equal(t, int32(7), resp.Msg.Workflows[0].CompletedJobs)
	assert.Equal(t, int32(2), resp.Msg.Workflows[0].FailedJobs)
	assert.Equal(t, int32(1), resp.Msg.Workflows[0].RunningJobs)
	assert.Equal(t, int64(1), resp.Msg.Total)
}

func TestListWorkflows_RunningExcludesCancelled(t *testing.T) {
	now := time.Now()
	root := &core.Job{ID: "root-1", Queue: "default", Type: "wf", Status: core.StatusRunning, CreatedAt: now}
	fo := &core.FanOut{
		ID: "fo-1", ParentJobID: "root-1", TotalCount: 10, CompletedCount: 6, FailedCount: 1, CancelledCount: 2,
		Strategy: core.StrategyCollectAll, Status: core.FanOutPending, CreatedAt: now, UpdatedAt: now,
	}

	store := &mockUIStorage{
		mockStorage: mockStorage{
			getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
				return []*core.FanOut{fo}, nil
			},
		},
		getWorkflowRootsFn: func(_ context.Context, _ string, _, _ int) ([]*core.Job, int64, error) {
			return []*core.Job{root}, 1, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{Limit: 10, Page: 1}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Workflows, 1)
	assert.Equal(t, int32(10), resp.Msg.Workflows[0].TotalJobs)
	assert.Equal(t, int32(6), resp.Msg.Workflows[0].CompletedJobs)
	assert.Equal(t, int32(1), resp.Msg.Workflows[0].FailedJobs)
	assert.Equal(t, int32(1), resp.Msg.Workflows[0].RunningJobs)
}

func TestListWorkflows_BatchesFanOutLookup(t *testing.T) {
	now := time.Now()
	rootA := &core.Job{ID: "root-a", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	rootB := &core.Job{ID: "root-b", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	foA := &core.FanOut{ID: "fo-a", ParentJobID: "root-a", TotalCount: 3, CompletedCount: 2, Strategy: core.StrategyCollectAll, Status: core.FanOutPending, CreatedAt: now, UpdatedAt: now}
	foB := &core.FanOut{ID: "fo-b", ParentJobID: "root-b", TotalCount: 4, FailedCount: 1, Strategy: core.StrategyFailFast, Status: core.FanOutPending, CreatedAt: now, UpdatedAt: now}

	var batchCalls int
	store := &mockUIWorkflowBatchStorage{
		mockUIStorage: mockUIStorage{
			mockStorage: mockStorage{
				getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
					t.Fatal("serial fan-out lookup should not be used")
					return nil, nil
				},
			},
			getWorkflowRootsFn: func(_ context.Context, _ string, _, _ int) ([]*core.Job, int64, error) {
				return []*core.Job{rootA, rootB}, 2, nil
			},
		},
		getFanOutsByParentsFn: func(_ context.Context, parentIDs []string) ([]*core.FanOut, error) {
			batchCalls++
			assert.Equal(t, []string{"root-a", "root-b"}, parentIDs)
			return []*core.FanOut{foA, foB}, nil
		},
	}

	svc := newJobsService(store, nil, nil)
	resp, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{Limit: 10, Page: 1}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Workflows, 2)
	assert.Equal(t, "root-a", resp.Msg.Workflows[0].RootJob.Id)
	assert.Equal(t, int32(3), resp.Msg.Workflows[0].TotalJobs)
	assert.Equal(t, int32(1), resp.Msg.Workflows[0].RunningJobs)
	assert.Equal(t, "root-b", resp.Msg.Workflows[1].RootJob.Id)
	assert.Equal(t, int32(4), resp.Msg.Workflows[1].TotalJobs)
	assert.Equal(t, int32(3), resp.Msg.Workflows[1].RunningJobs)
	assert.Equal(t, 1, batchCalls)
}

func TestListWorkflows_DefaultPagination(t *testing.T) {
	store := &mockUIStorage{
		getWorkflowRootsFn: func(_ context.Context, _ string, limit, offset int) ([]*core.Job, int64, error) {
			assert.Equal(t, 50, limit) // default
			assert.Equal(t, 0, offset) // page 1
			return nil, 0, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Msg.Page)
}

func TestListWorkflows_LimitCappedAt100(t *testing.T) {
	store := &mockUIStorage{
		getWorkflowRootsFn: func(_ context.Context, _ string, limit, _ int) ([]*core.Job, int64, error) {
			assert.Equal(t, 50, limit) // >100 resets to default 50
			return nil, 0, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{Limit: 200}))
	require.NoError(t, err)
}

func TestListWorkflows_StorageError(t *testing.T) {
	store := &mockUIStorage{
		getWorkflowRootsFn: func(_ context.Context, _ string, _, _ int) ([]*core.Job, int64, error) {
			return nil, 0, errors.New("db fail")
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestListWorkflows_FanOutFetchError(t *testing.T) {
	now := time.Now()
	root := &core.Job{ID: "root-1", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	store := &mockUIStorage{
		mockStorage: mockStorage{
			getFanOutsByParentFn: func(_ context.Context, _ string) ([]*core.FanOut, error) {
				return nil, errors.New("fanout fail")
			},
		},
		getWorkflowRootsFn: func(_ context.Context, _ string, _, _ int) ([]*core.Job, int64, error) {
			return []*core.Job{root}, 1, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	_, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestListWorkflows_NoFanOuts(t *testing.T) {
	now := time.Now()
	root := &core.Job{ID: "root-1", Queue: "default", Type: "wf", Status: core.StatusCompleted, CreatedAt: now}
	store := &mockUIStorage{
		getWorkflowRootsFn: func(_ context.Context, _ string, _, _ int) ([]*core.Job, int64, error) {
			return []*core.Job{root}, 1, nil
		},
	}
	svc := newJobsService(store, nil, nil)
	resp, err := svc.ListWorkflows(context.Background(), connect.NewRequest(&jobsv1.ListWorkflowsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Workflows, 1)
	assert.Equal(t, "", resp.Msg.Workflows[0].Strategy)
	assert.Equal(t, int32(0), resp.Msg.Workflows[0].TotalJobs)
}

// ---------------------------------------------------------------------------
// WatchEvents tests
// ---------------------------------------------------------------------------

func TestWatchEvents_NilQueue(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so <-ctx.Done() returns
	err := svc.WatchEvents(ctx, connect.NewRequest(&jobsv1.WatchEventsRequest{}), nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWatchEvents_TooManyStreams(t *testing.T) {
	q := queue.New(&mockStorage{})
	svc := newJobsService(&mockStorage{}, q, nil)

	// Fill up to maxWatchStreams.
	svc.activeStreams.Store(int32(maxWatchStreams))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := svc.WatchEvents(ctx, connect.NewRequest(&jobsv1.WatchEventsRequest{}), nil)
	require.Error(t, err)
	assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	// Counter should be back to maxWatchStreams (decremented after rejection).
	assert.Equal(t, int32(maxWatchStreams), svc.activeStreams.Load())
}

func TestWatchEvents_ContextCancelled(t *testing.T) {
	q := queue.New(&mockStorage{})
	svc := newJobsService(&mockStorage{}, q, nil)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- svc.WatchEvents(ctx, connect.NewRequest(&jobsv1.WatchEventsRequest{}), nil)
	}()

	// Give the goroutine time to subscribe.
	time.Sleep(10 * time.Millisecond)
	cancel()

	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// fanOutToProto tests
// ---------------------------------------------------------------------------

func TestFanOutToProto_AllFields(t *testing.T) {
	now := time.Now()
	timeout := now.Add(time.Hour)
	fo := &core.FanOut{
		ID: "fo-1", ParentJobID: "parent-1", TotalCount: 10, CompletedCount: 7,
		FailedCount: 2, CancelledCount: 1, Strategy: core.StrategyThreshold,
		Threshold: 0.8, Status: core.FanOutCompleted, CancelOnFail: true,
		TimeoutAt: &timeout, CreatedAt: now, UpdatedAt: now,
	}
	pb := fanOutToProto(fo)
	assert.Equal(t, "fo-1", pb.Id)
	assert.Equal(t, "parent-1", pb.ParentJobId)
	assert.Equal(t, int32(10), pb.TotalCount)
	assert.Equal(t, int32(7), pb.CompletedCount)
	assert.Equal(t, int32(2), pb.FailedCount)
	assert.Equal(t, int32(1), pb.CancelledCount)
	assert.Equal(t, "threshold", pb.Strategy)
	assert.Equal(t, 0.8, pb.Threshold)
	assert.Equal(t, "completed", pb.Status)
	assert.True(t, pb.CancelOnFail)
	assert.NotNil(t, pb.TimeoutAt)
	assert.NotNil(t, pb.CreatedAt)
}

func TestFanOutToProto_NilTimeout(t *testing.T) {
	now := time.Now()
	fo := &core.FanOut{
		ID: "fo-2", ParentJobID: "p", TotalCount: 5,
		Strategy: core.StrategyFailFast, Status: core.FanOutPending,
		CreatedAt: now, UpdatedAt: now,
	}
	pb := fanOutToProto(fo)
	assert.Nil(t, pb.TimeoutAt)
}

// ---------------------------------------------------------------------------
// jobToProto — optional pointer fields (ParentJobID, RootJobID, FanOutID)
// ---------------------------------------------------------------------------

func TestJobToProto_WithParentRootFanOutIDs(t *testing.T) {
	parentID := "parent-1"
	rootID := "root-1"
	fanOutID := "fo-1"
	now := time.Now()
	j := &core.Job{
		ID: "j1", Queue: "default", Type: "step", Status: core.StatusCompleted,
		ParentJobID: &parentID, RootJobID: &rootID, FanOutID: &fanOutID,
		FanOutIndex: 3, CreatedAt: now,
	}
	pb := jobToProto(j)
	require.NotNil(t, pb.ParentJobId)
	assert.Equal(t, "parent-1", *pb.ParentJobId)
	require.NotNil(t, pb.RootJobId)
	assert.Equal(t, "root-1", *pb.RootJobId)
	require.NotNil(t, pb.FanOutId)
	assert.Equal(t, "fo-1", *pb.FanOutId)
	assert.Equal(t, int32(3), pb.FanOutIndex)
}

func TestJobToProto_WithRunAt(t *testing.T) {
	runAt := time.Now().Add(time.Hour)
	j := &core.Job{
		ID: "j2", Queue: "default", Type: "t", Status: core.StatusPending,
		RunAt: &runAt, CreatedAt: time.Now(),
	}
	pb := jobToProto(j)
	assert.NotNil(t, pb.RunAt)
}

// ---------------------------------------------------------------------------
// PauseJob tests
// ---------------------------------------------------------------------------

func TestPauseJob_Success(t *testing.T) {
	paused := sampleJob("j1", "default", "work", core.StatusPaused)

	store := &mockStorage{
		pauseJobFn: func(_ context.Context, id string) error {
			assert.Equal(t, "j1", id)
			return nil
		},
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			assert.Equal(t, "j1", id)
			return paused, nil
		},
	}

	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	resp, err := svc.PauseJob(context.Background(), connect.NewRequest(&jobsv1.PauseJobRequest{Id: "j1"}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Job)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
	assert.Equal(t, string(core.StatusPaused), resp.Msg.Job.Status)
}

func TestPauseJob_NilQueue_ReturnsUnimplemented(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	_, err := svc.PauseJob(context.Background(), connect.NewRequest(&jobsv1.PauseJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestPauseJob_StorageError(t *testing.T) {
	store := &mockStorage{
		pauseJobFn: func(_ context.Context, _ string) error {
			return errors.New("db error")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.PauseJob(context.Background(), connect.NewRequest(&jobsv1.PauseJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestPauseJob_JobNotFoundAfterPause(t *testing.T) {
	store := &mockStorage{
		pauseJobFn: func(_ context.Context, _ string) error { return nil },
		getJobFn:   func(_ context.Context, _ string) (*core.Job, error) { return nil, nil },
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.PauseJob(context.Background(), connect.NewRequest(&jobsv1.PauseJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// ---------------------------------------------------------------------------
// CancelJob tests
// ---------------------------------------------------------------------------

func TestCancelJob_Success(t *testing.T) {
	cancelled := sampleJob("j1", "default", "work", core.StatusCancelled)

	store := &mockStorage{
		pauseJobFn: func(_ context.Context, id string) error {
			assert.Equal(t, "j1", id)
			return nil
		},
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			assert.Equal(t, "j1", id)
			return cancelled, nil
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	resp, err := svc.CancelJob(context.Background(), connect.NewRequest(&jobsv1.CancelJobRequest{Id: "j1"}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Job)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
	assert.Equal(t, string(core.StatusCancelled), resp.Msg.Job.Status)
}

func TestCancelJob_NilQueue_ReturnsUnimplemented(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	_, err := svc.CancelJob(context.Background(), connect.NewRequest(&jobsv1.CancelJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestCancelJob_StorageError(t *testing.T) {
	store := &mockStorage{
		pauseJobFn: func(_ context.Context, _ string) error {
			return errors.New("db error")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.CancelJob(context.Background(), connect.NewRequest(&jobsv1.CancelJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestCancelJob_JobNotFoundAfterCancel(t *testing.T) {
	store := &mockStorage{
		pauseJobFn: func(_ context.Context, _ string) error { return nil },
		getJobFn:   func(_ context.Context, _ string) (*core.Job, error) { return nil, nil },
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.CancelJob(context.Background(), connect.NewRequest(&jobsv1.CancelJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// ---------------------------------------------------------------------------
// ResumeJob tests
// ---------------------------------------------------------------------------

func TestResumeJob_Success(t *testing.T) {
	resumed := sampleJob("j1", "default", "work", core.StatusPending)

	store := &mockStorage{
		unpauseJobFn: func(_ context.Context, id string) error {
			assert.Equal(t, "j1", id)
			return nil
		},
		getJobFn: func(_ context.Context, id string) (*core.Job, error) {
			assert.Equal(t, "j1", id)
			return resumed, nil
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	resp, err := svc.ResumeJob(context.Background(), connect.NewRequest(&jobsv1.ResumeJobRequest{Id: "j1"}))
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.Job)
	assert.Equal(t, "j1", resp.Msg.Job.Id)
}

func TestResumeJob_NilQueue_ReturnsUnimplemented(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	_, err := svc.ResumeJob(context.Background(), connect.NewRequest(&jobsv1.ResumeJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestResumeJob_UnpauseError(t *testing.T) {
	store := &mockStorage{
		unpauseJobFn: func(_ context.Context, _ string) error {
			return errors.New("db error")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.ResumeJob(context.Background(), connect.NewRequest(&jobsv1.ResumeJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestResumeJob_JobNotFoundAfterUnpause(t *testing.T) {
	store := &mockStorage{
		unpauseJobFn: func(_ context.Context, _ string) error { return nil },
		getJobFn:     func(_ context.Context, _ string) (*core.Job, error) { return nil, nil },
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.ResumeJob(context.Background(), connect.NewRequest(&jobsv1.ResumeJobRequest{Id: "j1"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// ---------------------------------------------------------------------------
// PauseQueue tests
// ---------------------------------------------------------------------------

func TestPauseQueue_Success(t *testing.T) {
	called := false
	store := &mockStorage{
		pauseQueueFn: func(_ context.Context, name string) error {
			assert.Equal(t, "emails", name)
			called = true
			return nil
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	resp, err := svc.PauseQueue(context.Background(), connect.NewRequest(&jobsv1.PauseQueueRequest{Name: "emails"}))
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, called)
}

func TestPauseQueue_NilQueue_ReturnsUnimplemented(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	_, err := svc.PauseQueue(context.Background(), connect.NewRequest(&jobsv1.PauseQueueRequest{Name: "emails"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestPauseQueue_StorageError(t *testing.T) {
	store := &mockStorage{
		pauseQueueFn: func(_ context.Context, _ string) error {
			return errors.New("db error")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.PauseQueue(context.Background(), connect.NewRequest(&jobsv1.PauseQueueRequest{Name: "emails"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

// ---------------------------------------------------------------------------
// ResumeQueue tests
// ---------------------------------------------------------------------------

func TestResumeQueue_Success(t *testing.T) {
	called := false
	store := &mockStorage{
		unpauseQueueFn: func(_ context.Context, name string) error {
			assert.Equal(t, "emails", name)
			called = true
			return nil
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	resp, err := svc.ResumeQueue(context.Background(), connect.NewRequest(&jobsv1.ResumeQueueRequest{Name: "emails"}))
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, called)
}

func TestResumeQueue_NilQueue_ReturnsUnimplemented(t *testing.T) {
	svc := newJobsService(&mockStorage{}, nil, nil)
	_, err := svc.ResumeQueue(context.Background(), connect.NewRequest(&jobsv1.ResumeQueueRequest{Name: "emails"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestResumeQueue_StorageError(t *testing.T) {
	store := &mockStorage{
		unpauseQueueFn: func(_ context.Context, _ string) error {
			return errors.New("db error")
		},
	}
	q := queue.New(store)
	svc := newJobsService(store, q, nil)

	_, err := svc.ResumeQueue(context.Background(), connect.NewRequest(&jobsv1.ResumeQueueRequest{Name: "emails"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

// ---------------------------------------------------------------------------
// GetStats — paused count propagation
// ---------------------------------------------------------------------------

func TestGetStats_IncludesPausedCount(t *testing.T) {
	qs := []*jobsv1.QueueStats{
		{Name: "default", Pending: 2, Running: 1, Completed: 5, Failed: 0, Paused: 3},
	}
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return qs, nil
		},
	}
	svc := newServiceWithUIStorage(store)

	resp, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.Msg.TotalPaused)
	require.Len(t, resp.Msg.Queues, 1)
	assert.Equal(t, int64(3), resp.Msg.Queues[0].Paused)
}

func TestGetStats_FallbackIncludesPausedCount(t *testing.T) {
	// Exercises the fallback path (plain mockStorage, not UIStorage) with StatusPaused jobs.
	pausedJobs := []*core.Job{
		sampleJob("j-p1", "default", "work", core.StatusPaused),
		sampleJob("j-p2", "default", "work", core.StatusPaused),
	}
	store := &mockStorage{
		getJobsByStatusFn: func(_ context.Context, status core.JobStatus, _ int) ([]*core.Job, error) {
			if status == core.StatusPaused {
				return pausedJobs, nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(store)

	resp, err := svc.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Msg.TotalPaused)
	require.Len(t, resp.Msg.Queues, 1)
	assert.Equal(t, int64(2), resp.Msg.Queues[0].Paused)
}
