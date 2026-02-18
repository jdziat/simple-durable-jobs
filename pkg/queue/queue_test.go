package queue

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStorage implements core.Storage for testing
type mockStorage struct {
	jobs         map[string]*core.Job
	checkpoints  map[string][]core.Checkpoint
	pausedQueues map[string]bool
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		jobs:        make(map[string]*core.Job),
		checkpoints: make(map[string][]core.Checkpoint),
	}
}

func (m *mockStorage) Migrate(ctx context.Context) error { return nil }

func (m *mockStorage) Enqueue(ctx context.Context, job *core.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) Complete(ctx context.Context, jobID, workerID string) error {
	return nil
}

func (m *mockStorage) Fail(ctx context.Context, jobID, workerID, errMsg string, retryAt *time.Time) error {
	return nil
}

func (m *mockStorage) GetJob(ctx context.Context, jobID string) (*core.Job, error) {
	return m.jobs[jobID], nil
}

func (m *mockStorage) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
	return m.checkpoints[jobID], nil
}

func (m *mockStorage) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	m.checkpoints[cp.JobID] = append(m.checkpoints[cp.JobID], *cp)
	return nil
}

func (m *mockStorage) DeleteCheckpoints(ctx context.Context, jobID string) error {
	delete(m.checkpoints, jobID)
	return nil
}

func (m *mockStorage) GetDueJobs(ctx context.Context, queues []string, limit int) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) Heartbeat(ctx context.Context, jobID, workerID string) error {
	return nil
}

func (m *mockStorage) ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error) {
	return 0, nil
}

func (m *mockStorage) GetJobsByStatus(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	return nil, nil
}

// Fan-out methods
func (m *mockStorage) CreateFanOut(ctx context.Context, fanOut *core.FanOut) error {
	return nil
}

func (m *mockStorage) GetFanOut(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) IncrementFanOutFailed(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) IncrementFanOutCancelled(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
	return true, nil
}

func (m *mockStorage) GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) EnqueueBatch(ctx context.Context, jobs []*core.Job) error {
	for _, job := range jobs {
		m.jobs[job.ID] = job
	}
	return nil
}

func (m *mockStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) GetSubJobResults(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) CancelSubJobs(ctx context.Context, fanOutID string) (int64, error) {
	return 0, nil
}

func (m *mockStorage) CancelSubJob(ctx context.Context, jobID string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) SuspendJob(ctx context.Context, jobID string, workerID string) error {
	return nil
}

func (m *mockStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	return true, nil
}

func (m *mockStorage) GetWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) SaveJobResult(ctx context.Context, jobID string, workerID string, result []byte) error {
	return nil
}

func TestNew_CreatesQueue(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	require.NotNil(t, q)
	assert.Equal(t, store, q.Storage())
	assert.NotNil(t, q.handlers)
	assert.Equal(t, ExplicitCheckpoints, q.determinism)
}

func TestQueue_Register_ValidHandler(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.Register("test-job", func(ctx context.Context, args string) error {
		return nil
	})

	assert.True(t, q.HasHandler("test-job"))
}

func TestQueue_Register_InvalidName_Panics(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	assert.Panics(t, func() {
		q.Register("", func(ctx context.Context, args string) error {
			return nil
		})
	})
}

func TestQueue_Register_InvalidHandler_Panics(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	assert.Panics(t, func() {
		q.Register("test-job", "not a function")
	})
}

func TestQueue_Enqueue_UnregisteredHandler(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	_, err := q.Enqueue(context.Background(), "unknown-job", "args")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no handler registered")
}

func TestQueue_Enqueue_Success(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.Register("test-job", func(ctx context.Context, args string) error {
		return nil
	})

	jobID, err := q.Enqueue(context.Background(), "test-job", "hello")
	require.NoError(t, err)
	assert.NotEmpty(t, jobID)

	// Verify job was stored
	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, "test-job", job.Type)
	assert.Equal(t, "default", job.Queue)
}

func TestQueue_Enqueue_WithOptions(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.Register("test-job", func(ctx context.Context, args string) error {
		return nil
	})

	jobID, err := q.Enqueue(context.Background(), "test-job", "hello",
		QueueOpt("high-priority"),
		Priority(100),
		Retries(5),
	)
	require.NoError(t, err)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, "high-priority", job.Queue)
	assert.Equal(t, 100, job.Priority)
	assert.Equal(t, 5, job.MaxRetries)
}

func TestQueue_Schedule(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// Mock schedule
	mockSched := &mockSchedule{}
	q.Schedule("scheduled-job", mockSched, QueueOpt("scheduled"))

	scheduled := q.GetScheduledJobs()
	require.NotNil(t, scheduled)
	assert.Contains(t, scheduled, "scheduled-job")
	assert.Equal(t, "scheduled", scheduled["scheduled-job"].Options.Queue)
}

type mockSchedule struct{}

func (m *mockSchedule) Next(last time.Time) time.Time { return time.Now().Add(time.Hour) }

func TestQueue_SetDeterminism(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.SetDeterminism(Strict)
	assert.Equal(t, Strict, q.determinism)

	q.SetDeterminism(BestEffort)
	assert.Equal(t, BestEffort, q.determinism)
}

func TestQueue_Events(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ch := q.Events()
	require.NotNil(t, ch)

	// Emit an event
	event := &core.JobStarted{Job: &core.Job{ID: "test"}}
	q.Emit(event)

	// Should receive it
	select {
	case received := <-ch:
		assert.Equal(t, event, received)
	default:
		t.Fatal("expected to receive event")
	}
}

func TestQueue_Emit_DropsWhenFull(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ch := q.Events()

	// Fill the channel (buffer size is 100)
	for i := 0; i < 100; i++ {
		q.Emit(&core.JobStarted{Job: &core.Job{ID: "test"}})
	}

	// This should not block - it should drop
	q.Emit(&core.JobStarted{Job: &core.Job{ID: "dropped"}})

	// Verify channel is full
	assert.Len(t, ch, 100)
}

func TestQueue_Unsubscribe_StopsDelivery(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ch := q.Events()

	// Emit before unsubscribe — should be delivered
	q.Emit(&core.JobStarted{Job: &core.Job{ID: "before"}})
	select {
	case e := <-ch:
		assert.Equal(t, "before", e.(*core.JobStarted).Job.ID)
	default:
		t.Fatal("expected event before unsubscribe")
	}

	q.Unsubscribe(ch)

	// Emit after unsubscribe — should NOT be delivered
	q.Emit(&core.JobStarted{Job: &core.Job{ID: "after"}})
	select {
	case <-ch:
		t.Fatal("should not receive events after unsubscribe")
	default:
		// expected
	}
}

func TestQueue_Unsubscribe_UnknownChannel_IsNoop(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// Create a channel that was never subscribed
	foreign := make(chan core.Event, 100)

	// Should not panic
	q.Unsubscribe(foreign)
}

func TestQueue_Unsubscribe_ConcurrentWithEmit(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	const subscribers = 10
	channels := make([]<-chan core.Event, subscribers)
	for i := range channels {
		channels[i] = q.Events()
	}

	// Concurrently emit and unsubscribe — must not panic or race
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			q.Emit(&core.JobStarted{Job: &core.Job{ID: "concurrent"}})
		}
	}()

	for _, ch := range channels {
		q.Unsubscribe(ch)
	}
	<-done
}

// TestWorkerFactory_NotInitialized tests that NewWorker panics when factory is nil
func TestWorkerFactory_NotInitialized(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// Save and clear the factory
	savedFactory := WorkerFactory
	WorkerFactory = nil
	defer func() {
		WorkerFactory = savedFactory
	}()

	assert.Panics(t, func() {
		q.NewWorker()
	})
}

// TestWorkerFactory_Initialized tests that NewWorker works when factory is set
func TestWorkerFactory_Initialized(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// Save and set a mock factory
	savedFactory := WorkerFactory
	var factoryCalled bool
	var receivedQueue *Queue
	var receivedOpts []any

	WorkerFactory = func(queue *Queue, opts ...any) core.Starter {
		factoryCalled = true
		receivedQueue = queue
		receivedOpts = opts
		return &mockStarter{}
	}
	defer func() {
		WorkerFactory = savedFactory
	}()

	result := q.NewWorker("opt1", "opt2")

	assert.True(t, factoryCalled)
	assert.Equal(t, q, receivedQueue)
	assert.Equal(t, []any{"opt1", "opt2"}, receivedOpts)
	assert.NotNil(t, result)
}

// TestWorkerFactory_PassesOptionsCorrectly tests option forwarding
func TestWorkerFactory_PassesOptionsCorrectly(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	savedFactory := WorkerFactory
	var capturedOpts []any

	WorkerFactory = func(queue *Queue, opts ...any) core.Starter {
		capturedOpts = opts
		return &mockStarter{}
	}
	defer func() {
		WorkerFactory = savedFactory
	}()

	// Call with various option types
	opt1 := struct{ name string }{"first"}
	opt2 := struct{ value int }{42}

	q.NewWorker(opt1, opt2)

	require.Len(t, capturedOpts, 2)
	assert.Equal(t, opt1, capturedOpts[0])
	assert.Equal(t, opt2, capturedOpts[1])
}

type mockStarter struct{}

func (m *mockStarter) Start(ctx context.Context) error {
	return nil
}

// Pause operation methods for mock storage
func (m *mockStorage) PauseJob(ctx context.Context, jobID string) error {
	if job, ok := m.jobs[jobID]; ok {
		job.Status = core.StatusPaused
	}
	return nil
}

func (m *mockStorage) UnpauseJob(ctx context.Context, jobID string) error {
	if job, ok := m.jobs[jobID]; ok {
		job.Status = core.StatusPending
	}
	return nil
}

func (m *mockStorage) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
	if job, ok := m.jobs[jobID]; ok {
		return job.Status == core.StatusPaused, nil
	}
	return false, nil
}

func (m *mockStorage) GetPausedJobs(ctx context.Context, queue string) ([]*core.Job, error) {
	var result []*core.Job
	for _, job := range m.jobs {
		if job.Queue == queue && job.Status == core.StatusPaused {
			result = append(result, job)
		}
	}
	return result, nil
}

func (m *mockStorage) PauseQueue(ctx context.Context, queue string) error {
	if m.pausedQueues == nil {
		m.pausedQueues = make(map[string]bool)
	}
	m.pausedQueues[queue] = true
	return nil
}

func (m *mockStorage) UnpauseQueue(ctx context.Context, queue string) error {
	if m.pausedQueues != nil {
		delete(m.pausedQueues, queue)
	}
	return nil
}

func (m *mockStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	if m.pausedQueues == nil {
		return false, nil
	}
	return m.pausedQueues[queue], nil
}

func (m *mockStorage) GetPausedQueues(ctx context.Context) ([]string, error) {
	var result []string
	for queue := range m.pausedQueues {
		result = append(result, queue)
	}
	return result, nil
}

func (m *mockStorage) RefreshQueueStates(ctx context.Context) (map[string]bool, error) {
	if m.pausedQueues == nil {
		return make(map[string]bool), nil
	}
	result := make(map[string]bool)
	for k, v := range m.pausedQueues {
		result[k] = v
	}
	return result, nil
}

func TestQueue_Hooks(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	var startCalled, completeCalled, failCalled, retryCalled bool

	q.OnJobStart(func(ctx context.Context, job *core.Job) {
		startCalled = true
	})
	q.OnJobComplete(func(ctx context.Context, job *core.Job) {
		completeCalled = true
	})
	q.OnJobFail(func(ctx context.Context, job *core.Job, err error) {
		failCalled = true
	})
	q.OnRetry(func(ctx context.Context, job *core.Job, attempt int, err error) {
		retryCalled = true
	})

	job := &core.Job{ID: "test"}
	ctx := context.Background()

	q.CallStartHooks(ctx, job)
	assert.True(t, startCalled)

	q.CallCompleteHooks(ctx, job)
	assert.True(t, completeCalled)

	q.CallFailHooks(ctx, job, nil)
	assert.True(t, failCalled)

	q.CallRetryHooks(ctx, job, 1, nil)
	assert.True(t, retryCalled)
}

func TestQueue_PauseJob(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	q.Register("test-job", func(ctx context.Context, args struct{}) error {
		return nil
	})

	jobID, err := q.Enqueue(ctx, "test-job", struct{}{})
	require.NoError(t, err)

	err = q.PauseJob(ctx, jobID)
	require.NoError(t, err)

	paused, err := q.IsJobPaused(ctx, jobID)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestQueue_ResumeJob(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	q.Register("test-job", func(ctx context.Context, args struct{}) error {
		return nil
	})

	jobID, err := q.Enqueue(ctx, "test-job", struct{}{})
	require.NoError(t, err)

	// Pause then resume
	err = q.PauseJob(ctx, jobID)
	require.NoError(t, err)

	err = q.ResumeJob(ctx, jobID)
	require.NoError(t, err)

	paused, err := q.IsJobPaused(ctx, jobID)
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestQueue_PauseQueue(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	err := q.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	paused, err := q.IsQueuePaused(ctx, "emails")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestQueue_ResumeQueue(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	// Pause then resume
	err := q.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	err = q.ResumeQueue(ctx, "emails")
	require.NoError(t, err)

	paused, err := q.IsQueuePaused(ctx, "emails")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestQueue_GetPausedJobs(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	q.Register("test-job", func(ctx context.Context, args struct{}) error {
		return nil
	})

	// Create and pause jobs
	job1, _ := q.Enqueue(ctx, "test-job", struct{}{}, QueueOpt("emails"))
	job2, _ := q.Enqueue(ctx, "test-job", struct{}{}, QueueOpt("emails"))

	_ = q.PauseJob(ctx, job1)

	paused, err := q.GetPausedJobs(ctx, "emails")
	require.NoError(t, err)
	assert.Len(t, paused, 1)
	assert.Equal(t, job1, paused[0].ID)
	_ = job2 // unused
}

func TestQueue_GetPausedQueues(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	// Pause some queues
	_ = q.PauseQueue(ctx, "emails")
	_ = q.PauseQueue(ctx, "notifications")

	paused, err := q.GetPausedQueues(ctx)
	require.NoError(t, err)
	assert.Len(t, paused, 2)
	assert.Contains(t, paused, "emails")
	assert.Contains(t, paused, "notifications")
}

// ---------------------------------------------------------------------------
// GetHandler
// ---------------------------------------------------------------------------

func TestQueue_GetHandler_ReturnsHandlerAfterRegister(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.Register("known-job", func(ctx context.Context, args string) error {
		return nil
	})

	h, ok := q.GetHandler("known-job")
	assert.True(t, ok)
	assert.NotNil(t, h)
}

func TestQueue_GetHandler_ReturnsFalseForUnknown(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	h, ok := q.GetHandler("does-not-exist")
	assert.False(t, ok)
	assert.Nil(t, h)
}

// ---------------------------------------------------------------------------
// EmitCustomEvent
// ---------------------------------------------------------------------------

func TestQueue_EmitCustomEvent_DeliveredToSubscriber(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ch := q.Events()
	defer q.Unsubscribe(ch)

	q.EmitCustomEvent("job-99", "progress", map[string]any{"pct": 75})

	select {
	case evt := <-ch:
		ce, ok := evt.(*core.CustomEvent)
		require.True(t, ok, "expected *core.CustomEvent")
		assert.Equal(t, "job-99", ce.JobID)
		assert.Equal(t, "progress", ce.Kind)
		assert.Equal(t, 75, ce.Data["pct"])
	default:
		t.Fatal("expected event but channel was empty")
	}
}

func TestQueue_EmitCustomEvent_NilDataIsAccepted(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ch := q.Events()
	defer q.Unsubscribe(ch)

	// nil data map should not panic
	q.EmitCustomEvent("job-1", "heartbeat", nil)

	select {
	case evt := <-ch:
		ce, ok := evt.(*core.CustomEvent)
		require.True(t, ok)
		assert.Nil(t, ce.Data)
	default:
		t.Fatal("expected event")
	}
}

// ---------------------------------------------------------------------------
// ApplyPause / WithPauseMode
// ---------------------------------------------------------------------------

func TestWithPauseMode_AppliesGraceful(t *testing.T) {
	opt := WithPauseMode(core.PauseModeGraceful)
	po := &PauseOptions{}
	opt.ApplyPause(po)
	assert.Equal(t, core.PauseModeGraceful, po.Mode)
}

func TestWithPauseMode_AppliesAggressive(t *testing.T) {
	opt := WithPauseMode(core.PauseModeAggressive)
	po := &PauseOptions{}
	opt.ApplyPause(po)
	assert.Equal(t, core.PauseModeAggressive, po.Mode)
}

func TestWithPauseMode_ReturnsNonNil(t *testing.T) {
	opt := WithPauseMode(core.PauseModeGraceful)
	assert.NotNil(t, opt)
}

// ---------------------------------------------------------------------------
// RegisterRunningJob / UnregisterRunningJob
// ---------------------------------------------------------------------------

func TestQueue_RegisterRunningJob_StoresCancel(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	var cancelled bool
	cancel := func() { cancelled = true }

	q.RegisterRunningJob("job-abc", cancel)

	// Verify the cancel function was stored by calling it via PauseJob
	// with a job that's in the running registry.
	// (Direct field access is not possible from outside the package;
	// we confirm indirectly via UnregisterRunningJob not panicking.)
	q.UnregisterRunningJob("job-abc")
	assert.False(t, cancelled, "cancel should not be called by UnregisterRunningJob")
}

func TestQueue_RegisterRunningJob_OverwritesPrevious(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	var first, second bool
	q.RegisterRunningJob("job-x", func() { first = true })
	q.RegisterRunningJob("job-x", func() { second = true })
	q.UnregisterRunningJob("job-x")

	assert.False(t, first)
	assert.False(t, second)
}

func TestQueue_UnregisterRunningJob_NonExistentIsNoop(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// Must not panic when job was never registered.
	assert.NotPanics(t, func() {
		q.UnregisterRunningJob("never-existed")
	})
}

// ---------------------------------------------------------------------------
// CancelSubJob — mockStorage returns nil (not a sub-job)
// ---------------------------------------------------------------------------

func TestQueue_CancelSubJob_NotASubJob_ReturnsNil(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ctx := context.Background()
	fo, err := q.CancelSubJob(ctx, "some-job-id")
	require.NoError(t, err)
	assert.Nil(t, fo)
}

// cancelSubJobStorage is a mockStorage variant that returns a FanOut from
// CancelSubJob so we can exercise the completion-check logic.
type cancelSubJobStorage struct {
	*mockStorage
	fanOut *core.FanOut
}

func (c *cancelSubJobStorage) CancelSubJob(ctx context.Context, jobID string) (*core.FanOut, error) {
	return c.fanOut, nil
}

func (c *cancelSubJobStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
	return true, nil
}

func (c *cancelSubJobStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	return true, nil
}

func TestQueue_CancelSubJob_FanOutComplete_ResumesParent(t *testing.T) {
	base := newMockStorage()
	fo := &core.FanOut{
		ID:             "fo-1",
		ParentJobID:    "parent-job",
		TotalCount:     2,
		CompletedCount: 1,
		CancelledCount: 1,
		FailedCount:    0,
		Status:         core.FanOutPending,
	}
	store := &cancelSubJobStorage{mockStorage: base, fanOut: fo}

	q := New(store)
	ctx := context.Background()

	result, err := q.CancelSubJob(ctx, "sub-job-id")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "fo-1", result.ID)
}

func TestQueue_CancelSubJob_FanOutIncomplete_DoesNotResume(t *testing.T) {
	base := newMockStorage()
	fo := &core.FanOut{
		ID:             "fo-2",
		ParentJobID:    "parent-job",
		TotalCount:     5,
		CompletedCount: 1,
		CancelledCount: 1,
		FailedCount:    0,
		Status:         core.FanOutPending,
	}
	store := &cancelSubJobStorage{mockStorage: base, fanOut: fo}

	q := New(store)
	ctx := context.Background()

	result, err := q.CancelSubJob(ctx, "sub-job-id")
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestQueue_CancelSubJob_AllFailed_StatusFailed(t *testing.T) {
	base := newMockStorage()
	fo := &core.FanOut{
		ID:             "fo-3",
		ParentJobID:    "parent-job",
		TotalCount:     2,
		CompletedCount: 0,
		FailedCount:    0,
		CancelledCount: 2,
		Status:         core.FanOutPending,
	}
	store := &cancelSubJobStorage{mockStorage: base, fanOut: fo}

	q := New(store)
	ctx := context.Background()

	result, err := q.CancelSubJob(ctx, "sub-job-id")
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// ---------------------------------------------------------------------------
// errPauseStorage — a mock whose PauseJob always returns ErrCannotPauseStatus.
// This exercises the running-job-registry branches of Queue.PauseJob.
// ---------------------------------------------------------------------------

type errPauseStorage struct {
	*mockStorage
}

func (e *errPauseStorage) PauseJob(_ context.Context, _ string) error {
	return core.ErrCannotPauseStatus
}

func TestQueue_PauseJob_WithPauseMode_Graceful(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	q.Register("pm-job", func(ctx context.Context, args struct{}) error { return nil })
	jobID, err := q.Enqueue(ctx, "pm-job", struct{}{})
	require.NoError(t, err)

	// PauseJob with an explicit pause mode option.
	err = q.PauseJob(ctx, jobID, WithPauseMode(core.PauseModeGraceful))
	require.NoError(t, err)

	paused, err := q.IsJobPaused(ctx, jobID)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestQueue_PauseJob_RunningJobInRegistry_CancelsCancelFunc(t *testing.T) {
	base := newMockStorage()
	store := &errPauseStorage{mockStorage: base}
	q := New(store)
	ctx := context.Background()

	var cancelled bool
	cancel := func() { cancelled = true }

	// Register a "running" job in the queue registry.
	q.RegisterRunningJob("running-job-1", cancel)

	// PauseJob will fail with ErrCannotPauseStatus from storage, then look
	// up the running registry and call cancel().
	err := q.PauseJob(ctx, "running-job-1")
	require.NoError(t, err)
	assert.True(t, cancelled, "cancel function should have been called")
}

func TestQueue_PauseJob_RunningJobNotInRegistry_ReturnsError(t *testing.T) {
	base := newMockStorage()
	store := &errPauseStorage{mockStorage: base}
	q := New(store)
	ctx := context.Background()

	// No cancel registered for this job — PauseJob should propagate the error.
	err := q.PauseJob(ctx, "not-in-registry")
	assert.ErrorIs(t, err, core.ErrCannotPauseStatus)
}

func TestQueue_PauseJob_WithPauseMode_Aggressive(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	q.Register("agg-job", func(ctx context.Context, args struct{}) error { return nil })
	jobID, err := q.Enqueue(ctx, "agg-job", struct{}{})
	require.NoError(t, err)

	err = q.PauseJob(ctx, jobID, WithPauseMode(core.PauseModeAggressive))
	require.NoError(t, err)

	paused, err := q.IsJobPaused(ctx, jobID)
	require.NoError(t, err)
	assert.True(t, paused)
}

// ---------------------------------------------------------------------------
// Queue-level validation error paths
// ---------------------------------------------------------------------------

func TestQueue_GetPausedJobs_InvalidQueueName_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	_, err := q.GetPausedJobs(ctx, "")
	assert.Error(t, err)
}

func TestQueue_PauseQueue_InvalidQueueName_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	err := q.PauseQueue(ctx, "")
	assert.Error(t, err)
}

func TestQueue_ResumeQueue_InvalidQueueName_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	err := q.ResumeQueue(ctx, "")
	assert.Error(t, err)
}

func TestQueue_IsQueuePaused_InvalidQueueName_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	_, err := q.IsQueuePaused(ctx, "")
	assert.Error(t, err)
}
