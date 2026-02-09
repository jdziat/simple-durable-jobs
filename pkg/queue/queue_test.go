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
	jobs        map[string]*core.Job
	checkpoints map[string][]core.Checkpoint
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
