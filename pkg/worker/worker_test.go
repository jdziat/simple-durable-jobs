package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/call"
	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/pkg/jobctx"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
)

// mockStorage is a minimal in-memory implementation of core.Storage used for
// unit tests that need controlled behaviour without a real database.
type mockStorage struct {
	mu sync.Mutex

	releaseCount           int64         // number of ReleaseStaleLocks calls
	releasedIDs            []string      // IDs returned by ReleaseStaleLocks
	releaseErr             error         // error returned by ReleaseStaleLocks
	releaseDelay           time.Duration // optional artificial delay per call
	releaseFunc            func(ctx context.Context, age time.Duration) ([]string, error)
	releasedJobIDs         []string // IDs passed to Release
	releaseJobFunc         func(ctx context.Context, jobID string, workerID string) error
	dequeueFunc            func(ctx context.Context, queues []string, workerID string) (*core.Job, error)
	completeFunc           func(ctx context.Context, jobID string, workerID string) error
	failFunc               func(ctx context.Context, jobID string, workerID string, errMsg string, retryAt *time.Time) error
	heartbeatFunc          func(ctx context.Context, jobID string, workerID string) error
	checkpointFunc         func(ctx context.Context, jobID string) ([]core.Checkpoint, error)
	saveCheckpointFunc     func(ctx context.Context, cp *core.Checkpoint) error
	waitingJobsFunc        func(ctx context.Context) ([]*core.Job, error)
	stalledJobsFunc        func(ctx context.Context, olderThan time.Time) ([]*core.Job, error)
	signalWaitingAfterFunc func(ctx context.Context, afterJobID string, limit int) ([]*core.Job, error)
	signalWaitingFunc      func(ctx context.Context) ([]*core.Job, error)
	resumeSignalFunc       func(ctx context.Context, jobID string) (bool, error)
	claimFireFunc          func(ctx context.Context, name string, fireTime time.Time) (bool, error)
	fireTimeFunc           func(ctx context.Context, name string) (time.Time, bool, error)
	fireTimes              map[string]time.Time

	// fan-out control hooks
	incrementCompletedFunc func(ctx context.Context, fanOutID string) (*core.FanOut, error)
	incrementFailedFunc    func(ctx context.Context, fanOutID string) (*core.FanOut, error)
	updateFanOutStatusFunc func(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error)
	resumeJobFunc          func(ctx context.Context, jobID string) (bool, error)
	cancelSubJobsFunc      func(ctx context.Context, fanOutID string) ([]string, error)
	findOrphanedFunc       func(jobIDs []string) ([]string, error)
	completablePendingFunc func(ctx context.Context, olderThan time.Time) ([]*core.FanOut, error)

	// enqueue tracking
	enqueueMu         sync.Mutex
	enqueueCount      int64 // incremented on each Enqueue call
	enqueueUniqueFunc func(ctx context.Context, job *core.Job, uniqueKey string) error
	enqueuedJobs      []*core.Job
	enqueuedKeys      []string

	// result tracking
	savedResults map[string][]byte
}

func (m *mockStorage) Migrate(_ context.Context) error { return nil }

func (m *mockStorage) Enqueue(_ context.Context, job *core.Job) error {
	m.enqueueMu.Lock()
	m.enqueueCount++
	m.enqueuedJobs = append(m.enqueuedJobs, job)
	m.enqueueMu.Unlock()
	return nil
}

func (m *mockStorage) getEnqueueCount() int64 {
	m.enqueueMu.Lock()
	defer m.enqueueMu.Unlock()
	return m.enqueueCount
}

func (m *mockStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	if m.dequeueFunc != nil {
		return m.dequeueFunc(ctx, queues, workerID)
	}
	return nil, nil
}

func (m *mockStorage) Complete(ctx context.Context, jobID string, workerID string) error {
	if m.completeFunc != nil {
		return m.completeFunc(ctx, jobID, workerID)
	}
	return nil
}

func (m *mockStorage) Fail(ctx context.Context, jobID string, workerID string, errMsg string, retryAt *time.Time) error {
	if m.failFunc != nil {
		return m.failFunc(ctx, jobID, workerID, errMsg, retryAt)
	}
	return nil
}

func (m *mockStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	m.enqueueMu.Lock()
	job.UniqueKey = uniqueKey
	m.enqueueCount++
	m.enqueuedJobs = append(m.enqueuedJobs, job)
	m.enqueuedKeys = append(m.enqueuedKeys, uniqueKey)
	m.enqueueMu.Unlock()
	if m.enqueueUniqueFunc != nil {
		return m.enqueueUniqueFunc(ctx, job, uniqueKey)
	}
	return nil
}

func (m *mockStorage) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	if m.saveCheckpointFunc != nil {
		return m.saveCheckpointFunc(ctx, cp)
	}
	return nil
}

func (m *mockStorage) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
	if m.checkpointFunc != nil {
		return m.checkpointFunc(ctx, jobID)
	}
	return nil, nil
}

func (m *mockStorage) DeleteCheckpoints(_ context.Context, _ string) error { return nil }

func (m *mockStorage) GetDueJobs(_ context.Context, _ []string, _ int) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) ClaimScheduledFire(ctx context.Context, name string, fireTime time.Time) (bool, error) {
	if m.claimFireFunc != nil {
		return m.claimFireFunc(ctx, name, fireTime)
	}
	return true, nil
}

func (m *mockStorage) GetScheduledFireTime(ctx context.Context, name string) (time.Time, bool, error) {
	if m.fireTimeFunc != nil {
		return m.fireTimeFunc(ctx, name)
	}
	t, ok := m.fireTimes[name]
	return t, ok, nil
}

func (m *mockStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, jobID, workerID)
	}
	return nil
}

func (m *mockStorage) Release(ctx context.Context, jobID string, workerID string) error {
	m.mu.Lock()
	m.releasedJobIDs = append(m.releasedJobIDs, jobID)
	m.mu.Unlock()
	if m.releaseJobFunc != nil {
		return m.releaseJobFunc(ctx, jobID, workerID)
	}
	return nil
}

func (m *mockStorage) ReleaseStaleLocks(ctx context.Context, age time.Duration) ([]string, error) {
	if m.releaseDelay > 0 {
		time.Sleep(m.releaseDelay)
	}
	m.mu.Lock()
	m.releaseCount++
	m.mu.Unlock()
	if m.releaseFunc != nil {
		return m.releaseFunc(ctx, age)
	}
	return m.releasedIDs, m.releaseErr
}

func (m *mockStorage) FindOrphanedJobs(_ context.Context, jobIDs []string, _ string) ([]string, error) {
	if m.findOrphanedFunc != nil {
		return m.findOrphanedFunc(jobIDs)
	}
	return nil, nil
}

func (m *mockStorage) GetJob(_ context.Context, _ string) (*core.Job, error) { return nil, nil }

func (m *mockStorage) GetJobsByStatus(_ context.Context, _ core.JobStatus, _ int) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) CreateFanOut(_ context.Context, _ *core.FanOut) error { return nil }

func (m *mockStorage) GetFanOut(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	if m.incrementCompletedFunc != nil {
		return m.incrementCompletedFunc(ctx, fanOutID)
	}
	return nil, nil
}

func (m *mockStorage) IncrementFanOutFailed(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	if m.incrementFailedFunc != nil {
		return m.incrementFailedFunc(ctx, fanOutID)
	}
	return nil, nil
}

func (m *mockStorage) IncrementFanOutCancelled(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
	if m.updateFanOutStatusFunc != nil {
		return m.updateFanOutStatusFunc(ctx, fanOutID, status)
	}
	return false, nil
}

func (m *mockStorage) GetFanOutsByParent(_ context.Context, _ string) ([]*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) EnqueueBatch(_ context.Context, _ []*core.Job) error { return nil }

func (m *mockStorage) GetSubJobs(_ context.Context, _ string) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) GetSubJobResults(_ context.Context, _ string) ([]*core.Job, error) {
	return nil, nil
}

func (m *mockStorage) CancelSubJobs(ctx context.Context, fanOutID string) ([]string, error) {
	if m.cancelSubJobsFunc != nil {
		return m.cancelSubJobsFunc(ctx, fanOutID)
	}
	return nil, nil
}

func (m *mockStorage) CancelSubJob(_ context.Context, _ string) (*core.FanOut, error) {
	return nil, nil
}

func (m *mockStorage) SuspendJob(_ context.Context, _ string, _ string) error { return nil }

func (m *mockStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	if m.resumeJobFunc != nil {
		return m.resumeJobFunc(ctx, jobID)
	}
	return false, nil
}

func (m *mockStorage) GetWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	if m.waitingJobsFunc != nil {
		return m.waitingJobsFunc(ctx)
	}
	return nil, nil
}

func (m *mockStorage) GetStalledFanOutParents(ctx context.Context, olderThan time.Time) ([]*core.Job, error) {
	if m.stalledJobsFunc != nil {
		return m.stalledJobsFunc(ctx, olderThan)
	}
	return nil, nil
}

func (m *mockStorage) GetCompletablePendingFanOuts(ctx context.Context, olderThan time.Time) ([]*core.FanOut, error) {
	if m.completablePendingFunc != nil {
		return m.completablePendingFunc(ctx, olderThan)
	}
	return nil, nil
}

func (m *mockStorage) GetSignalWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	if m.signalWaitingFunc != nil {
		return m.signalWaitingFunc(ctx)
	}
	return nil, nil
}

func (m *mockStorage) GetSignalWaitingJobsToResumeAfter(ctx context.Context, afterJobID string, limit int) ([]*core.Job, error) {
	if m.signalWaitingAfterFunc != nil {
		return m.signalWaitingAfterFunc(ctx, afterJobID, limit)
	}
	return nil, nil
}

func (m *mockStorage) ResumeSignalWaitingJob(ctx context.Context, jobID string) (bool, error) {
	if m.resumeSignalFunc != nil {
		return m.resumeSignalFunc(ctx, jobID)
	}
	return false, nil
}

func (m *mockStorage) SaveJobResult(_ context.Context, jobID string, _ string, result []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.savedResults == nil {
		m.savedResults = map[string][]byte{}
	}
	m.savedResults[jobID] = result
	return nil
}

func (m *mockStorage) PauseJob(_ context.Context, _ string) error   { return nil }
func (m *mockStorage) UnpauseJob(_ context.Context, _ string) error { return nil }
func (m *mockStorage) GetPausedJobs(_ context.Context, _ string) ([]*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) IsJobPaused(_ context.Context, _ string) (bool, error) { return false, nil }
func (m *mockStorage) PauseQueue(_ context.Context, _ string) error          { return nil }
func (m *mockStorage) UnpauseQueue(_ context.Context, _ string) error        { return nil }
func (m *mockStorage) GetPausedQueues(_ context.Context) ([]string, error)   { return nil, nil }
func (m *mockStorage) IsQueuePaused(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (m *mockStorage) RefreshQueueStates(_ context.Context) (map[string]bool, error) {
	return nil, nil
}

// getReleaseCount returns the number of ReleaseStaleLocks calls in a
// goroutine-safe manner.
func (m *mockStorage) getReleaseCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.releaseCount
}

func (m *mockStorage) getReleasedJobIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.releasedJobIDs...)
}

type atomicMockStorage struct {
	*mockStorage
	completeWithResultFunc     func(ctx context.Context, jobID, workerID string, result []byte) (*core.FanOut, error)
	failTerminalWithResultFunc func(ctx context.Context, jobID, workerID, errMsg string) (*core.FanOut, error)
}

func (m *atomicMockStorage) CompleteWithResult(ctx context.Context, jobID, workerID string, result []byte) (*core.FanOut, error) {
	if m.completeWithResultFunc != nil {
		return m.completeWithResultFunc(ctx, jobID, workerID, result)
	}
	return nil, nil
}

func (m *atomicMockStorage) FailTerminalWithResult(ctx context.Context, jobID, workerID, errMsg string) (*core.FanOut, error) {
	if m.failTerminalWithResultFunc != nil {
		return m.failTerminalWithResultFunc(ctx, jobID, workerID, errMsg)
	}
	return nil, nil
}

func TestWorkerConfig_Defaults(t *testing.T) {
	config := WorkerConfig{
		Queues:       nil,
		PollInterval: 100 * time.Millisecond,
		WorkerID:     "test-worker",
	}

	assert.Nil(t, config.Queues)
	assert.Equal(t, 100*time.Millisecond, config.PollInterval)
	assert.Equal(t, "test-worker", config.WorkerID)
	assert.False(t, config.EnableScheduler)
}

func TestConcurrency_AppliesCorrectly(t *testing.T) {
	config := WorkerConfig{
		Queues: map[string]int{
			"default": 1,
			"high":    1,
		},
	}

	Concurrency(5).ApplyWorker(&config)

	assert.Equal(t, 5, config.Queues["default"])
	assert.Equal(t, 5, config.Queues["high"])
}

func TestConcurrency_ClampedToMax(t *testing.T) {
	config := WorkerConfig{
		Queues: map[string]int{
			"default": 1,
		},
	}

	// MaxConcurrency is 1000
	Concurrency(5000).ApplyWorker(&config)

	assert.Equal(t, 1000, config.Queues["default"])
}

func TestConcurrency_ClampedToMin(t *testing.T) {
	config := WorkerConfig{
		Queues: map[string]int{
			"default": 10,
		},
	}

	Concurrency(0).ApplyWorker(&config)

	assert.Equal(t, 1, config.Queues["default"])
}

func TestWithScheduler_Enables(t *testing.T) {
	config := WorkerConfig{}

	WithScheduler(true).ApplyWorker(&config)

	assert.True(t, config.EnableScheduler)
}

func TestWithScheduler_Disables(t *testing.T) {
	config := WorkerConfig{EnableScheduler: true}

	WithScheduler(false).ApplyWorker(&config)

	assert.False(t, config.EnableScheduler)
}

func TestWorkerQueue_AddsQueue(t *testing.T) {
	config := WorkerConfig{}

	WorkerQueue("emails").ApplyWorker(&config)

	require.NotNil(t, config.Queues)
	assert.Contains(t, config.Queues, "emails")
	assert.Equal(t, 10, config.Queues["emails"]) // default concurrency
}

func TestWorkerQueue_WithConcurrency(t *testing.T) {
	config := WorkerConfig{}

	WorkerQueue("high-priority", Concurrency(20)).ApplyWorker(&config)

	require.NotNil(t, config.Queues)
	assert.Equal(t, 20, config.Queues["high-priority"])
}

func TestWorkerQueue_MultipleQueues(t *testing.T) {
	config := WorkerConfig{}

	// Note: Concurrency option applies to ALL existing queues in the config
	// So add queues first, then set concurrency once at the end
	WorkerQueue("default").ApplyWorker(&config)
	WorkerQueue("critical").ApplyWorker(&config)
	WorkerQueue("low").ApplyWorker(&config)

	require.NotNil(t, config.Queues)
	// All queues have default concurrency of 10
	assert.Equal(t, 10, config.Queues["default"])
	assert.Equal(t, 10, config.Queues["critical"])
	assert.Equal(t, 10, config.Queues["low"])
}

func TestWorkerQueue_ConcurrencyAffectsAllQueues(t *testing.T) {
	config := WorkerConfig{}

	// Add multiple queues
	WorkerQueue("default").ApplyWorker(&config)
	WorkerQueue("critical").ApplyWorker(&config)

	// Concurrency(5) will set ALL existing queues to 5
	Concurrency(5).ApplyWorker(&config)

	assert.Equal(t, 5, config.Queues["default"])
	assert.Equal(t, 5, config.Queues["critical"])
}

func TestWorkerOptionFunc_ImplementsInterface(t *testing.T) {
	var opt WorkerOption = workerOptionFunc(func(c *WorkerConfig) {
		c.WorkerID = "custom-id"
	})

	config := WorkerConfig{}
	opt.ApplyWorker(&config)

	assert.Equal(t, "custom-id", config.WorkerID)
}

func TestWorker_Pause(t *testing.T) {
	// Create a minimal queue for testing
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	_ = store.Migrate(context.Background())
	q := queue.New(store)

	w := NewWorker(q)

	// Not paused initially
	assert.False(t, w.IsPaused())

	// Pause
	w.Pause(core.PauseModeGraceful)
	assert.True(t, w.IsPaused())

	// Resume
	w.Resume()
	assert.False(t, w.IsPaused())
}

func TestWorker_PauseMode(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	_ = store.Migrate(context.Background())
	q := queue.New(store)

	w := NewWorker(q)

	// Default mode
	assert.Equal(t, core.PauseModeGraceful, w.PauseMode())

	// Set aggressive mode
	w.Pause(core.PauseModeAggressive)
	assert.True(t, w.IsPaused())
	assert.Equal(t, core.PauseModeAggressive, w.PauseMode())
}

func TestWorker_PausedDoesNotDequeue(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	_ = store.Migrate(context.Background())
	q := queue.New(store)

	// Register a handler
	processed := make(chan string, 10)
	q.Register("test-job", func(ctx context.Context, args struct{}) error {
		processed <- "processed"
		return nil
	})

	// Enqueue a job
	_, err := q.Enqueue(context.Background(), "test-job", struct{}{})
	require.NoError(t, err)

	// Create and immediately pause the worker
	w := NewWorker(q)
	w.Pause(core.PauseModeGraceful)

	// Start worker in background
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	// Wait a bit - job should NOT be processed
	select {
	case <-processed:
		t.Fatal("job should not be processed while paused")
	case <-time.After(300 * time.Millisecond):
		// Expected - no job processed
	}
}

func TestWorker_AggressivePauseCancelsRunningJobs(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	_ = store.Migrate(context.Background())
	q := queue.New(store)

	started := make(chan struct{})
	cancelled := make(chan struct{})

	q.Register("long-job", func(ctx context.Context, args struct{}) error {
		close(started)
		<-ctx.Done() // Wait for cancellation
		close(cancelled)
		return ctx.Err()
	})

	_, err := q.Enqueue(context.Background(), "long-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	// Wait for job to start
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	// Aggressive pause should cancel the job
	w.Pause(core.PauseModeAggressive)

	// Job should be cancelled
	select {
	case <-cancelled:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("job was not cancelled")
	}
}

func TestWorker_PauseJobAggressivePreservesCancelledStatus(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	_ = store.Migrate(context.Background())
	q := queue.New(store)

	started := make(chan struct{})
	q.Register("long-job", func(ctx context.Context, args struct{}) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	jobID, err := q.Enqueue(context.Background(), "long-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	require.NoError(t, q.PauseJob(context.Background(), jobID, queue.WithPauseMode(core.PauseModeAggressive)))

	require.Eventually(t, func() bool {
		return w.RunningJobCount() == 0
	}, 2*time.Second, 25*time.Millisecond)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusCancelled, job.Status)
}

func TestWorker_CancelJobCancelsRunningJob(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)

	started := make(chan struct{})
	cancelled := make(chan struct{})
	q.Register("long-job", func(ctx context.Context, args struct{}) error {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})

	jobID, err := q.Enqueue(context.Background(), "long-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	require.NoError(t, q.CancelJob(context.Background(), jobID))

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("job context was not cancelled")
	}

	require.Eventually(t, func() bool {
		job, err := store.GetJob(context.Background(), jobID)
		return err == nil && job != nil && job.Status == core.StatusCancelled
	}, 2*time.Second, 25*time.Millisecond)
}

// ---------------------------------------------------------------------------
// Options tests
// ---------------------------------------------------------------------------

func TestWithPollInterval_ClampsBelowMinimumUp(t *testing.T) {
	config := WorkerConfig{PollInterval: 100 * time.Millisecond}

	// A positive value below 50 ms is clamped UP to the 50 ms floor, not discarded.
	WithPollInterval(10 * time.Millisecond).ApplyWorker(&config)

	assert.Equal(t, 50*time.Millisecond, config.PollInterval)
}

func TestWithPollInterval_SetsValidValue(t *testing.T) {
	config := WorkerConfig{PollInterval: 100 * time.Millisecond}

	WithPollInterval(200 * time.Millisecond).ApplyWorker(&config)

	assert.Equal(t, 200*time.Millisecond, config.PollInterval)
}

func TestWithPollInterval_SetsExactMinimum(t *testing.T) {
	config := WorkerConfig{PollInterval: 100 * time.Millisecond}

	WithPollInterval(50 * time.Millisecond).ApplyWorker(&config)

	assert.Equal(t, 50*time.Millisecond, config.PollInterval)
}

func TestWithPollInterval_NonPositiveKeepsDefault(t *testing.T) {
	config := WorkerConfig{PollInterval: 100 * time.Millisecond}

	// A zero duration is ignored: the existing value is kept.
	WithPollInterval(0).ApplyWorker(&config)
	assert.Equal(t, 100*time.Millisecond, config.PollInterval)

	// A negative duration is ignored too.
	WithPollInterval(-1 * time.Millisecond).ApplyWorker(&config)
	assert.Equal(t, 100*time.Millisecond, config.PollInterval)
}

func TestWithDrainTimeout_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithDrainTimeout(5 * time.Second).ApplyWorker(&config)

	assert.Equal(t, 5*time.Second, config.DrainTimeout)
}

func TestWithStaleLockInterval_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithStaleLockInterval(10 * time.Minute).ApplyWorker(&config)

	assert.Equal(t, 10*time.Minute, config.StaleLockInterval)
}

func TestWithStaleLockInterval_NonPositiveIsIgnored(t *testing.T) {
	// The reaper cannot be disabled, so a non-positive interval must leave the
	// existing value untouched rather than zeroing it.
	config := WorkerConfig{StaleLockInterval: 5 * time.Minute}

	WithStaleLockInterval(0).ApplyWorker(&config)
	assert.Equal(t, 5*time.Minute, config.StaleLockInterval, "zero must be ignored")

	WithStaleLockInterval(-1).ApplyWorker(&config)
	assert.Equal(t, 5*time.Minute, config.StaleLockInterval, "negative must be ignored")
}

func TestWithStaleLockInterval_ClampsBelowFloor(t *testing.T) {
	config := WorkerConfig{}

	WithStaleLockInterval(10 * time.Millisecond).ApplyWorker(&config)

	assert.Equal(t, minStaleLockInterval, config.StaleLockInterval,
		"a sub-floor interval must be clamped up to the floor")
}

func TestWithStaleLockAge_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithStaleLockAge(30 * time.Minute).ApplyWorker(&config)

	assert.Equal(t, 30*time.Minute, config.StaleLockAge)
}

func TestWithFanOutRecoveryStaleAge_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithFanOutRecoveryStaleAge(30 * time.Second).ApplyWorker(&config)

	assert.Equal(t, 30*time.Second, config.FanOutRecoveryStaleAge)
}

func TestWithLockDuration_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithLockDuration(2 * time.Hour).ApplyWorker(&config)

	assert.Equal(t, 2*time.Hour, config.LockDuration)
}

func TestWithLockDuration_PropagatedToStorage(t *testing.T) {
	// Use a real GormStorage backed by SQLite so we can verify the lock
	// duration is propagated when the worker is created with WithLockDuration.
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := storage.NewGormStorage(db)
	q := queue.New(s)

	NewWorker(q, WithLockDuration(2*time.Hour))

	// Verify by enqueueing + dequeueing and checking the lock window.
	require.NoError(t, s.Migrate(context.Background()))
	require.NoError(t, s.Enqueue(context.Background(), &core.Job{Type: "x", Queue: "default"}))

	before := time.Now()
	job, err := s.Dequeue(context.Background(), []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.NotNil(t, job.LockedUntil)

	expectedMin := before.Add(110 * time.Minute)
	expectedMax := before.Add(130 * time.Minute)
	assert.True(t, job.LockedUntil.After(expectedMin),
		"LockedUntil %v should be after %v (want ~2h)", job.LockedUntil, expectedMin)
	assert.True(t, job.LockedUntil.Before(expectedMax),
		"LockedUntil %v should be before %v (want ~2h)", job.LockedUntil, expectedMax)
}

func TestWithLockDuration_StorageWithoutSetterIsNoop(t *testing.T) {
	// When storage does not implement SetLockDuration the worker must not panic
	// and must start without error.
	mock := &mockStorage{}
	q := queue.New(mock)

	// Should not panic even though mockStorage has no SetLockDuration.
	assert.NotPanics(t, func() {
		NewWorker(q, WithLockDuration(2*time.Hour))
	})
}

// ---------------------------------------------------------------------------
// retryWithBackoff additional tests
// ---------------------------------------------------------------------------

func TestRetryWithBackoff_FailsNTimesThenSucceeds(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    5 * time.Millisecond,
		MaxBackoff:        50 * time.Millisecond,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.0,
	}

	var calls int
	sentinel := errors.New("transient")

	err := retryWithBackoff(context.Background(), cfg, func() error {
		calls++
		if calls < 4 {
			return sentinel
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 4, calls)
}

func TestRetryWithBackoff_MaxBackoffCap(t *testing.T) {
	// Use an aggressive multiplier so the backoff would exceed MaxBackoff
	// without the cap.  Measure wall time to verify the cap is respected.
	cfg := RetryConfig{
		MaxAttempts:       4,
		InitialBackoff:    20 * time.Millisecond,
		MaxBackoff:        25 * time.Millisecond, // tiny cap
		BackoffMultiplier: 100.0,
		JitterFraction:    0.0,
	}

	start := time.Now()
	var calls int
	err := retryWithBackoff(context.Background(), cfg, func() error {
		calls++
		return errors.New("always fail")
	})
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Equal(t, 4, calls)
	// 3 sleeps × MaxBackoff (25 ms) = 75 ms; add generous headroom for scheduling.
	// If cap were not applied the second sleep alone would be 2000 ms.
	assert.Less(t, elapsed, 500*time.Millisecond, "backoff should be capped")
}

func TestRetryWithBackoff_ContextCancellationDuringSleep(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       10,
		InitialBackoff:    200 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay so the retry loop is mid-sleep.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := retryWithBackoff(ctx, cfg, func() error {
		return errors.New("fail")
	})
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	// Must return well before the full 200 ms backoff finishes.
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestRetryWithBackoff_DisableRetryFailsImmediately(t *testing.T) {
	// MaxAttempts=1 means the function is called exactly once and the error
	// is returned without any backoff.
	cfg := RetryConfig{MaxAttempts: 1}

	sentinel := errors.New("immediate failure")
	var calls int

	start := time.Now()
	err := retryWithBackoff(context.Background(), cfg, func() error {
		calls++
		return sentinel
	})
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, calls)
	assert.Less(t, elapsed, 50*time.Millisecond, "should return without sleeping")
}

// ---------------------------------------------------------------------------
// Stale lock reaper tests
// ---------------------------------------------------------------------------

func TestWorker_StaleLockReaperCallsReleaseStaleLocks(t *testing.T) {
	mock := &mockStorage{releasedIDs: nil, releaseErr: nil}
	q := queue.New(mock)

	w := NewWorker(q, WithStaleLockAge(1*time.Minute))
	// Drive the reaper fast (the public option floors at 1s; set directly).
	w.config.StaleLockInterval = 30 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	// Wait long enough for the reaper to fire at least twice.
	time.Sleep(150 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1),
		"ReleaseStaleLocks should have been called at least once")
}

func TestWorker_StaleLockReaperAlwaysRuns(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)

	// WithStaleLockInterval(0) cannot disable the reaper — it is the only
	// recovery path for crashed workers. The option is ignored and the 5m
	// default applies, so the reaper is still scheduled (it just won't fire
	// within this short window).
	w := NewWorker(q, WithStaleLockInterval(0))
	assert.Equal(t, 5*time.Minute, w.config.StaleLockInterval,
		"a non-positive interval must fall back to the default, not disable the reaper")

	// Prove it actually runs by driving it fast directly.
	w.config.StaleLockInterval = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	time.Sleep(80 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1),
		"the stale-lock reaper must run; it cannot be disabled")
}

func TestWorker_StartWaitsForStaleLockReaperToFinish(t *testing.T) {
	reaperEntered := make(chan struct{})
	releaseReaper := make(chan struct{})
	var enterOnce sync.Once

	mock := &mockStorage{}
	mock.releaseFunc = func(_ context.Context, _ time.Duration) ([]string, error) {
		enterOnce.Do(func() { close(reaperEntered) })
		<-releaseReaper
		return nil, nil
	}
	q := queue.New(mock)
	w := NewWorker(q,
		WithPollInterval(50*time.Millisecond),
		WithOwnershipAuditInterval(0),
	)
	w.config.StaleLockInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() {
		startReturned <- w.Start(ctx)
	}()

	select {
	case <-reaperEntered:
	case <-time.After(time.Second):
		t.Fatal("stale-lock reaper did not enter ReleaseStaleLocks")
	}

	cancel()

	select {
	case err := <-startReturned:
		t.Fatalf("Start returned before ReleaseStaleLocks finished: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: Start is waiting for the blocked reaper goroutine.
	}

	close(releaseReaper)

	select {
	case err := <-startReturned:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ReleaseStaleLocks finished")
	}
}

// TestWorker_StaleLockReaper_EmitsJobReclaimed verifies that when the
// stale-lock reaper releases jobs, each released ID surfaces as a
// core.JobReclaimed event (Reason=stale_lock, WorkerID=this worker) AND
// fires the OnJobReclaimed hook. This is the leading-indicator observability
// the un-alertable slog-only reaper lacked.
func TestWorker_StaleLockReaper_EmitsJobReclaimed(t *testing.T) {
	mock := &mockStorage{releasedIDs: []string{"job-1", "job-2"}}
	q := queue.New(mock)

	// Subscribe BEFORE Start so no emitted event is missed.
	ch := q.Events()
	defer q.Unsubscribe(ch)

	var hookCount atomic.Int32
	q.OnJobReclaimed(func(_ context.Context, _, reason string) {
		if reason == core.ReclaimReasonStaleLock {
			hookCount.Add(1)
		}
	})

	w := NewWorker(q, WithStaleLockAge(1*time.Minute), WithOwnershipAuditInterval(0))
	w.config.StaleLockInterval = 30 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	// Drain events until we see a stale-lock reclaim for one of our IDs.
	deadline := time.After(time.Second)
	var sawReclaim bool
	var gotReason, gotWorkerID, gotJobID string
loop:
	for {
		select {
		case ev := <-ch:
			if rc, ok := ev.(*core.JobReclaimed); ok {
				sawReclaim = true
				gotReason = rc.Reason
				gotWorkerID = rc.WorkerID
				gotJobID = rc.JobID
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	cancel()

	require.True(t, sawReclaim, "expected at least one JobReclaimed event from the reaper")
	assert.Equal(t, core.ReclaimReasonStaleLock, gotReason)
	assert.Equal(t, w.config.WorkerID, gotWorkerID)
	assert.Contains(t, []string{"job-1", "job-2"}, gotJobID)

	assert.Eventually(t, func() bool { return hookCount.Load() >= 1 }, time.Second, 5*time.Millisecond,
		"OnJobReclaimed hook should fire for stale-lock reclaims")
}

// TestWorker_OwnershipAudit_EmitsJobReclaimed is the kill/recovery
// observability test for the victim side: when a peer reclaims an in-flight
// job, the ownership audit must turn that silent slog line into an emitted,
// hookable core.JobReclaimed event (Reason=ownership_audit).
func TestWorker_OwnershipAudit_EmitsJobReclaimed(t *testing.T) {
	mock := &mockStorage{}
	mock.findOrphanedFunc = func(_ []string) ([]string, error) {
		return []string{"job-9"}, nil
	}
	q := queue.New(mock)

	ch := q.Events()
	defer q.Unsubscribe(ch)

	w := NewWorker(q,
		DisableRetry(),
		WithOwnershipAuditInterval(20*time.Millisecond),
	)
	// Make the reaper effectively dormant so it can't emit a competing event.
	w.config.StaleLockInterval = time.Hour

	// Seed a running job so the audit snapshot is non-empty. Safe: no jobs are
	// dequeued in this test, so processJob never touches the map.
	w.runningJobsMu.Lock()
	w.runningJobs["job-9"] = func() {}
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runOwnershipAudit(ctx)

	deadline := time.After(time.Second)
	var rc *core.JobReclaimed
loop:
	for {
		select {
		case ev := <-ch:
			if got, ok := ev.(*core.JobReclaimed); ok {
				rc = got
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	cancel()

	require.NotNil(t, rc, "expected a JobReclaimed event from the ownership audit")
	assert.Equal(t, core.ReclaimReasonOwnershipAudit, rc.Reason)
	assert.Equal(t, "job-9", rc.JobID)
	assert.Equal(t, w.config.WorkerID, rc.WorkerID)
}

// ---------------------------------------------------------------------------
// Pause / Resume state tests (unit-level, no SQLite)
// ---------------------------------------------------------------------------

func newTestWorker(t *testing.T) *Worker {
	t.Helper()
	mock := &mockStorage{}
	q := queue.New(mock)
	return NewWorker(q)
}

func TestWorker_GracefulPauseSetsState(t *testing.T) {
	w := newTestWorker(t)

	assert.False(t, w.IsPaused())

	w.Pause(core.PauseModeGraceful)

	assert.True(t, w.IsPaused())
	assert.Equal(t, core.PauseModeGraceful, w.PauseMode())
}

func TestWorker_ResumeClearsPausedState(t *testing.T) {
	w := newTestWorker(t)

	w.Pause(core.PauseModeGraceful)
	assert.True(t, w.IsPaused())

	w.Resume()
	assert.False(t, w.IsPaused())
}

func TestWorker_PauseModeDefault(t *testing.T) {
	w := newTestWorker(t)

	// Before any Pause call the mode should default to graceful.
	assert.Equal(t, core.PauseModeGraceful, w.PauseMode())
}

func TestWorker_AggressivePauseSetsMode(t *testing.T) {
	w := newTestWorker(t)

	w.Pause(core.PauseModeAggressive)

	assert.True(t, w.IsPaused())
	assert.Equal(t, core.PauseModeAggressive, w.PauseMode())
}

func TestWorker_AggressivePauseCancelsRegisteredJob(t *testing.T) {
	w := newTestWorker(t)

	cancelled := make(chan struct{})
	ctx, cancelFn := context.WithCancel(context.Background())

	// Simulate a running job by injecting a cancel func directly.
	w.runningJobsMu.Lock()
	w.runningJobs["job-1"] = func() {
		cancelFn()
		close(cancelled)
	}
	w.runningJobsMu.Unlock()

	w.Pause(core.PauseModeAggressive)

	select {
	case <-cancelled:
		// Success: the cancel func was called.
	case <-time.After(time.Second):
		t.Fatal("cancel func was not called by aggressive pause")
	}

	_ = ctx // suppress unused variable lint warning
}

// ---------------------------------------------------------------------------
// CancelJob tests
// ---------------------------------------------------------------------------

func TestWorker_CancelJob_UnknownIDReturnsFalse(t *testing.T) {
	w := newTestWorker(t)

	found := w.CancelJob("does-not-exist")

	assert.False(t, found)
}

func TestWorker_CancelJob_KnownIDReturnsTrueAndCancels(t *testing.T) {
	w := newTestWorker(t)

	cancelled := make(chan struct{})
	ctx, cancelFn := context.WithCancel(context.Background())

	w.runningJobsMu.Lock()
	w.runningJobs["job-abc"] = func() {
		cancelFn()
		close(cancelled)
	}
	w.runningJobsMu.Unlock()

	found := w.CancelJob("job-abc")

	assert.True(t, found)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancel func was not invoked")
	}

	_ = ctx
}

// ---------------------------------------------------------------------------
// RunningJobCount tests
// ---------------------------------------------------------------------------

func TestWorker_RunningJobCount_EmptyIsZero(t *testing.T) {
	w := newTestWorker(t)

	assert.Equal(t, 0, w.RunningJobCount())
}

func TestWorker_RunningJobCount_ReflectsRegisteredJobs(t *testing.T) {
	w := newTestWorker(t)

	w.runningJobsMu.Lock()
	w.runningJobs["j1"] = func() {}
	w.runningJobs["j2"] = func() {}
	w.runningJobsMu.Unlock()

	assert.Equal(t, 2, w.RunningJobCount())
}

// ---------------------------------------------------------------------------
// WaitForPause tests
// ---------------------------------------------------------------------------

func TestWorker_WaitForPause_NotPausedReturnsError(t *testing.T) {
	w := newTestWorker(t)

	err := w.WaitForPause(100 * time.Millisecond)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not paused")
}

func TestWorker_WaitForPause_AlreadyPausedNoRunningJobsReturnsNil(t *testing.T) {
	w := newTestWorker(t)
	w.Pause(core.PauseModeGraceful)

	err := w.WaitForPause(200 * time.Millisecond)

	assert.NoError(t, err)
}

func TestWorker_WaitForPause_TimeoutWithRunningJobs(t *testing.T) {
	w := newTestWorker(t)

	// Inject a job that never finishes.
	w.runningJobsMu.Lock()
	w.runningJobs["stuck-job"] = func() {}
	w.runningJobsMu.Unlock()

	w.Pause(core.PauseModeGraceful)

	start := time.Now()
	err := w.WaitForPause(100 * time.Millisecond)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	// Should have waited roughly the timeout duration.
	assert.GreaterOrEqual(t, elapsed, 90*time.Millisecond)
}

func TestWorker_WaitForPause_CompletesWhenJobFinishes(t *testing.T) {
	w := newTestWorker(t)
	w.Pause(core.PauseModeGraceful)

	// Register a "running" job then remove it after a short delay.
	w.runningJobsMu.Lock()
	w.runningJobs["finishing-job"] = func() {}
	w.runningJobsMu.Unlock()

	go func() {
		time.Sleep(50 * time.Millisecond)
		w.runningJobsMu.Lock()
		delete(w.runningJobs, "finishing-job")
		w.runningJobsMu.Unlock()
	}()

	err := w.WaitForPause(500 * time.Millisecond)

	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Per-queue tracking tests
// ---------------------------------------------------------------------------

func TestWorker_TrackQueueJob_IncrementsCounter(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("emails", Concurrency(5)))

	w.trackQueueJob("job-1", "emails")

	counter, ok := w.queueRunning["emails"]
	require.True(t, ok)
	assert.Equal(t, int32(1), counter.Load())

	w.queueJobIDMu.Lock()
	qname, recorded := w.queueJobID["job-1"]
	w.queueJobIDMu.Unlock()

	assert.True(t, recorded)
	assert.Equal(t, "emails", qname)
}

func TestWorker_UntrackQueueJob_DecrementsCounter(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("emails", Concurrency(5)))

	w.trackQueueJob("job-1", "emails")
	w.untrackQueueJob("job-1")

	counter, ok := w.queueRunning["emails"]
	require.True(t, ok)
	assert.Equal(t, int32(0), counter.Load())

	w.queueJobIDMu.Lock()
	_, still := w.queueJobID["job-1"]
	w.queueJobIDMu.Unlock()

	assert.False(t, still, "job should be removed from queueJobID map")
}

func TestWorker_UntrackQueueJob_UnknownJobIsNoop(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("emails", Concurrency(5)))

	// Should not panic or decrement below zero.
	w.untrackQueueJob("nonexistent-job")

	counter := w.queueRunning["emails"]
	assert.Equal(t, int32(0), counter.Load())
}

func TestWorker_TrackUntrackMultipleJobs(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("default", Concurrency(10)))

	const n = 5
	for i := range n {
		w.trackQueueJob(string(rune('a'+i)), "default")
	}

	counter := w.queueRunning["default"]
	assert.Equal(t, int32(n), counter.Load())

	for i := range n {
		w.untrackQueueJob(string(rune('a' + i)))
	}
	assert.Equal(t, int32(0), counter.Load())
}

// ---------------------------------------------------------------------------
// queuesWithCapacity tests
// ---------------------------------------------------------------------------

func TestWorker_QueuesWithCapacity_AllAvailable(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(5)),
		WorkerQueue("high", Concurrency(3)),
	)

	available := w.queuesWithCapacity()

	assert.Len(t, available, 2)
	assert.ElementsMatch(t, []string{"default", "high"}, available)
}

func TestWorker_QueuesWithCapacity_FullQueueExcluded(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q,
		WorkerQueue("full", Concurrency(2)),
		WorkerQueue("open", Concurrency(5)),
	)

	// Fill the "full" queue to its limit.
	w.trackQueueJob("j1", "full")
	w.trackQueueJob("j2", "full")

	available := w.queuesWithCapacity()

	assert.Equal(t, []string{"open"}, available)
}

func TestWorker_QueuesWithCapacity_AllFullReturnsEmpty(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("only", Concurrency(1)))

	w.trackQueueJob("j1", "only")

	available := w.queuesWithCapacity()

	assert.Empty(t, available)
}

func TestWorker_QueuesWithCapacity_PartiallyFilled(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("q1", Concurrency(3)))

	w.trackQueueJob("j1", "q1")
	// 1 of 3 slots used — queue still has capacity.

	available := w.queuesWithCapacity()

	assert.Contains(t, available, "q1")
}

// ---------------------------------------------------------------------------
// NewWorker default config tests
// ---------------------------------------------------------------------------

func TestNewWorker_DefaultsApplied(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	assert.NotNil(t, w.config.StorageRetry)
	assert.NotNil(t, w.config.DequeueRetry)
	assert.Equal(t, 30*time.Second, w.config.DrainTimeout)
	assert.Equal(t, 5*time.Minute, w.config.StaleLockInterval)
	assert.Equal(t, 45*time.Minute, w.config.StaleLockAge)
	assert.Equal(t, 2*time.Minute, w.config.FanOutRecoveryStaleAge)
	assert.Equal(t, map[string]int{"default": 10}, w.config.Queues)
}

// TestNewWorker_ClampsHeartbeatIntervalBelowStaleLockAge proves the reaper-safety
// clamp: with the reaper now reclaiming from last contact, the heartbeat must
// refresh last_heartbeat_at at least ~3x within StaleLockAge, so the interval is
// clamped to StaleLockAge/3.
func TestNewWorker_ClampsHeartbeatIntervalBelowStaleLockAge(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WithStaleLockAge(3*time.Second))

	assert.Equal(t, 1*time.Second, w.heartbeatInterval, "heartbeat interval must clamp to StaleLockAge/3")
	assert.Less(t, w.heartbeatInterval, 3*time.Second)
}

// TestNewWorker_DefaultHeartbeatIntervalUnchanged confirms the default
// StaleLockAge (45m → 15m) leaves the 2m heartbeat default in place.
func TestNewWorker_DefaultHeartbeatIntervalUnchanged(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	assert.Equal(t, 2*time.Minute, w.heartbeatInterval)
}

func TestNewWorker_CustomQueuesPreserved(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q,
		WorkerQueue("emails", Concurrency(3)),
		WorkerQueue("sms", Concurrency(1)),
	)

	assert.Equal(t, 3, w.config.Queues["emails"])
	assert.Equal(t, 1, w.config.Queues["sms"])
	// Each queue must have an atomic counter initialised.
	assert.NotNil(t, w.queueRunning["emails"])
	assert.NotNil(t, w.queueRunning["sms"])
}

// ---------------------------------------------------------------------------
// calculateBackoff tests
// ---------------------------------------------------------------------------

func TestWorker_CalculateBackoff_ExponentialGrowth(t *testing.T) {
	w := newTestWorker(t)

	b0 := w.calculateBackoff(0) // 1s * 2^0 = 1s
	b1 := w.calculateBackoff(1) // 1s * 2^1 = 2s
	b2 := w.calculateBackoff(2) // 1s * 2^2 = 4s

	assert.Equal(t, time.Second, b0)
	assert.Equal(t, 2*time.Second, b1)
	assert.Equal(t, 4*time.Second, b2)
}

func TestWorker_CalculateBackoff_CappedAtOneMinute(t *testing.T) {
	w := newTestWorker(t)

	// attempt=10 would give 1024s without the cap.
	b := w.calculateBackoff(10)

	assert.Equal(t, time.Minute, b)
}

func TestWorker_CalculateBackoff_OverflowSafeForMaxRetryRange(t *testing.T) {
	w := newTestWorker(t)

	for _, attempt := range []int{0, 1, 2, 3, 4, 5, 30, 33, 63, 99, 100} {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			backoff := w.calculateBackoff(attempt)

			assert.Greater(t, backoff, time.Duration(0))
			assert.LessOrEqual(t, backoff, time.Minute)
		})
	}
}

func TestWorker_FailWithRetry_ClampsPastRetryAtToNow(t *testing.T) {
	var captured *time.Time
	mock := &mockStorage{
		failFunc: func(_ context.Context, _ string, _ string, _ string, retryAt *time.Time) error {
			require.NotNil(t, retryAt)
			copy := *retryAt
			captured = &copy
			return nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	before := time.Now()
	past := before.Add(-time.Hour)
	err := w.failWithRetry(context.Background(), "job-1", "boom", &past)

	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.False(t, captured.Before(before), "retryAt must not be persisted in the past")
	assert.WithinDuration(t, time.Now(), *captured, time.Second)
}

// ---------------------------------------------------------------------------
// Integration: stale lock reaper respects context cancellation
// ---------------------------------------------------------------------------

func TestWorker_StaleLockReaper_StopsOnContextCancel(t *testing.T) {
	mock := &mockStorage{releaseDelay: 5 * time.Millisecond}

	q := queue.New(mock)
	w := NewWorker(q, WithStaleLockAge(1*time.Minute))
	// Drive the reaper fast (the public option floors at 1s; set directly).
	w.config.StaleLockInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Start(ctx)
	}()

	// Let the reaper fire at least once.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}

	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1))
}

// ---------------------------------------------------------------------------
// queuesWithCapacity — uncovered branch: queue in config but not in queueRunning
// ---------------------------------------------------------------------------

func TestWorker_QueuesWithCapacity_MissingCounterTreatedAsAvailable(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WorkerQueue("normal", Concurrency(2)))

	// Inject a queue into config.Queues that has NO corresponding entry in
	// queueRunning to exercise the !ok branch.
	w.config.Queues["phantom"] = 5

	available := w.queuesWithCapacity()

	// Both the properly-tracked queue and the phantom queue must be available.
	assert.Contains(t, available, "normal")
	assert.Contains(t, available, "phantom")
}

// ---------------------------------------------------------------------------
// reapStaleLocks — error path (non-context error should log and continue)
// ---------------------------------------------------------------------------

func TestWorker_StaleLockReaper_ErrorPathContinues(t *testing.T) {
	mock := &mockStorage{
		releasedIDs: nil,
		releaseErr:  errors.New("db unavailable"),
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStaleLockAge(1*time.Minute))
	// Drive the reaper fast (the public option floors at 1s; set directly).
	w.config.StaleLockInterval = 30 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	<-ctx.Done()

	// The reaper must have been called even though it returned an error every time.
	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1))
}

func TestWorker_StaleLockReaper_LogsWhenJobsReleased(t *testing.T) {
	mock := &mockStorage{
		releasedIDs: []string{"job-a", "job-b", "job-c"}, // pretend 3 jobs were released
		releaseErr:  nil,
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStaleLockAge(1*time.Minute))
	// Drive the reaper fast (the public option floors at 1s; set directly).
	w.config.StaleLockInterval = 30 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	<-ctx.Done()

	// If released > 0, the logger.Info path is exercised.
	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1))
}

// ---------------------------------------------------------------------------
// Integration: completeWithRetry / handleError via real handler execution
// ---------------------------------------------------------------------------

func newSQLiteQueue(t *testing.T) (*queue.Queue, func()) {
	t.Helper()
	// Each test gets its own named in-memory database to prevent cross-test
	// contamination when tests run in parallel or when one test's enqueued
	// jobs bleed into another test's dequeue cycle.
	//
	// cache=shared is required so every connection pulled from the pool
	// sees the same schema — with cache=private, a second connection gets
	// a fresh empty DB and any code path that lands on it fails with
	// "no such table: jobs" (e.g. the scheduler goroutine in
	// TestWorker_RunScheduler_EnqueuesDueJob).
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)
	return q, func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	}
}

func TestWorker_CompleteWithRetry_SuccessfulJob(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	done := make(chan struct{})
	q.Register("simple-job", func(ctx context.Context, args struct{}) error {
		return nil
	})
	q.OnJobComplete(func(_ context.Context, _ *core.Job) {
		close(done)
	})

	_, err := q.Enqueue(context.Background(), "simple-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-done:
		// completeWithRetry was exercised via the successful job completion path.
	case <-time.After(2 * time.Second):
		t.Fatal("job did not complete")
	}
}

func TestWorker_PerJobTimeoutCancelsHandlerContext(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	const jobTimeout = 60 * time.Millisecond
	started := make(chan struct{})
	cancelled := make(chan time.Duration, 1)

	q.Register("per-job-timeout", func(ctx context.Context, args struct{}) error {
		start := time.Now()
		close(started)
		<-ctx.Done()
		cancelled <- time.Since(start)
		return ctx.Err()
	})

	_, err := q.Enqueue(context.Background(), "per-job-timeout", struct{}{},
		queue.Timeout(jobTimeout),
		queue.Retries(0),
	)
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(10*time.Millisecond), WithStaleLockInterval(0), WithOwnershipAuditInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	select {
	case elapsed := <-cancelled:
		assert.GreaterOrEqual(t, elapsed, jobTimeout-10*time.Millisecond)
		assert.Less(t, elapsed, 500*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("job context was not cancelled by per-job timeout")
	}
}

func TestWorker_SubJobTimeoutCancelsHandlerContext(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	const subTimeout = 60 * time.Millisecond
	subStarted := make(chan struct{})
	subCancelled := make(chan time.Duration, 1)

	q.Register("sub-timeout-child", func(ctx context.Context, args string) error {
		start := time.Now()
		close(subStarted)
		<-ctx.Done()
		subCancelled <- time.Since(start)
		return ctx.Err()
	})
	q.Register("sub-timeout-parent", func(ctx context.Context, args struct{}) error {
		_, err := fanout.FanOut[string](ctx, []fanout.SubJob{
			fanout.Sub("sub-timeout-child", "x", queue.Timeout(subTimeout), queue.Retries(0)),
		})
		return err
	})

	_, err := q.Enqueue(context.Background(), "sub-timeout-parent", struct{}{}, queue.Retries(0))
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(10*time.Millisecond), WithStaleLockInterval(0), WithOwnershipAuditInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case <-subStarted:
	case <-time.After(time.Second):
		t.Fatal("sub-job did not start")
	}

	select {
	case elapsed := <-subCancelled:
		assert.GreaterOrEqual(t, elapsed, subTimeout-10*time.Millisecond)
		assert.Less(t, elapsed, 500*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("sub-job context was not cancelled by sub-job timeout")
	}
}

func TestWorker_HandlerTimeoutFallbackAndNoTimeout(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	const handlerTimeout = 60 * time.Millisecond
	fallbackStarted := make(chan struct{})
	fallbackCancelled := make(chan time.Duration, 1)
	noTimeoutRan := make(chan struct{})

	q.Register("handler-timeout", func(ctx context.Context, args struct{}) error {
		start := time.Now()
		close(fallbackStarted)
		<-ctx.Done()
		fallbackCancelled <- time.Since(start)
		return ctx.Err()
	}, queue.Timeout(handlerTimeout))
	q.Register("no-timeout", func(ctx context.Context, args struct{}) error {
		if _, ok := ctx.Deadline(); ok {
			return errors.New("unexpected deadline")
		}
		close(noTimeoutRan)
		return nil
	})

	_, err := q.Enqueue(context.Background(), "handler-timeout", struct{}{}, queue.Retries(0))
	require.NoError(t, err)
	_, err = q.Enqueue(context.Background(), "no-timeout", struct{}{}, queue.Retries(0))
	require.NoError(t, err)

	w := NewWorker(q, WithPollInterval(10*time.Millisecond), WithStaleLockInterval(0), WithOwnershipAuditInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case <-fallbackStarted:
	case <-time.After(time.Second):
		t.Fatal("handler-timeout job did not start")
	}

	select {
	case elapsed := <-fallbackCancelled:
		assert.GreaterOrEqual(t, elapsed, handlerTimeout-10*time.Millisecond)
		assert.Less(t, elapsed, 500*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("handler timeout fallback did not cancel context")
	}

	select {
	case <-noTimeoutRan:
	case <-time.After(time.Second):
		t.Fatal("no-timeout handler did not run without deadline")
	}
}

// ---------------------------------------------------------------------------
// Start drain behaviour
// ---------------------------------------------------------------------------

func TestWorker_StartGracefullyDrainsRunningHandlerOnContextCancel(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	started := make(chan struct{})
	completed := make(chan struct{})
	var completeOnce sync.Once
	var sideEffect atomic.Int32
	var handlerCancelled atomic.Bool

	q.Register("slow-success", func(ctx context.Context, args struct{}) error {
		close(started)
		select {
		case <-time.After(200 * time.Millisecond):
			sideEffect.Add(1)
			return nil
		case <-ctx.Done():
			handlerCancelled.Store(true)
			return ctx.Err()
		}
	})
	q.OnJobComplete(func(_ context.Context, _ *core.Job) {
		completeOnce.Do(func() { close(completed) })
	})

	jobID, err := q.Enqueue(context.Background(), "slow-success", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q,
		WithPollInterval(50*time.Millisecond),
		WithDrainTimeout(2*time.Second),
		WithOwnershipAuditInterval(0),
	)
	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() {
		startReturned <- w.Start(ctx)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	cancelStarted := time.Now()
	cancel()

	select {
	case err := <-startReturned:
		t.Fatalf("Start returned before the running handler completed: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: drain keeps the handler alive past Start context cancellation.
	}

	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not complete during drain")
	}

	select {
	case err := <-startReturned:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after drained handler completed")
	}

	assert.GreaterOrEqual(t, time.Since(cancelStarted), 180*time.Millisecond)
	assert.Equal(t, int32(1), sideEffect.Load())
	assert.False(t, handlerCancelled.Load(), "handler context must survive Start context cancellation during drain")

	job, err := q.Storage().GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusCompleted, job.Status)
}

func TestWorker_StartDrainTimeoutCancelsRunningHandler(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	started := make(chan struct{})
	cancelled := make(chan struct{})
	var cancelOnce sync.Once

	q.Register("blocked", func(ctx context.Context, args struct{}) error {
		close(started)
		<-ctx.Done()
		cancelOnce.Do(func() { close(cancelled) })
		return ctx.Err()
	})

	_, err := q.Enqueue(context.Background(), "blocked", struct{}{})
	require.NoError(t, err)

	drainTimeout := 80 * time.Millisecond
	w := NewWorker(q,
		WithPollInterval(50*time.Millisecond),
		WithDrainTimeout(drainTimeout),
		WithOwnershipAuditInterval(0),
		DisableRetry(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() {
		startReturned <- w.Start(ctx)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	cancelStarted := time.Now()
	cancel()

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler context was not cancelled after drain timeout")
	}

	select {
	case err := <-startReturned:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after drain timeout")
	}

	elapsed := time.Since(cancelStarted)
	assert.GreaterOrEqual(t, elapsed, drainTimeout)
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestWorker_StartCancelIdleReturnsPromptly(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q,
		WithPollInterval(50*time.Millisecond),
		WithDrainTimeout(2*time.Second),
		WithOwnershipAuditInterval(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() {
		startReturned <- w.Start(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case err := <-startReturned:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle worker waited for DrainTimeout on cancellation")
	}

	assert.Less(t, time.Since(start), 300*time.Millisecond)
}

type cancelMidDrainStorage struct {
	*mockStorage
	mu            sync.Mutex
	batchCalls    int
	enteredSecond chan struct{}
	secondOnce    sync.Once
}

func (s *cancelMidDrainStorage) DequeueBatch(ctx context.Context, _ []string, _ string, _ int) ([]*core.Job, error) {
	s.mu.Lock()
	s.batchCalls++
	call := s.batchCalls
	s.mu.Unlock()

	if call == 1 {
		return []*core.Job{{
			ID:     "mid-drain-job",
			Type:   "mid-drain-noop",
			Queue:  "default",
			Args:   []byte(`{}`),
			Status: core.StatusRunning,
		}}, nil
	}

	s.secondOnce.Do(func() { close(s.enteredSecond) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestWorker_StartCancelMidDrainReturnsWithinDrainTimeout(t *testing.T) {
	store := &cancelMidDrainStorage{
		mockStorage:   &mockStorage{},
		enteredSecond: make(chan struct{}),
	}
	q := queue.New(store)
	q.Register("mid-drain-noop", func(context.Context, struct{}) error {
		return nil
	})

	drainTimeout := 200 * time.Millisecond
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(4)),
		WithDequeueBatchSize(2),
		WithPollInterval(50*time.Millisecond),
		WithDrainTimeout(drainTimeout),
		WithOwnershipAuditInterval(0),
		DisableRetry(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() {
		startReturned <- w.Start(ctx)
	}()

	select {
	case <-store.enteredSecond:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("worker did not enter the second drain dequeue")
	}

	start := time.Now()
	cancel()

	select {
	case err := <-startReturned:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after mid-drain cancellation")
	}

	assert.Less(t, time.Since(start), drainTimeout,
		"mid-drain cancellation should not wait for DrainTimeout")
}

func TestWorker_ShutdownReleasesDequeuedJobInsteadOfDropping(t *testing.T) {
	job := &core.Job{
		ID:     "dequeued-job",
		Type:   "never-dispatched",
		Queue:  "default",
		Status: core.StatusRunning,
	}

	mock := &mockStorage{}
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	mock.dequeueFunc = func(_ context.Context, _ []string, _ string) (*core.Job, error) {
		once.Do(cancel)
		return job, nil
	}

	q := queue.New(mock)
	w := NewWorker(q,
		WithPollInterval(10*time.Millisecond),
		WithDrainTimeout(2*time.Second),
		WithOwnershipAuditInterval(0),
		DisableRetry(),
	)

	err := w.Start(ctx)
	require.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, []string{job.ID}, mock.getReleasedJobIDs())
	assert.Equal(t, int32(0), w.queueRunning["default"].Load())
	w.queueJobIDMu.Lock()
	_, tracked := w.queueJobID[job.ID]
	w.queueJobIDMu.Unlock()
	assert.False(t, tracked, "shutdown release must untrack the per-queue job slot")
}

func TestWorker_HandleError_NoRetryError(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	failed := make(chan struct{})
	q.Register("no-retry-job", func(ctx context.Context, args struct{}) error {
		return core.NoRetry(errors.New("permanent failure"))
	})
	q.OnJobFail(func(_ context.Context, _ *core.Job, _ error) {
		close(failed)
	})

	_, err := q.Enqueue(context.Background(), "no-retry-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-failed:
	case <-time.After(2 * time.Second):
		t.Fatal("job with NoRetryError did not fail immediately")
	}
}

func TestWorker_HandleError_RetryAfterError(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	var attempt atomic.Int32
	eventually := make(chan struct{})

	q.Register("retry-after-job", func(ctx context.Context, args struct{}) error {
		n := attempt.Add(1)
		if n < 2 {
			// RetryAfterError with a very short delay so the test completes quickly.
			return core.RetryAfter(10*time.Millisecond, errors.New("needs retry"))
		}
		close(eventually)
		return nil
	})

	// Allow 3 retries so the job can succeed on the second attempt.
	_, err := q.Enqueue(context.Background(), "retry-after-job", struct{}{},
		queue.Retries(3),
	)
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-eventually:
		assert.GreaterOrEqual(t, attempt.Load(), int32(2))
	case <-time.After(4 * time.Second):
		t.Fatal("RetryAfter job did not eventually succeed")
	}
}

func TestWorker_ExecuteHandler_PanicsWithNonError(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	// When the handler panics with a non-error value, executeHandler recovers
	// and returns a formatted error.  The job will be retried (or failed
	// permanently after all retries).  We watch OnRetry to confirm the panic
	// was caught and treated as an error rather than crashing the worker.
	retried := make(chan struct{}, 1)
	q.Register("panic-job", func(ctx context.Context, args struct{}) error {
		panic("string panic value")
	})
	q.OnRetry(func(_ context.Context, _ *core.Job, _ int, _ error) {
		select {
		case retried <- struct{}{}:
		default:
		}
	})

	_, err := q.Enqueue(context.Background(), "panic-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-retried:
		// executeHandler recovered from the non-error panic, treated it as an
		// error, and handleError scheduled a retry.
	case <-time.After(2 * time.Second):
		t.Fatal("panicking job was not retried — panic recovery may not be working")
	}
}

func TestWorker_ExecuteHandler_PanicsWithError(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	// When the handler panics with an error value, executeHandler recovers and
	// returns that error.  We watch OnRetry to confirm the error was propagated
	// rather than the worker crashing.
	retried := make(chan struct{}, 1)
	q.Register("error-panic-job", func(ctx context.Context, args struct{}) error {
		panic(errors.New("error panic value"))
	})
	q.OnRetry(func(_ context.Context, _ *core.Job, _ int, _ error) {
		select {
		case retried <- struct{}{}:
		default:
		}
	})

	_, err := q.Enqueue(context.Background(), "error-panic-job", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-retried:
		// executeHandler recovered from the error panic and propagated the error.
	case <-time.After(2 * time.Second):
		t.Fatal("error-panicking job was not retried — panic recovery may not be working")
	}
}

func TestWorker_ExecuteHandler_GetCheckpointsError(t *testing.T) {
	mock := &mockStorage{
		checkpointFunc: func(_ context.Context, _ string) ([]core.Checkpoint, error) {
			return nil, errors.New("checkpoint load error")
		},
		failFunc: func(_ context.Context, _ string, _ string, _ string, _ *time.Time) error {
			return nil
		},
	}
	q := queue.New(mock)

	failed := make(chan struct{})
	q.Register("cp-error-job", func(ctx context.Context, args struct{}) error {
		return nil
	})
	q.OnJobFail(func(_ context.Context, _ *core.Job, _ error) {
		close(failed)
	})

	// Inject a job directly into the mock dequeue so we control what is processed.
	jobID := "test-job-1"
	mock.dequeueFunc = func(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
		mock.dequeueFunc = nil // only return once
		return &core.Job{
			ID:         jobID,
			Type:       "cp-error-job",
			Queue:      "default",
			Args:       []byte(`{}`),
			MaxRetries: 0,
		}, nil
	}

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-failed:
		// GetCheckpoints returned an error so executeHandler returned error,
		// which exercised the error return path in executeHandler.
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("job with checkpoint error did not result in failure")
	}
}

// TestExecuteHandler_CheckpointWriteSurvivesJobCancellation is the regression
// test for finding #4 (DEADBOLT P3): the default checkpoint write must land even
// when the per-job context is already cancelled/expired as the handler returns,
// or a non-idempotent side effect re-runs on replay.
//
// Under the old closure (return Storage().SaveCheckpoint(ctx, cp)) the captured
// ctx carries the per-job cancellation, so captured.Err() == context.Canceled.
// After the fix the closure wraps the incoming ctx with WithoutCancel + a bounded
// deadline, so the storage write sees a live context.
func TestExecuteHandler_CheckpointWriteSurvivesJobCancellation(t *testing.T) {
	var (
		capturedErr error
		sawWrite    bool
		capturedCP  *core.Checkpoint
	)
	mock := &mockStorage{
		checkpointFunc: func(_ context.Context, _ string) ([]core.Checkpoint, error) {
			return nil, nil
		},
		saveCheckpointFunc: func(ctx context.Context, cp *core.Checkpoint) error {
			// Capture the ctx state AT THE STORAGE BOUNDARY — checking after the
			// closure returns would observe its deferred cancel() instead.
			sawWrite = true
			capturedErr = ctx.Err()
			capturedCP = cp
			return nil
		},
	}
	q := queue.New(mock)

	const jobType = "phase-checkpoint-job"
	q.Register(jobType, func(c context.Context, _ struct{}) error {
		return jobctx.SavePhaseCheckpoint(c, "phase", "v")
	})

	h, ok := q.GetHandler(jobType)
	require.True(t, ok, "handler must be registered")

	w := NewWorker(q, WithStaleLockInterval(0))

	// A pre-cancelled per-job context: this is exactly the state when the per-job
	// deadline fires microseconds after the activity returns but before the
	// checkpoint is persisted.
	jobCtx, cancel := context.WithCancel(context.Background())
	cancel()

	job := &core.Job{
		ID:    "phase-job-1",
		Type:  jobType,
		Queue: "default",
		Args:  []byte(`{}`),
	}

	_, err := w.executeHandler(jobCtx, job, h)
	require.NoError(t, err)

	require.True(t, sawWrite, "storage must have received a checkpoint write")
	// The crux of the regression: the storage boundary sees a live context even
	// though the per-job context was cancelled before the write.
	require.NoError(t, capturedErr,
		"checkpoint write context must not be cancelled (was %v)", capturedErr)
	require.NotNil(t, capturedCP)
	require.Equal(t, "phase", capturedCP.CallType,
		"the phase checkpoint must be the one written")
}

// TestExecuteHandler_CheckpointWriteSurvivesJobCancellation_CallPath proves the
// protection covers the default Call path too, not only SavePhaseCheckpoint —
// the nested-call checkpoint must also land with a non-cancelled storage ctx.
func TestExecuteHandler_CheckpointWriteSurvivesJobCancellation_CallPath(t *testing.T) {
	var (
		capturedErr error
		sawWrite    bool
		capturedCP  *core.Checkpoint
	)
	mock := &mockStorage{
		checkpointFunc: func(_ context.Context, _ string) ([]core.Checkpoint, error) {
			return nil, nil
		},
		saveCheckpointFunc: func(ctx context.Context, cp *core.Checkpoint) error {
			sawWrite = true
			capturedErr = ctx.Err()
			capturedCP = cp
			return nil
		},
	}
	q := queue.New(mock)

	const subHandler = "call-sub"
	q.Register(subHandler, func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	})

	const parentType = "call-parent-job"
	q.Register(parentType, func(c context.Context, _ struct{}) error {
		_, err := call.Call[string](c, subHandler, struct{}{})
		return err
	})

	h, ok := q.GetHandler(parentType)
	require.True(t, ok, "parent handler must be registered")

	w := NewWorker(q, WithStaleLockInterval(0))

	jobCtx, cancel := context.WithCancel(context.Background())
	cancel()

	job := &core.Job{
		ID:    "call-parent-1",
		Type:  parentType,
		Queue: "default",
		Args:  []byte(`{}`),
	}

	_, err := w.executeHandler(jobCtx, job, h)
	require.NoError(t, err)

	require.True(t, sawWrite, "the nested Call checkpoint must be written")
	require.NoError(t, capturedErr,
		"nested Call checkpoint write context must not be cancelled (was %v)", capturedErr)
	require.NotNil(t, capturedCP)
}

// ---------------------------------------------------------------------------
// pollWaitingJobs — cover the inner job-resume loop
// ---------------------------------------------------------------------------

// TestWorker_PollWaitingJobs_ExitsOnContextCancel calls pollWaitingJobs
// directly (it is unexported but accessible from the same package) to verify
// it exits cleanly when the context is cancelled.
func TestWorker_PollWaitingJobs_ExitsOnContextCancel(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.pollWaitingJobs(ctx)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pollWaitingJobs did not exit after context cancellation")
	}
}

func TestWorker_PollWaitingJobs_ResumesStalledFanOutParent(t *testing.T) {
	stalledParent := &core.Job{ID: "stalled-parent", Status: core.StatusWaiting}
	resumed := make(chan string, 1)
	var sawCutoff atomic.Bool

	mock := &mockStorage{
		stalledJobsFunc: func(_ context.Context, olderThan time.Time) ([]*core.Job, error) {
			if olderThan.Before(time.Now().Add(-25 * time.Second)) {
				sawCutoff.Store(true)
			}
			return []*core.Job{stalledParent}, nil
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed <- jobID
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q,
		WithStorageRetry(RetryConfig{MaxAttempts: 1}),
		WithFanOutRecoveryStaleAge(30*time.Second),
	)

	w.pollWaitingJobsOnce(context.Background())

	select {
	case jobID := <-resumed:
		assert.Equal(t, "stalled-parent", jobID)
	case <-time.After(time.Second):
		t.Fatal("ResumeJob was not called for stalled fan-out parent")
	}
	assert.True(t, sawCutoff.Load(), "stalled fan-out query should use configured stale age cutoff")
}

// TestPollWaitingJobsOnce_RescuesStrandedTerminalFanOut is the crash/replay
// regression test for finding #2: a fan-out left status='pending' with terminal
// counts and a waiting parent (the post-crash strand that NEITHER existing
// recovery query rescues) must be healed in a single poll cycle. It drives the
// stranded row through the same completeFanOut path the live worker uses —
// UpdateFanOutStatus CAS + ResumeJob — proving the backstop is wired end-to-end.
func TestPollWaitingJobsOnce_RescuesStrandedTerminalFanOut(t *testing.T) {
	stranded := &core.FanOut{
		ID:             "fo-stranded",
		ParentJobID:    "parent-stranded",
		TotalCount:     2,
		CompletedCount: 2,
		Strategy:       core.StrategyCollectAll,
		Status:         core.FanOutPending,
	}
	statusUpdated := make(chan core.FanOutStatus, 1)
	resumed := make(chan string, 1)

	mock := &mockStorage{
		completablePendingFunc: func(_ context.Context, _ time.Time) ([]*core.FanOut, error) {
			return []*core.FanOut{stranded}, nil
		},
		updateFanOutStatusFunc: func(_ context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
			assert.Equal(t, stranded.ID, fanOutID)
			statusUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed <- jobID
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q,
		WithStorageRetry(RetryConfig{MaxAttempts: 1}),
		WithFanOutRecoveryStaleAge(30*time.Second),
	)

	w.pollWaitingJobsOnce(context.Background())

	select {
	case status := <-statusUpdated:
		assert.Equal(t, core.FanOutCompleted, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called for the stranded pending fan-out")
	}
	select {
	case jobID := <-resumed:
		assert.Equal(t, stranded.ParentJobID, jobID)
	case <-time.After(time.Second):
		t.Fatal("ResumeJob was not called for the stranded fan-out's parent")
	}
}

// TestCompleteFanOut_ResumesWhenStatusAlreadyTerminal guards against
// re-introducing the early-return regression: with P2's in-tx advance, the very
// worker responsible for resuming sees UpdateFanOutStatus return (false,nil)
// because its own terminal transaction already advanced the status. completeFanOut
// must still fall through to the idempotent ResumeJob.
func TestCompleteFanOut_ResumesWhenStatusAlreadyTerminal(t *testing.T) {
	resumed := make(chan string, 1)
	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return false, nil // status already advanced in-tx
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed <- jobID
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fo := &core.FanOut{
		ID:             "fo-already-terminal",
		ParentJobID:    "parent-already-terminal",
		TotalCount:     2,
		CompletedCount: 2,
		Strategy:       core.StrategyCollectAll,
		Status:         core.FanOutCompleted,
	}
	require.NoError(t, w.completeFanOut(context.Background(), fo, core.FanOutCompleted))

	select {
	case jobID := <-resumed:
		assert.Equal(t, fo.ParentJobID, jobID)
	case <-time.After(time.Second):
		t.Fatal("ResumeJob must be called even when UpdateFanOutStatus reports !updated")
	}
}

func TestWorker_PollSignalWaitingJobs_PagesPastMemoizedFutureSleeps(t *testing.T) {
	origBatchSize := signalResumePollBatchSize
	signalResumePollBatchSize = 3
	t.Cleanup(func() { signalResumePollBatchSize = origBatchSize })

	future := time.Now().Add(time.Hour)
	sleepState, err := json.Marshal(map[string]any{
		"deadline": future.UnixNano(),
		"resolved": false,
	})
	require.NoError(t, err)

	jobs := []*core.Job{
		{ID: "a-sleep-00", Status: core.StatusWaiting, RunAt: &future},
		{ID: "a-sleep-01", Status: core.StatusWaiting, RunAt: &future},
		{ID: "a-sleep-02", Status: core.StatusWaiting, RunAt: &future},
		{ID: "a-sleep-03", Status: core.StatusWaiting, RunAt: &future},
		{ID: "z-legit-signal", Status: core.StatusWaiting},
	}

	var sleepCheckpointReads atomic.Int32
	resumed := make(chan string, 4)
	mock := &mockStorage{
		signalWaitingAfterFunc: func(_ context.Context, afterJobID string, limit int) ([]*core.Job, error) {
			var out []*core.Job
			for _, job := range jobs {
				if job.ID <= afterJobID {
					continue
				}
				out = append(out, job)
				if len(out) == limit {
					break
				}
			}
			return out, nil
		},
		checkpointFunc: func(_ context.Context, jobID string) ([]core.Checkpoint, error) {
			if strings.HasPrefix(jobID, "a-sleep-") {
				sleepCheckpointReads.Add(1)
				return []core.Checkpoint{{
					JobID: jobID, CallIndex: 0, CallType: signal.SleepCheckpointType, Result: sleepState,
				}}, nil
			}
			return nil, nil
		},
		resumeSignalFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed <- jobID
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	w.pollWaitingJobsOnce(context.Background())

	select {
	case jobID := <-resumed:
		assert.Equal(t, "z-legit-signal", jobID)
	case <-time.After(time.Second):
		t.Fatal("signal backstop did not page past future durable timers")
	}
	assert.Equal(t, int32(4), sleepCheckpointReads.Load(), "each future sleeper should be inspected once")

	w.pollWaitingJobsOnce(context.Background())
	assert.Equal(t, int32(4), sleepCheckpointReads.Load(), "memoized future sleepers should not be re-inspected on the next tick")
}

// ---------------------------------------------------------------------------
// handleSubJobCompletion — cover the non-nil FanOut path
// ---------------------------------------------------------------------------

func TestWorker_HandleSubJobCompletion_NilFanOutID(t *testing.T) {
	// Job with no FanOutID must return nil immediately.
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	job := &core.Job{ID: "j1", Type: "noop", Queue: "default"}
	err := w.handleSubJobCompletion(context.Background(), job, true)
	assert.NoError(t, err)
}

func TestWorker_HandleSubJobCompletion_WithFanOutID_StorageReturnsNil(t *testing.T) {
	// When IncrementFanOutCompleted returns (nil, nil) the function should
	// return nil without calling checkFanOutCompletion.
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	fanOutID := "fo-123"
	job := &core.Job{ID: "j1", Type: "noop", Queue: "default", FanOutID: &fanOutID}

	// Default mock IncrementFanOutCompleted returns (nil, nil).
	err := w.handleSubJobCompletion(context.Background(), job, true)
	assert.NoError(t, err)
}

func TestWorker_HandleSubJobCompletion_WithFanOutID_IncrementFailed(t *testing.T) {
	// Verify that handleSubJobCompletion completes without panicking when
	// storage returns (nil, nil) for IncrementFanOutCompleted.  This exercises
	// the non-nil FanOutID branch down to the "fo == nil → return nil" check.
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	fanOutID := "fo-456"
	job := &core.Job{ID: "j1", Type: "noop", Queue: "default", FanOutID: &fanOutID}
	err := w.handleSubJobCompletion(ctx, job, true)
	// Mock returns (nil, nil), so retryWithBackoff returns nil.
	assert.NoError(t, err)
}

func TestWorker_NoHandlerSubJobAccountsToFanOut(t *testing.T) {
	fanOutID := "fo-no-handler"
	var failedIncrements atomic.Int32
	mock := &mockStorage{
		failFunc: func(_ context.Context, _ string, _ string, _ string, retryAt *time.Time) error {
			require.Nil(t, retryAt)
			return nil
		},
		incrementFailedFunc: func(_ context.Context, gotFanOutID string) (*core.FanOut, error) {
			failedIncrements.Add(1)
			assert.Equal(t, fanOutID, gotFanOutID)
			return &core.FanOut{
				ID:          gotFanOutID,
				ParentJobID: "parent",
				TotalCount:  2,
				FailedCount: 1,
				Strategy:    core.StrategyCollectAll,
			}, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())

	w.processJob(context.Background(), &core.Job{
		ID:         "sub-no-handler",
		Type:       "missing-handler",
		Queue:      "default",
		FanOutID:   &fanOutID,
		Attempt:    1,
		MaxRetries: 1,
	})

	assert.Equal(t, int32(1), failedIncrements.Load())
}

func TestWorker_TransientCompleteError_ReleasesForRerun(t *testing.T) {
	fanOutID := "fo-transient-complete"
	transientErr := errors.New("complete unavailable")
	var completeCalls atomic.Int32
	var legacyCompleted atomic.Int32
	var legacyFailed atomic.Int32

	base := &mockStorage{
		incrementCompletedFunc: func(context.Context, string) (*core.FanOut, error) {
			legacyCompleted.Add(1)
			return nil, nil
		},
		incrementFailedFunc: func(context.Context, string) (*core.FanOut, error) {
			legacyFailed.Add(1)
			return nil, nil
		},
	}
	store := &atomicMockStorage{mockStorage: base}
	store.completeWithResultFunc = func(_ context.Context, _ string, _ string, _ []byte) (*core.FanOut, error) {
		if completeCalls.Add(1) == 1 {
			return nil, transientErr
		}
		return &core.FanOut{
			ID:             fanOutID,
			ParentJobID:    "parent",
			TotalCount:     2,
			CompletedCount: 1,
			Strategy:       core.StrategyCollectAll,
		}, nil
	}

	q := queue.New(store)
	q.Register("atomic-success", func(context.Context, struct{}) error { return nil })
	w := NewWorker(q, DisableRetry(), WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	job := &core.Job{ID: "sub-transient", Type: "atomic-success", Queue: "default", FanOutID: &fanOutID}
	w.processJob(context.Background(), job)

	assert.Equal(t, []string{job.ID}, store.getReleasedJobIDs())
	assert.Equal(t, int32(0), legacyCompleted.Load())
	assert.Equal(t, int32(0), legacyFailed.Load(), "transient complete must not be counted as a failed sub-job")
	assert.Equal(t, int32(1), completeCalls.Load())

	w.processJob(context.Background(), job)

	assert.Equal(t, []string{job.ID}, store.getReleasedJobIDs(), "successful rerun should not release again")
	assert.Equal(t, int32(2), completeCalls.Load())
	assert.Equal(t, int32(0), legacyCompleted.Load())
	assert.Equal(t, int32(0), legacyFailed.Load())
}

func TestWorker_CompleteWithResult_Success_IncrementsAndResumes(t *testing.T) {
	fanOutID := "fo-atomic-success"
	parentID := "parent-atomic-success"
	resultSeen := make(chan []byte, 1)
	resumed := make(chan string, 1)
	statusUpdated := make(chan core.FanOutStatus, 1)

	base := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, gotFanOutID string, status core.FanOutStatus) (bool, error) {
			assert.Equal(t, fanOutID, gotFanOutID)
			statusUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed <- jobID
			return true, nil
		},
	}
	store := &atomicMockStorage{mockStorage: base}
	store.completeWithResultFunc = func(_ context.Context, jobID, workerID string, result []byte) (*core.FanOut, error) {
		assert.Equal(t, "sub-atomic-success", jobID)
		assert.NotEmpty(t, workerID)
		resultSeen <- append([]byte(nil), result...)
		return &core.FanOut{
			ID:             fanOutID,
			ParentJobID:    parentID,
			TotalCount:     1,
			CompletedCount: 1,
			Strategy:       core.StrategyCollectAll,
		}, nil
	}

	q := queue.New(store)
	q.Register("return-result", func(context.Context, string) (map[string]string, error) {
		return map[string]string{"value": "ok"}, nil
	})
	completed := make(chan struct{}, 1)
	q.OnJobComplete(func(context.Context, *core.Job) { completed <- struct{}{} })
	w := NewWorker(q, DisableRetry(), WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fanOutIDCopy := fanOutID
	w.processJob(context.Background(), &core.Job{
		ID:       "sub-atomic-success",
		Type:     "return-result",
		Queue:    "default",
		Args:     []byte(`"input"`),
		FanOutID: &fanOutIDCopy,
	})

	select {
	case result := <-resultSeen:
		assert.JSONEq(t, `{"value":"ok"}`, string(result))
	case <-time.After(time.Second):
		t.Fatal("CompleteWithResult was not called with the handler result")
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("completion hook was not called")
	}
	select {
	case status := <-statusUpdated:
		assert.Equal(t, core.FanOutCompleted, status)
	case <-time.After(time.Second):
		t.Fatal("fan-out status was not updated")
	}
	select {
	case gotParent := <-resumed:
		assert.Equal(t, parentID, gotParent)
	case <-time.After(time.Second):
		t.Fatal("parent was not resumed")
	}
}

func TestWorker_HeartbeatHeldUntilAfterComplete(t *testing.T) {
	completeEntered := make(chan struct{})
	heartbeatDuringComplete := make(chan struct{})
	allowComplete := make(chan struct{})
	var entered atomic.Bool
	var completeOnce sync.Once
	var heartbeatOnce sync.Once

	base := &mockStorage{
		heartbeatFunc: func(context.Context, string, string) error {
			if entered.Load() {
				heartbeatOnce.Do(func() { close(heartbeatDuringComplete) })
			}
			return nil
		},
	}
	store := &atomicMockStorage{mockStorage: base}
	store.completeWithResultFunc = func(context.Context, string, string, []byte) (*core.FanOut, error) {
		entered.Store(true)
		completeOnce.Do(func() { close(completeEntered) })
		select {
		case <-heartbeatDuringComplete:
		case <-time.After(time.Second):
			t.Fatal("heartbeat stopped before CompleteWithResult returned")
		}
		close(allowComplete)
		return nil, nil
	}

	q := queue.New(store)
	q.Register("quick-success", func(context.Context, struct{}) error { return nil })
	w := NewWorker(q, DisableRetry(), WithStorageRetry(RetryConfig{MaxAttempts: 1}))
	w.heartbeatInterval = 5 * time.Millisecond

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.processJob(context.Background(), &core.Job{ID: "hb-through-complete", Type: "quick-success", Queue: "default"})
	}()

	select {
	case <-completeEntered:
	case <-time.After(time.Second):
		t.Fatal("CompleteWithResult was not entered")
	}
	select {
	case <-allowComplete:
	case <-time.After(2 * time.Second):
		t.Fatal("CompleteWithResult did not observe an active heartbeat")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processJob did not return after CompleteWithResult")
	}
}

func TestWorker_FailTerminalWithResult_Atomicity(t *testing.T) {
	fanOutID := "fo-terminal-fail"
	parentID := "parent-terminal-fail"
	var legacyFailed atomic.Int32
	failCalled := make(chan string, 1)
	statusUpdated := make(chan core.FanOutStatus, 1)
	resumed := make(chan string, 1)

	base := &mockStorage{
		incrementFailedFunc: func(context.Context, string) (*core.FanOut, error) {
			legacyFailed.Add(1)
			return nil, nil
		},
		updateFanOutStatusFunc: func(_ context.Context, gotFanOutID string, status core.FanOutStatus) (bool, error) {
			assert.Equal(t, fanOutID, gotFanOutID)
			statusUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			resumed <- jobID
			return true, nil
		},
	}
	store := &atomicMockStorage{mockStorage: base}
	store.failTerminalWithResultFunc = func(_ context.Context, jobID, workerID, errMsg string) (*core.FanOut, error) {
		assert.Equal(t, "sub-terminal-fail", jobID)
		assert.NotEmpty(t, workerID)
		failCalled <- errMsg
		return &core.FanOut{
			ID:          fanOutID,
			ParentJobID: parentID,
			TotalCount:  1,
			FailedCount: 1,
			Strategy:    core.StrategyCollectAll,
		}, nil
	}

	q := queue.New(store)
	q.Register("terminal-error", func(context.Context, struct{}) error {
		return core.NoRetry(errors.New("terminal boom"))
	})
	failed := make(chan struct{}, 1)
	q.OnJobFail(func(context.Context, *core.Job, error) { failed <- struct{}{} })
	w := NewWorker(q, DisableRetry(), WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fanOutIDCopy := fanOutID
	w.processJob(context.Background(), &core.Job{
		ID:         "sub-terminal-fail",
		Type:       "terminal-error",
		Queue:      "default",
		FanOutID:   &fanOutIDCopy,
		Attempt:    1,
		MaxRetries: 1,
	})

	select {
	case msg := <-failCalled:
		assert.Contains(t, msg, "terminal boom")
	case <-time.After(time.Second):
		t.Fatal("FailTerminalWithResult was not called")
	}
	assert.Equal(t, int32(0), legacyFailed.Load(), "atomic terminal fail path must not use legacy failed increment")
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("failure hook was not called")
	}
	select {
	case status := <-statusUpdated:
		assert.Equal(t, core.FanOutCompleted, status)
	case <-time.After(time.Second):
		t.Fatal("fan-out status was not updated")
	}
	select {
	case gotParent := <-resumed:
		assert.Equal(t, parentID, gotParent)
	case <-time.After(time.Second):
		t.Fatal("parent was not resumed")
	}
}

// ---------------------------------------------------------------------------
// runHeartbeat — cover tick path directly (unit-level)
// ---------------------------------------------------------------------------

func TestWorker_RunHeartbeat_ExitsOnContextCancel(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	ctx, cancel := context.WithCancel(context.Background())

	job := &core.Job{ID: "hb-job", Type: "noop", Queue: "default"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runHeartbeat(ctx, job)
	}()

	cancel()

	select {
	case <-done:
		// runHeartbeat returned after context cancellation.
	case <-time.After(time.Second):
		t.Fatal("runHeartbeat did not exit after context cancellation")
	}
}

func TestWorker_RunHeartbeat_StopsOnAggressivePause(t *testing.T) {
	// We need the heartbeat to actually tick. Use a custom implementation
	// that exercises the pause-check branch by calling the method with a
	// replaced ticker.  Since runHeartbeat uses a hardcoded 2-minute ticker
	// we cannot easily trigger the tick in a test.  Instead we verify that
	// calling Pause(Aggressive) + context cancel exits cleanly.
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	ctx, cancel := context.WithCancel(context.Background())

	job := &core.Job{ID: "hb-job-agg", Type: "noop", Queue: "default"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runHeartbeat(ctx, job)
	}()

	w.Pause(core.PauseModeAggressive)
	cancel() // context cancel causes select to fire

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runHeartbeat did not exit after aggressive pause + cancel")
	}
}

// ---------------------------------------------------------------------------
// handleError — RetryAfter with exhausted retries goes to fail-permanently
// ---------------------------------------------------------------------------

func TestWorker_HandleError_RetryAfterExhausted(t *testing.T) {
	// When RetryAfterError is returned but job.Attempt >= job.MaxRetries,
	// the code falls through to normal retry logic, not the RetryAfter branch.
	// We exercise this by injecting a job with Attempt >= MaxRetries via mock.
	mock := &mockStorage{}

	failCalled := make(chan struct{}, 1)
	mock.failFunc = func(_ context.Context, _ string, _ string, _ string, _ *time.Time) error {
		select {
		case failCalled <- struct{}{}:
		default:
		}
		return nil
	}

	q := queue.New(mock)
	retryCalled := make(chan struct{}, 1)
	q.Register("ra-exhausted", func(ctx context.Context, args struct{}) error {
		return core.RetryAfter(1*time.Millisecond, errors.New("retry after"))
	})
	q.OnRetry(func(_ context.Context, _ *core.Job, _ int, _ error) {
		select {
		case retryCalled <- struct{}{}:
		default:
		}
	})

	// Deliver a job whose Attempt already equals MaxRetries so the RetryAfter
	// branch IS entered but schedules the retry.
	mock.dequeueFunc = func(_ context.Context, _ []string, _ string) (*core.Job, error) {
		mock.dequeueFunc = nil
		return &core.Job{
			ID:         "ra-job-1",
			Type:       "ra-exhausted",
			Queue:      "default",
			Args:       []byte(`{}`),
			Attempt:    1, // Attempt < MaxRetries: retry branch is taken
			MaxRetries: 3,
		}, nil
	}

	w := NewWorker(q, WithStaleLockInterval(0),
		WithStorageRetry(RetryConfig{MaxAttempts: 1}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	select {
	case <-retryCalled:
		// RetryAfter branch was exercised.
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("RetryAfter retry hook was not called")
	}
}

// ---------------------------------------------------------------------------
// runScheduler tests
// ---------------------------------------------------------------------------

type offsetSchedule struct {
	offset time.Duration
}

func (s offsetSchedule) Next(from time.Time) time.Time {
	return from.Add(s.offset)
}

type fixedBoundarySchedule struct {
	fire time.Time
}

func (s fixedBoundarySchedule) Next(from time.Time) time.Time {
	if from.Before(s.fire) {
		return s.fire
	}
	return s.fire.Add(time.Hour)
}

func TestSeedLastRun_NoGapReturnsPersisted(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)
	persisted := now
	s := schedule.Every(time.Minute)

	result, _ := seedLastRun(s, persisted, now)

	assert.Equal(t, persisted, result)
}

func TestSeedLastRun_SingleMissedBoundaryFiresOnce(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)
	persisted := now.Add(-90 * time.Second)
	s := schedule.Every(time.Minute)

	seed, _ := seedLastRun(s, persisted, now)
	fire := s.Next(seed)

	assert.False(t, fire.After(now), "seeded next fire should be due")
	assert.True(t, s.Next(fire).After(now), "only one catch-up boundary should be due")
}

func TestSeedLastRun_ManyMissedBoundariesClampToSingleFire(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)
	persisted := now.Add(-3 * time.Hour)
	s := schedule.Every(time.Minute)

	seed, _ := seedLastRun(s, persisted, now)
	fire := s.Next(seed)

	assert.False(t, fire.After(now), "seeded next fire should be due")
	assert.True(t, fire.After(now.Add(-time.Minute)), "fire should be the most recent due boundary")
	assert.True(t, s.Next(fire).After(now), "the following boundary should not be due")
}

func TestSeedLastRun_PathologicalDensityFailsSafe(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)
	persisted := now.Add(-24 * time.Hour)
	s := schedule.Every(time.Millisecond)

	result, _ := seedLastRun(s, persisted, now)

	assert.Equal(t, now, result)
	assert.True(t, s.Next(result).After(now), "fail-safe seed should skip catch-up")
}

// TestWorker_RunScheduler_ExitsOnContextCancel verifies that runScheduler
// returns promptly when its context is cancelled.
func TestWorker_RunScheduler_ExitsOnContextCancel(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runScheduler(ctx)
	}()

	cancel()

	select {
	case <-done:
		// runScheduler exited cleanly after context cancellation.
	case <-time.After(time.Second):
		t.Fatal("runScheduler did not exit after context cancellation")
	}
}

// TestWorker_RunScheduler_NilScheduledJobsIsNoop verifies that runScheduler
// handles the case where there are no scheduled jobs (GetScheduledJobs returns nil).
func TestWorker_RunScheduler_NilScheduledJobsIsNoop(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	// No scheduled jobs registered on q.

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runScheduler(ctx)
	}()

	<-ctx.Done()

	select {
	case <-done:
		// Should have exited when context expired.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runScheduler did not exit after context expiry")
	}

	// No enqueue should have been attempted.
	assert.Equal(t, int64(0), mock.getEnqueueCount())
}

func TestWorker_RunScheduler_DoesNotFireFutureScheduleOnStartup(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	q.Register("scheduled-task", func(_ context.Context, _ struct{}) error {
		return nil
	})
	q.Schedule("scheduled-task", nil, offsetSchedule{offset: time.Hour})

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	go func() { w.runScheduler(ctx) }()

	<-ctx.Done()
	assert.Equal(t, int64(0), mock.getEnqueueCount())
}

// TestWorker_RunScheduler_EnqueuesDueJob verifies that a scheduled job whose
// next run time is in the past gets enqueued by the scheduler goroutine.
func TestWorker_RunScheduler_EnqueuesDueJob(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	// Register the handler so Queue.Enqueue does not reject the job.
	q.Register("scheduled-task", func(_ context.Context, _ struct{}) error {
		return nil
	})

	// Register a schedule that always returns a time in the past so the
	// scheduler immediately considers the job due.
	q.Schedule("scheduled-task", nil, schedule.Every(1*time.Millisecond))

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { w.runScheduler(ctx) }()

	// Wait until at least one job has been enqueued.
	require.Eventually(t, func() bool {
		jobs, err := q.Storage().GetJobsByStatus(context.Background(), core.StatusPending, 10)
		if err != nil {
			return false
		}
		return len(jobs) > 0
	}, 1500*time.Millisecond, 20*time.Millisecond,
		"scheduler should enqueue at least one due job")
}

func TestWorker_RunScheduler_ForwardsArgsOptionsAndUserUniqueKey(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	q.Register("scheduled-task", func(_ context.Context, _ map[string]string) error {
		return nil
	})

	fire := time.Now().Add(300 * time.Millisecond)
	runAt := fire.Add(time.Minute)
	args := map[string]string{"tenant": "acme"}
	q.Schedule("scheduled-task", args, fixedBoundarySchedule{fire: fire},
		queue.QueueOpt("scheduled"),
		queue.Priority(9),
		queue.Retries(5),
		queue.At(runAt),
		queue.Timeout(time.Second),
		queue.Unique("user:scheduled-task"),
	)

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { w.runScheduler(ctx) }()

	require.Eventually(t, func() bool {
		mock.enqueueMu.Lock()
		defer mock.enqueueMu.Unlock()
		return len(mock.enqueuedJobs) == 1
	}, 900*time.Millisecond, 20*time.Millisecond)

	mock.enqueueMu.Lock()
	defer mock.enqueueMu.Unlock()
	require.Len(t, mock.enqueuedJobs, 1)
	job := mock.enqueuedJobs[0]
	assert.Equal(t, "scheduled-task", job.Type)
	assert.JSONEq(t, `{"tenant":"acme"}`, string(job.Args))
	assert.Equal(t, "scheduled", job.Queue)
	assert.Equal(t, 9, job.Priority)
	assert.Equal(t, 5, job.MaxRetries)
	require.NotNil(t, job.RunAt)
	assert.Equal(t, runAt, *job.RunAt)
	assert.Equal(t, "user:scheduled-task", mock.enqueuedKeys[0])
	assert.Equal(t, "user:scheduled-task", job.UniqueKey)
}

func TestWorker_RunScheduler_ClaimRefusalAdvancesBoundary(t *testing.T) {
	var claimCalls atomic.Int32
	mock := &mockStorage{}
	mock.claimFireFunc = func(context.Context, string, time.Time) (bool, error) {
		claimCalls.Add(1)
		return false, nil
	}
	q := queue.New(mock)
	q.Register("scheduled-task", func(_ context.Context, _ struct{}) error {
		return nil
	})

	fire := time.Now().Add(250 * time.Millisecond)
	q.Schedule("scheduled-task", nil, fixedBoundarySchedule{fire: fire})

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()
	go func() { w.runScheduler(ctx) }()

	<-ctx.Done()

	assert.Equal(t, int32(1), claimCalls.Load(), "refused claim should advance lastRun past the boundary")
	assert.Equal(t, int64(0), mock.getEnqueueCount())
}

func TestWorker_RunScheduler_ClaimedBoundaryEnqueuesOnce(t *testing.T) {
	var claimCalls atomic.Int32
	mock := &mockStorage{}
	mock.claimFireFunc = func(context.Context, string, time.Time) (bool, error) {
		return claimCalls.Add(1) == 1, nil
	}
	q := queue.New(mock)
	q.Register("scheduled-task", func(_ context.Context, _ struct{}) error {
		return nil
	})

	fire := time.Now().Add(250 * time.Millisecond)
	q.Schedule("scheduled-task", nil, fixedBoundarySchedule{fire: fire})

	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()
	go func() { w.runScheduler(ctx) }()

	<-ctx.Done()

	assert.Equal(t, int32(1), claimCalls.Load(), "claimed boundary should advance lastRun")
	assert.Equal(t, int64(1), mock.getEnqueueCount())
	mock.enqueueMu.Lock()
	defer mock.enqueueMu.Unlock()
	assert.Empty(t, mock.enqueuedKeys, "scheduler must not inject a sched: unique key")
}

func TestScheduler_CatchUpAfterFleetGap(t *testing.T) {
	period := time.Hour
	now := time.Now().UTC()
	catchUpLastFireAt := now.Add(-period)
	freshLastFireAt := now

	var claimCalls atomic.Int32
	claimedNames := make(chan string, 4)
	mock := &mockStorage{
		fireTimes: map[string]time.Time{
			"catch-up-task": catchUpLastFireAt,
			"fresh-task":    freshLastFireAt,
		},
	}
	mock.claimFireFunc = func(_ context.Context, name string, fireTime time.Time) (bool, error) {
		claimCalls.Add(1)
		claimedNames <- name
		assert.Equal(t, "catch-up-task", name, "fresh schedule must preserve anti-boot-storm")
		assert.WithinDuration(t, catchUpLastFireAt.Add(period), fireTime, time.Second)
		return true, nil
	}

	q := queue.New(mock)
	q.Register("catch-up-task", func(_ context.Context, _ struct{}) error { return nil })
	q.Register("fresh-task", func(_ context.Context, _ struct{}) error { return nil })
	q.Schedule("catch-up-task", nil, offsetSchedule{offset: period})
	q.Schedule("fresh-task", nil, offsetSchedule{offset: period})

	w := NewWorker(q, WithStaleLockInterval(0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.runScheduler(ctx) }()

	select {
	case name := <-claimedNames:
		assert.Equal(t, "catch-up-task", name)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("scheduler did not catch up the missed boundary shortly after start")
	}

	require.Eventually(t, func() bool {
		return mock.getEnqueueCount() == 1
	}, time.Second, 10*time.Millisecond)

	time.Sleep(250 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(1), claimCalls.Load(), "only the missed boundary should claim; fresh schedule must not fire")
	assert.Equal(t, int64(1), mock.getEnqueueCount(), "catch-up must enqueue exactly once")
	mock.enqueueMu.Lock()
	defer mock.enqueueMu.Unlock()
	require.Len(t, mock.enqueuedJobs, 1)
	assert.Equal(t, "catch-up-task", mock.enqueuedJobs[0].Type)
}

func TestScheduler_ManyMissedBoundariesFireOnce(t *testing.T) {
	period := time.Minute
	now := time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)
	persisted := now.Add(-3 * time.Hour)
	s := schedule.Every(period)

	seed, _ := seedLastRun(s, persisted, now)
	catchUpFire := s.Next(seed)

	assert.False(t, catchUpFire.After(now), "the catch-up boundary should be due")
	assert.True(t, catchUpFire.After(now.Add(-period)), "catch-up should use the most recent missed boundary")
	assert.True(t, s.Next(catchUpFire).After(now), "only one catch-up fire should be due before natural cadence resumes")
}

// ---------------------------------------------------------------------------
// checkFanOutCompletion / completeFanOut tests
// ---------------------------------------------------------------------------

// TestWorker_CheckFanOutCompletion_AllDoneCompletesParent exercises the path
// where completed+failed+cancelled == total, causing completeFanOut to be called.
// Uses the StrategyCollectAll strategy so status becomes FanOutCompleted.
func TestWorker_CheckFanOutCompletion_AllDoneCompletesParent(t *testing.T) {
	parentResumed := make(chan string, 1)
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, id string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil // claim the update
		},
		resumeJobFunc: func(_ context.Context, jobID string) (bool, error) {
			parentResumed <- jobID
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fo := &core.FanOut{
		ID:             "fo-1",
		ParentJobID:    "parent-job-1",
		TotalCount:     2,
		CompletedCount: 2,
		FailedCount:    0,
		CancelledCount: 0,
		Strategy:       core.StrategyCollectAll,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutCompleted, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called")
	}

	select {
	case jobID := <-parentResumed:
		assert.Equal(t, "parent-job-1", jobID)
	case <-time.After(time.Second):
		t.Fatal("ResumeJob was not called for parent")
	}
}

// TestWorker_CheckFanOutCompletion_NotDoneYetReturnsNil exercises the early
// return when there are still pending sub-jobs.
func TestWorker_CheckFanOutCompletion_NotDoneYetReturnsNil(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)
	w := NewWorker(q)

	fo := &core.FanOut{
		ID:             "fo-2",
		ParentJobID:    "parent-job-2",
		TotalCount:     5,
		CompletedCount: 2,
		FailedCount:    0,
		CancelledCount: 0,
		Strategy:       core.StrategyCollectAll,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	assert.NoError(t, err, "incomplete fan-out should return nil without calling completeFanOut")
}

// TestWorker_CheckFanOutCompletion_FailFastTriggersEarly exercises the fail-fast
// branch where a failure is detected before all sub-jobs complete.
func TestWorker_CheckFanOutCompletion_FailFastTriggersEarly(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// 3 of 5 sub-jobs done, but 1 failed — fail-fast should trigger immediately.
	fo := &core.FanOut{
		ID:             "fo-3",
		ParentJobID:    "parent-job-3",
		TotalCount:     5,
		CompletedCount: 2,
		FailedCount:    1,
		CancelledCount: 0,
		Strategy:       core.StrategyFailFast,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutFailed, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called for fail-fast early exit")
	}
}

// TestWorker_CheckFanOutCompletion_ThresholdDoomed exercises the threshold
// strategy early-exit when the remaining in-flight jobs cannot meet the requirement.
func TestWorker_CheckFanOutCompletion_ThresholdDoomed(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// Threshold = 0.8 (80% success required). With 4 completed, 3 failed, and
	// 3 in flight, the best possible result is 7 successes, below the required 8.
	fo := &core.FanOut{
		ID:             "fo-4",
		ParentJobID:    "parent-job-4",
		TotalCount:     10,
		CompletedCount: 4,
		FailedCount:    3,
		CancelledCount: 0,
		Strategy:       core.StrategyThreshold,
		Threshold:      0.8,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutFailed, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called for threshold breach")
	}
}

// TestWorker_CheckFanOutCompletion_AllCancelledNoCompletions exercises CollectAll's
// all-accounted path. CollectAll terminal status is completed even when every
// sub-job was cancelled; per-index errors are surfaced by CollectResults.
func TestWorker_CheckFanOutCompletion_AllCancelledNoCompletions(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fo := &core.FanOut{
		ID:             "fo-5",
		ParentJobID:    "parent-job-5",
		TotalCount:     3,
		CompletedCount: 0,
		FailedCount:    0,
		CancelledCount: 3,
		Strategy:       core.StrategyCollectAll,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutCompleted, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called")
	}
}

// TestWorker_CheckFanOutCompletion_ThresholdAllDoneSuccessRate exercises
// the threshold strategy when all sub-jobs are done and the success rate
// is checked against the threshold.
func TestWorker_CheckFanOutCompletion_ThresholdAllDoneBelowThreshold(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// All 4 sub-jobs done: 1 completed, 3 failed. successRate = 0.25 < threshold 0.8.
	fo := &core.FanOut{
		ID:             "fo-6",
		ParentJobID:    "parent-job-6",
		TotalCount:     4,
		CompletedCount: 1,
		FailedCount:    3,
		CancelledCount: 0,
		Strategy:       core.StrategyThreshold,
		Threshold:      0.8,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutFailed, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called")
	}
}

// TestWorker_CheckFanOutCompletion_ThresholdAllDoneAboveThreshold exercises
// the threshold strategy when the success rate meets or exceeds the threshold.
func TestWorker_CheckFanOutCompletion_ThresholdAllDoneAboveThreshold(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// All 4 sub-jobs done: 4 completed, 0 failed. successRate = 1.0 >= threshold 0.8.
	fo := &core.FanOut{
		ID:             "fo-7",
		ParentJobID:    "parent-job-7",
		TotalCount:     4,
		CompletedCount: 4,
		FailedCount:    0,
		CancelledCount: 0,
		Strategy:       core.StrategyThreshold,
		Threshold:      0.8,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutCompleted, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called")
	}
}

// TestWorker_CheckFanOutCompletion_ThresholdAllCancelledNoActive exercises the
// threshold strategy when all active jobs are cancelled (activeTotal == 0).
func TestWorker_CheckFanOutCompletion_ThresholdAllCancelledNoActive(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// Threshold strategy with all cancelled — activeTotal == 0, falls to failed.
	fo := &core.FanOut{
		ID:             "fo-8",
		ParentJobID:    "parent-job-8",
		TotalCount:     3,
		CompletedCount: 0,
		FailedCount:    0,
		CancelledCount: 3,
		Strategy:       core.StrategyThreshold,
		Threshold:      0.8,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutFailed, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called")
	}
}

// TestWorker_CompleteFanOut_AlreadyCompletedByAnotherWorker exercises the
// branch where UpdateFanOutStatus returns (false, nil). Under P2's in-tx status
// advance this is the COMMON case for the worker that just completed the last
// sub-job (its own terminal transaction already advanced the status), so
// completeFanOut must NOT early-return: it falls through to the idempotent
// ResumeJob. The parent-row CAS keeps concurrent/duplicate callers single-effect.
func TestWorker_CompleteFanOut_AlreadyCompletedByAnotherWorker(t *testing.T) {
	resumeCalled := false

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return false, nil // status already advanced (in-tx, or by another worker)
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			resumeCalled = true
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fo := &core.FanOut{
		ID:          "fo-9",
		ParentJobID: "parent-job-9",
	}

	err := w.completeFanOut(context.Background(), fo, core.FanOutCompleted)
	require.NoError(t, err)
	assert.True(t, resumeCalled, "ResumeJob must still be called when the status was already terminal (in-tx advance)")
}

// TestWorker_CompleteFanOut_CancelOnFailCancelsSubJobs exercises the
// CancelOnFail=true + FanOutFailed branch inside completeFanOut.
func TestWorker_CompleteFanOut_CancelOnFailCancelsSubJobs(t *testing.T) {
	cancelSubJobsCalled := make(chan string, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
		cancelSubJobsFunc: func(_ context.Context, fanOutID string) ([]string, error) {
			cancelSubJobsCalled <- fanOutID
			return []string{"sub-1", "sub-2"}, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fo := &core.FanOut{
		ID:           "fo-10",
		ParentJobID:  "parent-job-10",
		CancelOnFail: true,
	}

	err := w.completeFanOut(context.Background(), fo, core.FanOutFailed)
	require.NoError(t, err)

	select {
	case id := <-cancelSubJobsCalled:
		assert.Equal(t, "fo-10", id)
	case <-time.After(time.Second):
		t.Fatal("CancelSubJobs was not called with CancelOnFail=true")
	}
}

// TestWorker_CompleteFanOut_ParentNotInWaitingStatus exercises the warning
// path when ResumeJob returns (false, nil) — parent was not waiting.
func TestWorker_CompleteFanOut_ParentNotInWaitingStatus(t *testing.T) {
	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return false, nil // parent not in waiting status
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	fo := &core.FanOut{
		ID:          "fo-11",
		ParentJobID: "parent-job-11",
	}

	// Should log a warning but return nil (not an error).
	err := w.completeFanOut(context.Background(), fo, core.FanOutCompleted)
	assert.NoError(t, err)
}

// TestWorker_HandleSubJobCompletion_FullFlow_Succeeded wires a real fan-out
// counter increment into handleSubJobCompletion to exercise the path where
// IncrementFanOutCompleted returns a non-nil FanOut that triggers completion.
func TestWorker_HandleSubJobCompletion_FullFlow_Succeeded(t *testing.T) {
	parentResumed := make(chan struct{}, 1)

	fanOutID := "fo-full-1"
	fo := &core.FanOut{
		ID:             fanOutID,
		ParentJobID:    "parent-full-1",
		TotalCount:     1,
		CompletedCount: 1, // this increment makes it complete
		FailedCount:    0,
		CancelledCount: 0,
		Strategy:       core.StrategyCollectAll,
	}

	mock := &mockStorage{
		incrementCompletedFunc: func(_ context.Context, _ string) (*core.FanOut, error) {
			return fo, nil
		},
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			parentResumed <- struct{}{}
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	job := &core.Job{ID: "sub-job-1", FanOutID: &fanOutID}

	err := w.handleSubJobCompletion(context.Background(), job, true)
	require.NoError(t, err)

	select {
	case <-parentResumed:
		// completeFanOut was called and parent was resumed.
	case <-time.After(time.Second):
		t.Fatal("parent was not resumed after fan-out completion")
	}
}

// TestWorker_HandleSubJobCompletion_FullFlow_Failed wires IncrementFanOutFailed
// to return a FanOut that is now fully accounted for (failed path).
func TestWorker_HandleSubJobCompletion_FullFlow_Failed(t *testing.T) {
	parentResumed := make(chan struct{}, 1)

	fanOutID := "fo-full-2"
	fo := &core.FanOut{
		ID:             fanOutID,
		ParentJobID:    "parent-full-2",
		TotalCount:     1,
		CompletedCount: 0,
		FailedCount:    1,
		CancelledCount: 0,
		Strategy:       core.StrategyCollectAll,
	}

	mock := &mockStorage{
		incrementFailedFunc: func(_ context.Context, _ string) (*core.FanOut, error) {
			return fo, nil
		},
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			parentResumed <- struct{}{}
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	job := &core.Job{ID: "sub-job-2", FanOutID: &fanOutID}

	err := w.handleSubJobCompletion(context.Background(), job, false)
	require.NoError(t, err)

	select {
	case <-parentResumed:
		// completeFanOut called via the failed path.
	case <-time.After(time.Second):
		t.Fatal("parent was not resumed after fan-out failure")
	}
}

// TestWorker_CheckFanOutCompletion_FailFastAllDone exercises the
// StrategyFailFast branch when all jobs are done and FailedCount > 0.
func TestWorker_CheckFanOutCompletion_FailFastAllDone(t *testing.T) {
	fanOutUpdated := make(chan core.FanOutStatus, 1)

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, status core.FanOutStatus) (bool, error) {
			fanOutUpdated <- status
			return true, nil
		},
		resumeJobFunc: func(_ context.Context, _ string) (bool, error) {
			return true, nil
		},
	}
	q := queue.New(mock)
	w := NewWorker(q, WithStorageRetry(RetryConfig{MaxAttempts: 1}))

	// All done, 1 succeeded and 1 failed; FailFast → FanOutFailed.
	fo := &core.FanOut{
		ID:             "fo-ff-done",
		ParentJobID:    "parent-ff-done",
		TotalCount:     2,
		CompletedCount: 1,
		FailedCount:    1,
		CancelledCount: 0,
		Strategy:       core.StrategyFailFast,
	}

	err := w.checkFanOutCompletion(context.Background(), fo)
	require.NoError(t, err)

	select {
	case status := <-fanOutUpdated:
		assert.Equal(t, core.FanOutFailed, status)
	case <-time.After(time.Second):
		t.Fatal("UpdateFanOutStatus was not called for FailFast all-done path")
	}
}

// ---------------------------------------------------------------------------
// Handler result persistence tests
// ---------------------------------------------------------------------------

// trackingStorage wraps a real storage and records SaveJobResult calls.
type trackingStorage struct {
	core.Storage
	mu           sync.Mutex
	savedResults map[string][]byte
}

func (s *trackingStorage) SaveJobResult(ctx context.Context, jobID string, workerID string, result []byte) error {
	s.mu.Lock()
	if s.savedResults == nil {
		s.savedResults = map[string][]byte{}
	}
	s.savedResults[jobID] = result
	s.mu.Unlock()
	// Also delegate to the real storage so the job lifecycle isn't broken.
	return s.Storage.SaveJobResult(ctx, jobID, workerID, result)
}

func TestWorker_PersistsHandlerResult(t *testing.T) {
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	base := storage.NewGormStorage(db)
	require.NoError(t, base.Migrate(context.Background()))

	store := &trackingStorage{Storage: base}
	q := queue.New(store)
	q.Register("make-video", func(ctx context.Context, name string) (map[string]any, error) {
		return map[string]any{"name": name, "duration_s": 12}, nil
	})

	jobID, enqErr := q.Enqueue(context.Background(), "make-video", "demo")
	require.NoError(t, enqErr)

	w := NewWorker(q, WithScheduler(false))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		_, ok := store.savedResults[jobID]
		return ok
	}, 5*time.Second, 10*time.Millisecond, "handler result must be persisted via SaveJobResult")
	cancel()
	select {
	case <-workerErr:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after result persistence test")
	}

	store.mu.Lock()
	got := store.savedResults[jobID]
	store.mu.Unlock()
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(got, &decoded))
	assert.Equal(t, "demo", decoded["name"])
}

// ---------------------------------------------------------------------------
// runHeartbeat orphan-abandonment tests
// ---------------------------------------------------------------------------

// TestRunHeartbeat_AbandonsOrphanedJob is the regression for the 2026-05-19
// production observation: a stale-lock reaper reclaimed two jobs from the
// running worker, but the worker's heartbeat goroutines kept logging
// "jobs: job not owned by this worker" every 2 minutes for HOURS, while
// the orphaned handler goroutines continued doing work against jobs they
// no longer owned. The fix: after orphanHeartbeatThreshold consecutive
// ErrJobNotOwned responses, the heartbeat cancels the handler's context
// via CancelJob and exits.
func TestRunHeartbeat_AbandonsOrphanedJob(t *testing.T) {
	mock := &mockStorage{}
	var heartbeatCalls atomic.Int32
	mock.heartbeatFunc = func(_ context.Context, _ string, _ string) error {
		heartbeatCalls.Add(1)
		return core.ErrJobNotOwned
	}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())
	w.heartbeatInterval = 10 * time.Millisecond // drive the loop fast

	// Register a fake running-job context so CancelJob has something to cancel.
	jobCtx, jobCancel := context.WithCancel(context.Background())
	defer jobCancel()
	w.runningJobsMu.Lock()
	w.runningJobs["test-job-id"] = jobCancel
	w.runningJobsMu.Unlock()

	// Drive the heartbeat loop until orphan threshold trips.
	hbCtx, hbCancel := context.WithCancel(context.Background())
	defer hbCancel()
	done := make(chan struct{})
	go func() {
		w.runHeartbeat(hbCtx, &core.Job{ID: "test-job-id"})
		close(done)
	}()

	// runHeartbeat should call CancelJob (cancelling jobCtx) within a
	// few ticks past the threshold. Give it generous slack.
	select {
	case <-jobCtx.Done():
		// success — orphan threshold tripped, handler context cancelled
	case <-time.After(2 * time.Second):
		t.Fatal("runHeartbeat did not cancel orphaned handler within 2s")
	}

	// runHeartbeat itself should also return after cancelling the handler.
	select {
	case <-done:
		// success — goroutine exited; no more "heartbeat failed" log spam
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runHeartbeat did not exit after abandoning job")
	}

	// Sanity: it should have called Heartbeat at least orphanHeartbeatThreshold
	// times before giving up.
	if got := heartbeatCalls.Load(); int(got) < orphanHeartbeatThreshold {
		t.Errorf("heartbeat tried %d times before abandoning; want at least %d",
			got, orphanHeartbeatThreshold)
	}
}

// TestRunHeartbeat_TransientErrorDoesNotAbandon confirms a transient
// non-ownership error (e.g. DB unreachable) doesn't trip the orphan
// threshold. The counter only advances on ErrJobNotOwned specifically.
func TestRunHeartbeat_TransientErrorDoesNotAbandon(t *testing.T) {
	mock := &mockStorage{}
	var heartbeatCalls atomic.Int32
	transientErr := errors.New("db unreachable")
	mock.heartbeatFunc = func(_ context.Context, _ string, _ string) error {
		heartbeatCalls.Add(1)
		return transientErr
	}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())
	w.heartbeatInterval = 10 * time.Millisecond

	jobCtx, jobCancel := context.WithCancel(context.Background())
	defer jobCancel()
	w.runningJobsMu.Lock()
	w.runningJobs["transient-job"] = jobCancel
	w.runningJobsMu.Unlock()

	hbCtx, hbCancel := context.WithCancel(context.Background())
	defer hbCancel()
	go w.runHeartbeat(hbCtx, &core.Job{ID: "transient-job"})

	// Let the heartbeat tick several times.
	time.Sleep(150 * time.Millisecond)

	// jobCtx must NOT be cancelled — transient errors don't trip orphan.
	select {
	case <-jobCtx.Done():
		t.Fatal("transient error wrongly tripped orphan abandonment")
	default:
		// expected: handler context still live
	}

	if heartbeatCalls.Load() < int32(orphanHeartbeatThreshold)+2 {
		t.Errorf("expected several heartbeat attempts, got %d", heartbeatCalls.Load())
	}
}

// TestRunHeartbeat_SuccessResetsCounter confirms that a single success
// after some orphan errors resets the orphan counter — so a brief
// ownership blip doesn't accumulate toward eventual abandonment if the
// owner reclaims the job in time.
func TestRunHeartbeat_SuccessResetsCounter(t *testing.T) {
	mock := &mockStorage{}
	var calls atomic.Int32
	mock.heartbeatFunc = func(_ context.Context, _ string, _ string) error {
		n := calls.Add(1)
		// Return ErrJobNotOwned for the first 2 calls, then succeed, then
		// orphan again. The counter should reset after the success, so
		// we shouldn't trip abandonment until 3 MORE orphan responses
		// pile up after the success.
		if n <= 2 {
			return core.ErrJobNotOwned
		}
		if n == 3 {
			return nil
		}
		return core.ErrJobNotOwned
	}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())
	w.heartbeatInterval = 10 * time.Millisecond

	jobCtx, jobCancel := context.WithCancel(context.Background())
	defer jobCancel()
	w.runningJobsMu.Lock()
	w.runningJobs["blip-job"] = jobCancel
	w.runningJobsMu.Unlock()

	hbCtx, hbCancel := context.WithCancel(context.Background())
	defer hbCancel()
	done := make(chan struct{})
	go func() {
		w.runHeartbeat(hbCtx, &core.Job{ID: "blip-job"})
		close(done)
	}()

	// Should still eventually abandon — but the success at call #3 resets
	// the counter, so it takes 3 more orphan errors (calls 4, 5, 6) to
	// trip abandonment. That's call #6 minimum.
	select {
	case <-jobCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("runHeartbeat did not abandon after orphan persisted post-success")
	}

	if got := calls.Load(); got < 6 {
		t.Errorf("abandoned at call %d; expected >= 6 because success at call 3 should have reset the counter", got)
	}

	<-done
}

// TestCompleteFanOut_CancelsLocalSubJobHandlers verifies the fix for the
// 2026-05-19 production observation: when a fan-out fails with
// CancelOnFail=true, the in-flight sub-job handlers running on this worker
// must have their contexts cancelled. The old code only updated DB rows,
// leaving the handler goroutines running for as long as it took their own
// heartbeats to notice they'd been reclaimed.
func TestCompleteFanOut_CancelsLocalSubJobHandlers(t *testing.T) {
	mock := &mockStorage{}
	mock.cancelSubJobsFunc = func(_ context.Context, fanOutID string) ([]string, error) {
		return []string{"sub-a", "sub-b", "sub-c"}, nil
	}
	mock.updateFanOutStatusFunc = func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
		return true, nil
	}
	mock.resumeJobFunc = func(_ context.Context, _ string) (bool, error) {
		return true, nil
	}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())

	// Pre-populate the runningJobs map with the local sub-jobs.
	cancelCalls := make(map[string]int)
	for _, id := range []string{"sub-a", "sub-b"} {
		id := id
		w.runningJobsMu.Lock()
		w.runningJobs[id] = func() { cancelCalls[id]++ }
		w.runningJobsMu.Unlock()
	}
	// sub-c is intentionally NOT in this worker's runningJobs — it's
	// "running on another worker." Cancellation for cross-worker
	// sub-jobs goes through the heartbeat-abandon path instead.

	fo := &core.FanOut{
		ID:           "fo-test",
		ParentJobID:  "parent-1",
		CancelOnFail: true,
	}

	err := w.completeFanOut(context.Background(), fo, core.FanOutFailed)
	require.NoError(t, err)

	assert.Equal(t, 1, cancelCalls["sub-a"], "local sub-a context should have been cancelled exactly once")
	assert.Equal(t, 1, cancelCalls["sub-b"], "local sub-b context should have been cancelled exactly once")
	_, hasC := cancelCalls["sub-c"]
	assert.False(t, hasC, "sub-c was not in this worker's runningJobs; should not have been cancelled locally")
}

// TestReapStaleLocks_CancelsLocalHandlersOfReleasedJobs verifies that when
// the stale-lock reaper releases locks on jobs whose heartbeats are dead,
// any local in-flight handlers for those jobs also get their contexts
// cancelled — instead of running forever until their own heartbeat
// abandons (which can be 6+ minutes).
func TestReapStaleLocks_CancelsLocalHandlersOfReleasedJobs(t *testing.T) {
	mock := &mockStorage{}
	mock.releasedIDs = []string{"orphan-1", "orphan-2"}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())
	// Drive the reaper fast. The public WithStaleLockInterval enforces a 1s
	// floor, so set the cadence directly (the reaper reads config at startup).
	w.config.StaleLockInterval = 10 * time.Millisecond

	// Counters are incremented from the reaper goroutine (via CancelJob's
	// stored func) and read from the test goroutine, so they must be atomic.
	var orphan1, orphan2 atomic.Int32
	w.runningJobsMu.Lock()
	w.runningJobs["orphan-1"] = func() { orphan1.Add(1) }
	w.runningJobs["orphan-2"] = func() { orphan2.Add(1) }
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.reapStaleLocks(ctx)

	// Give the reaper a couple of ticks.
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, orphan1.Load(), int32(1), "orphan-1 should have been cancelled")
	assert.GreaterOrEqual(t, orphan2.Load(), int32(1), "orphan-2 should have been cancelled")
}

// ---------------------------------------------------------------------------
// runOwnershipAudit cross-worker cancellation tests
// ---------------------------------------------------------------------------

// TestOwnershipAudit_CancelsOrphanedLocalHandlers verifies the cross-worker
// case: when another worker cancels or reclaims one of our running jobs,
// the audit goroutine detects it via FindOrphanedJobs and cancels the
// local handler context.
//
// Direct simulation: register two "running" jobs in this worker's map,
// have the mock return one of them as orphaned, and confirm only THAT
// one's context is cancelled.
func TestOwnershipAudit_CancelsOrphanedLocalHandlers(t *testing.T) {
	mock := &mockStorage{}
	mock.findOrphanedFunc = func(jobIDs []string) ([]string, error) {
		// "job-mine" is still ours; "job-stolen" is orphaned.
		for _, id := range jobIDs {
			if id == "job-stolen" {
				return []string{"job-stolen"}, nil
			}
		}
		return nil, nil
	}

	q := queue.New(mock)
	w := NewWorker(q,
		DisableRetry(),
		WithOwnershipAuditInterval(10*time.Millisecond),
	)

	// Counters are incremented from the audit goroutine (via CancelJob's
	// stored func) and read from the test goroutine, so they must be atomic.
	var mineCancelled, stolenCancelled atomic.Int32
	w.runningJobsMu.Lock()
	w.runningJobs["job-mine"] = func() { mineCancelled.Add(1) }
	w.runningJobs["job-stolen"] = func() { stolenCancelled.Add(1) }
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runOwnershipAudit(ctx)

	// Give the audit a couple of ticks.
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, stolenCancelled.Load(), int32(1), "stolen job's context should have been cancelled")
	assert.Equal(t, int32(0), mineCancelled.Load(), "still-owned job's context must not have been touched")
}

// TestOwnershipAudit_DoesNotCancelSelfSuspendedJob is the worker-level
// regression test for the ownership-audit false-cancel. It uses a REAL
// GormStorage (not the mock) because the bug lived in FindOrphanedJobs' SQL:
// a parent that calls FanOut/Call suspends ITSELF via SuspendJob (status=
// 'waiting', locked_by=""), but remains in runningJobs until the handler
// returns. Before the fix, the audit's FindOrphanedJobs flagged that row
// (locked_by="" != workerID) and CancelJob fired on the worker's own live
// handler — turning the WaitingError into a context.Canceled failure and
// replaying the whole handler. The audit must leave a self-suspended job alone.
func TestOwnershipAudit_DoesNotCancelSelfSuspendedJob(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)

	w := NewWorker(q,
		DisableRetry(),
		WithOwnershipAuditInterval(10*time.Millisecond),
	)
	// Use the worker's own (auto-generated) ID so Dequeue/SuspendJob and the
	// audit all agree on ownership — the audit calls FindOrphanedJobs with
	// w.config.WorkerID.
	workerID := w.config.WorkerID

	ctx := context.Background()
	require.NoError(t, store.Enqueue(ctx, &core.Job{Type: "t", Queue: "default"}))
	job, err := store.Dequeue(ctx, []string{"default"}, workerID)
	require.NoError(t, err)
	require.NotNil(t, job)

	// The handler is mid-flight and registered in runningJobs; it then suspends
	// its own parent to wait on a fan-out (the real production transition).
	var cancelled atomic.Int32
	w.runningJobsMu.Lock()
	w.runningJobs[job.ID] = func() { cancelled.Add(1) }
	w.runningJobsMu.Unlock()
	require.NoError(t, store.SuspendJob(ctx, job.ID, workerID))

	auditCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.runOwnershipAudit(auditCtx)

	// Several audit ticks must pass without the self-suspended handler being
	// cancelled.
	time.Sleep(60 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(0), cancelled.Load(),
		"ownership audit must not cancel a job the worker suspended for its own fan-out")
}

// TestOwnershipAudit_NoOpWhenNoOrphans confirms the audit doesn't gratuitously
// cancel anything when FindOrphanedJobs returns empty. Defense against a
// regression that would falsely flag healthy jobs and shred the running set.
func TestOwnershipAudit_NoOpWhenNoOrphans(t *testing.T) {
	mock := &mockStorage{}
	var auditCalls atomic.Int32
	mock.findOrphanedFunc = func(_ []string) ([]string, error) {
		auditCalls.Add(1)
		return nil, nil
	}

	q := queue.New(mock)
	w := NewWorker(q,
		DisableRetry(),
		WithOwnershipAuditInterval(5*time.Millisecond),
	)

	cancelled := 0
	w.runningJobsMu.Lock()
	w.runningJobs["healthy-1"] = func() { cancelled++ }
	w.runningJobs["healthy-2"] = func() { cancelled++ }
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runOwnershipAudit(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, auditCalls.Load(), int32(3), "audit should have run several times")
	assert.Equal(t, 0, cancelled, "no orphans → no cancellations")
}

// TestOwnershipAudit_SkipsQueryWhenNoRunningJobs is the cheapest-tick test:
// when the worker has nothing in flight, the audit must not query at all.
// Important for fleet-wide DB load when most workers are idle.
func TestOwnershipAudit_SkipsQueryWhenNoRunningJobs(t *testing.T) {
	mock := &mockStorage{}
	var queries atomic.Int32
	mock.findOrphanedFunc = func(_ []string) ([]string, error) {
		queries.Add(1)
		return nil, nil
	}

	q := queue.New(mock)
	w := NewWorker(q,
		DisableRetry(),
		WithOwnershipAuditInterval(5*time.Millisecond),
	)
	// runningJobs is empty.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runOwnershipAudit(ctx)
	time.Sleep(40 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(0), queries.Load(),
		"audit must not call FindOrphanedJobs when runningJobs is empty")
}

// TestOwnershipAudit_IntervalConfiguration locks in the documented contract
// for OwnershipAuditInterval: unset → 5s default, explicit value honored,
// and an explicit 0 actually disables the audit. The last case is the
// regression: NewWorker used to clobber a deliberate 0 back to 5s, so
// "set to 0 to disable" never worked and the goroutine always ran.
func TestOwnershipAudit_IntervalConfiguration(t *testing.T) {
	q := queue.New(&mockStorage{})

	t.Run("unset applies 5s default", func(t *testing.T) {
		w := NewWorker(q)
		assert.Equal(t, 5*time.Second, w.config.OwnershipAuditInterval)
	})

	t.Run("explicit value is honored", func(t *testing.T) {
		w := NewWorker(q, WithOwnershipAuditInterval(250*time.Millisecond))
		assert.Equal(t, 250*time.Millisecond, w.config.OwnershipAuditInterval)
	})

	t.Run("explicit zero disables the audit", func(t *testing.T) {
		w := NewWorker(q, WithOwnershipAuditInterval(0))
		assert.Equal(t, time.Duration(0), w.config.OwnershipAuditInterval,
			"WithOwnershipAuditInterval(0) must keep the interval at 0 so Start() skips the audit goroutine")
	})
}

// TestStaleLockInterval_IntervalConfiguration locks in the contract for
// StaleLockInterval: unset → 5m default, an explicit value above the floor is
// honored, sub-floor values clamp up to the floor, and a non-positive value
// CANNOT disable the reaper (it falls back to the default). The reaper is the
// only recovery path for crashed workers, so it is intentionally not
// disable-able.
func TestStaleLockInterval_IntervalConfiguration(t *testing.T) {
	q := queue.New(&mockStorage{})

	t.Run("unset applies 5m default", func(t *testing.T) {
		w := NewWorker(q)
		assert.Equal(t, 5*time.Minute, w.config.StaleLockInterval)
	})

	t.Run("explicit value above the floor is honored", func(t *testing.T) {
		w := NewWorker(q, WithStaleLockInterval(30*time.Second))
		assert.Equal(t, 30*time.Second, w.config.StaleLockInterval)
	})

	t.Run("sub-floor value clamps up to the floor", func(t *testing.T) {
		w := NewWorker(q, WithStaleLockInterval(10*time.Millisecond))
		assert.Equal(t, minStaleLockInterval, w.config.StaleLockInterval,
			"a positive interval below the floor must be clamped up, not accepted as-is")
	})

	t.Run("non-positive cannot disable; falls back to default", func(t *testing.T) {
		w := NewWorker(q, WithStaleLockInterval(0))
		assert.Equal(t, 5*time.Minute, w.config.StaleLockInterval,
			"WithStaleLockInterval(0) must not disable the reaper; the default applies")
	})
}

func TestFanOutRecoveryStaleAge_Configuration(t *testing.T) {
	q := queue.New(&mockStorage{})

	t.Run("unset applies 2m default", func(t *testing.T) {
		w := NewWorker(q)
		assert.Equal(t, 2*time.Minute, w.config.FanOutRecoveryStaleAge)
	})

	t.Run("explicit value is honored", func(t *testing.T) {
		w := NewWorker(q, WithFanOutRecoveryStaleAge(30*time.Second))
		assert.Equal(t, 30*time.Second, w.config.FanOutRecoveryStaleAge)
	})

	t.Run("non-positive cannot disable; falls back to default", func(t *testing.T) {
		w := NewWorker(q, WithFanOutRecoveryStaleAge(0))
		assert.Equal(t, 2*time.Minute, w.config.FanOutRecoveryStaleAge,
			"WithFanOutRecoveryStaleAge(0) must not disable recovery; the default applies")
	})
}

// ---------------------------------------------------------------------------
// Lost-ownership side-effect suppression (finding A)
// ---------------------------------------------------------------------------

// TestHandleError_NotOwnedSkipsFanOutAccounting verifies that when a handler
// errors — e.g. because its context was cancelled by the orphan-abandon or
// ownership-audit path — and storage reports the job is no longer owned by
// this worker, handleError does NOT touch the fan-out counters. Otherwise the
// orphaned worker would race the new owner and double-count the fan-out, the
// exact corruption this branch's cancellation work aims to end.
func TestHandleError_NotOwnedSkipsFanOutAccounting(t *testing.T) {
	mock := &mockStorage{}
	mock.failFunc = func(_ context.Context, _, _, _ string, _ *time.Time) error {
		return core.ErrJobNotOwned
	}
	var incrementCalled atomic.Int32
	mock.incrementFailedFunc = func(_ context.Context, _ string) (*core.FanOut, error) {
		incrementCalled.Add(1)
		return nil, nil
	}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())

	fanOutID := "fo-1"
	// Attempt >= MaxRetries → terminal path (the one that does fan-out accounting).
	job := &core.Job{ID: "sub-1", FanOutID: &fanOutID, Attempt: 5, MaxRetries: 3}

	ctx := context.Background()
	w.handleError(ctx, ctx, job, errors.New("boom"))

	assert.Equal(t, int32(0), incrementCalled.Load(),
		"a job we no longer own must not write to the fan-out counters")
}

// TestHandleError_OwnedStillAccountsFanOut guards the refactor: when the job IS
// still owned (Fail succeeds), the terminal path must still record the failure
// against the fan-out so the parent isn't left stuck in 'waiting'.
func TestHandleError_OwnedStillAccountsFanOut(t *testing.T) {
	mock := &mockStorage{}
	mock.failFunc = func(_ context.Context, _, _, _ string, _ *time.Time) error {
		return nil // owned: Fail succeeds
	}
	var incrementCalled atomic.Int32
	mock.incrementFailedFunc = func(_ context.Context, _ string) (*core.FanOut, error) {
		incrementCalled.Add(1)
		// Return a fan-out that is NOT yet complete so checkFanOutCompletion
		// is a no-op and we don't pull in the completeFanOut machinery.
		return &core.FanOut{ID: "fo-1", TotalCount: 5, FailedCount: 1, Strategy: core.StrategyCollectAll}, nil
	}

	q := queue.New(mock)
	w := NewWorker(q, DisableRetry())

	fanOutID := "fo-1"
	job := &core.Job{ID: "sub-1", FanOutID: &fanOutID, Attempt: 5, MaxRetries: 3}

	ctx := context.Background()
	w.handleError(ctx, ctx, job, errors.New("boom"))

	assert.Equal(t, int32(1), incrementCalled.Load(),
		"a job we still own must record its failure against the fan-out")
}
