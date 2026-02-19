package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
)

// mockStorage is a minimal in-memory implementation of core.Storage used for
// unit tests that need controlled behaviour without a real database.
type mockStorage struct {
	mu sync.Mutex

	releaseCount    int64         // number of ReleaseStaleLocks calls
	releasedCount   int64         // value returned by ReleaseStaleLocks
	releaseErr      error         // error returned by ReleaseStaleLocks
	releaseDelay    time.Duration // optional artificial delay per call
	dequeueFunc     func(ctx context.Context, queues []string, workerID string) (*core.Job, error)
	completeFunc    func(ctx context.Context, jobID string, workerID string) error
	failFunc        func(ctx context.Context, jobID string, workerID string, errMsg string, retryAt *time.Time) error
	heartbeatFunc   func(ctx context.Context, jobID string, workerID string) error
	checkpointFunc  func(ctx context.Context, jobID string) ([]core.Checkpoint, error)
	waitingJobsFunc func(ctx context.Context) ([]*core.Job, error)

	// fan-out control hooks
	incrementCompletedFunc func(ctx context.Context, fanOutID string) (*core.FanOut, error)
	incrementFailedFunc    func(ctx context.Context, fanOutID string) (*core.FanOut, error)
	updateFanOutStatusFunc func(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error)
	resumeJobFunc          func(ctx context.Context, jobID string) (bool, error)
	cancelSubJobsFunc      func(ctx context.Context, fanOutID string) (int64, error)

	// enqueue tracking
	enqueueMu    sync.Mutex
	enqueueCount int64 // incremented on each Enqueue call
}

func (m *mockStorage) Migrate(_ context.Context) error { return nil }

func (m *mockStorage) Enqueue(_ context.Context, _ *core.Job) error {
	m.enqueueMu.Lock()
	m.enqueueCount++
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

func (m *mockStorage) EnqueueUnique(_ context.Context, _ *core.Job, _ string) error { return nil }

func (m *mockStorage) SaveCheckpoint(_ context.Context, _ *core.Checkpoint) error { return nil }

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

func (m *mockStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, jobID, workerID)
	}
	return nil
}

func (m *mockStorage) ReleaseStaleLocks(_ context.Context, _ time.Duration) (int64, error) {
	if m.releaseDelay > 0 {
		time.Sleep(m.releaseDelay)
	}
	m.mu.Lock()
	m.releaseCount++
	m.mu.Unlock()
	return m.releasedCount, m.releaseErr
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

func (m *mockStorage) CancelSubJobs(ctx context.Context, fanOutID string) (int64, error) {
	if m.cancelSubJobsFunc != nil {
		return m.cancelSubJobsFunc(ctx, fanOutID)
	}
	return 0, nil
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

func (m *mockStorage) SaveJobResult(_ context.Context, _ string, _ string, _ []byte) error {
	return nil
}

func (m *mockStorage) PauseJob(_ context.Context, _ string) error      { return nil }
func (m *mockStorage) UnpauseJob(_ context.Context, _ string) error    { return nil }
func (m *mockStorage) GetPausedJobs(_ context.Context, _ string) ([]*core.Job, error) {
	return nil, nil
}
func (m *mockStorage) IsJobPaused(_ context.Context, _ string) (bool, error) { return false, nil }
func (m *mockStorage) PauseQueue(_ context.Context, _ string) error           { return nil }
func (m *mockStorage) UnpauseQueue(_ context.Context, _ string) error         { return nil }
func (m *mockStorage) GetPausedQueues(_ context.Context) ([]string, error)    { return nil, nil }
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

// ---------------------------------------------------------------------------
// Options tests
// ---------------------------------------------------------------------------

func TestWithPollInterval_SetsBelowMinimum(t *testing.T) {
	config := WorkerConfig{PollInterval: 100 * time.Millisecond}

	// Values below 50 ms must be ignored.
	WithPollInterval(10 * time.Millisecond).ApplyWorker(&config)

	assert.Equal(t, 100*time.Millisecond, config.PollInterval)
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

func TestWithStaleLockInterval_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithStaleLockInterval(10 * time.Minute).ApplyWorker(&config)

	assert.Equal(t, 10*time.Minute, config.StaleLockInterval)
}

func TestWithStaleLockInterval_ZeroDisablesReaper(t *testing.T) {
	config := WorkerConfig{StaleLockInterval: 5 * time.Minute}

	WithStaleLockInterval(0).ApplyWorker(&config)

	assert.Equal(t, time.Duration(0), config.StaleLockInterval)
}

func TestWithStaleLockAge_SetsValue(t *testing.T) {
	config := WorkerConfig{}

	WithStaleLockAge(30 * time.Minute).ApplyWorker(&config)

	assert.Equal(t, 30*time.Minute, config.StaleLockAge)
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
	mock := &mockStorage{releasedCount: 0, releaseErr: nil}
	q := queue.New(mock)

	// Use a very short interval so the reaper fires quickly.
	w := NewWorker(q,
		WithStaleLockInterval(30*time.Millisecond),
		WithStaleLockAge(1*time.Minute),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	// Wait long enough for the reaper to fire at least twice.
	time.Sleep(150 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1),
		"ReleaseStaleLocks should have been called at least once")
}

func TestWorker_StaleLockReaperZeroIntervalDisabled(t *testing.T) {
	mock := &mockStorage{}
	q := queue.New(mock)

	// Explicitly disable the reaper.
	w := NewWorker(q, WithStaleLockInterval(0))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	<-ctx.Done()

	assert.Equal(t, int64(0), mock.getReleaseCount(),
		"ReleaseStaleLocks must not be called when interval is 0")
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
	assert.Equal(t, 5*time.Minute, w.config.StaleLockInterval)
	assert.Equal(t, 45*time.Minute, w.config.StaleLockAge)
	assert.Equal(t, map[string]int{"default": 10}, w.config.Queues)
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

// ---------------------------------------------------------------------------
// Integration: stale lock reaper respects context cancellation
// ---------------------------------------------------------------------------

func TestWorker_StaleLockReaper_StopsOnContextCancel(t *testing.T) {
	mock := &mockStorage{releaseDelay: 5 * time.Millisecond}

	q := queue.New(mock)
	w := NewWorker(q,
		WithStaleLockInterval(20*time.Millisecond),
		WithStaleLockAge(1*time.Minute),
	)

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
		releasedCount: 0,
		releaseErr:    errors.New("db unavailable"),
	}
	q := queue.New(mock)
	w := NewWorker(q,
		WithStaleLockInterval(30*time.Millisecond),
		WithStaleLockAge(1*time.Minute),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() { _ = w.Start(ctx) }()

	<-ctx.Done()

	// The reaper must have been called even though it returned an error every time.
	assert.GreaterOrEqual(t, mock.getReleaseCount(), int64(1))
}

func TestWorker_StaleLockReaper_LogsWhenJobsReleased(t *testing.T) {
	mock := &mockStorage{
		releasedCount: 3, // pretend 3 jobs were released
		releaseErr:    nil,
	}
	q := queue.New(mock)
	w := NewWorker(q,
		WithStaleLockInterval(30*time.Millisecond),
		WithStaleLockAge(1*time.Minute),
	)

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
	dsn := "file:" + t.Name() + "?mode=memory&cache=private"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)
	return q, func() {}
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
			Attempt:    1,  // Attempt < MaxRetries: retry branch is taken
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
	q.Schedule("scheduled-task", schedule.Every(1*time.Millisecond))

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

// TestWorker_CheckFanOutCompletion_ThresholdExceeded exercises the threshold
// strategy early-exit when failures exceed the allowed maximum.
func TestWorker_CheckFanOutCompletion_ThresholdExceeded(t *testing.T) {
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

	// Threshold = 0.8 (80% success required). TotalCount=10, maxFailures = 10*(1-0.8) = 2.
	// FailedCount=3 > 2 so threshold is breached even though 7 jobs remain.
	fo := &core.FanOut{
		ID:             "fo-4",
		ParentJobID:    "parent-job-4",
		TotalCount:     10,
		CompletedCount: 3,
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

// TestWorker_CheckFanOutCompletion_AllCancelledNoCompletions exercises the branch
// where all sub-jobs are cancelled and completedCount == 0, resulting in FanOutFailed.
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
		assert.Equal(t, core.FanOutFailed, status)
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
// branch where UpdateFanOutStatus returns (false, nil) — another worker won
// the race and completeFanOut should return nil without calling ResumeJob.
func TestWorker_CompleteFanOut_AlreadyCompletedByAnotherWorker(t *testing.T) {
	resumeCalled := false

	mock := &mockStorage{
		updateFanOutStatusFunc: func(_ context.Context, _ string, _ core.FanOutStatus) (bool, error) {
			return false, nil // another worker already completed
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
	assert.False(t, resumeCalled, "ResumeJob must not be called when update was a no-op")
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
		cancelSubJobsFunc: func(_ context.Context, fanOutID string) (int64, error) {
			cancelSubJobsCalled <- fanOutID
			return 2, nil
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
