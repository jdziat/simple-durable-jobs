package jobs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// setupTestStorage creates an in-memory SQLite storage for use in tests.
func setupTestStorage(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&_busy_timeout=5000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))

	q := jobs.New(store)
	return q, store
}

// ---------------------------------------------------------------------------
// TestFacadeNew - core queue and storage construction
// ---------------------------------------------------------------------------

func TestFacadeNew_CreatesQueue(t *testing.T) {
	q, _ := setupTestStorage(t)
	assert.NotNil(t, q)
}

func TestFacadeNew_NewGormStorage(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	assert.NotNil(t, store)
}

func TestFacadeNew_RegisterEnqueueGetJob(t *testing.T) {
	q, store := setupTestStorage(t)
	ctx := context.Background()

	q.Register("facade-roundtrip", func(_ context.Context, msg string) error {
		return nil
	})

	id, err := q.Enqueue(ctx, "facade-roundtrip", "hello")
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	job, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, id, job.ID)
	assert.Equal(t, "facade-roundtrip", job.Type)
}

// ---------------------------------------------------------------------------
// TestFacadeJobOptions - Option builders return non-nil options and apply correctly
// ---------------------------------------------------------------------------

func TestFacadeJobOptions_AllReturnNonNil(t *testing.T) {
	assert.NotNil(t, jobs.QueueOpt("high"))
	assert.NotNil(t, jobs.Priority(5))
	assert.NotNil(t, jobs.Retries(3))
	assert.NotNil(t, jobs.Delay(time.Second))
	assert.NotNil(t, jobs.At(time.Now().Add(time.Minute)))
	assert.NotNil(t, jobs.Unique("my-key"))
}

func TestFacadeJobOptions_EnqueueWithOptions(t *testing.T) {
	q, store := setupTestStorage(t)
	ctx := context.Background()

	q.Register("facade-opts-job", func(_ context.Context, n int) error {
		return nil
	})

	runAt := time.Now().Add(10 * time.Minute)
	id, err := q.Enqueue(ctx, "facade-opts-job", 42,
		jobs.QueueOpt("default"),
		jobs.Priority(3),
		jobs.Retries(5),
		jobs.At(runAt),
		jobs.Unique("opts-test-key"),
	)
	require.NoError(t, err)

	job, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, 3, job.Priority)
	assert.Equal(t, 5, job.MaxRetries)
	assert.Equal(t, "opts-test-key", job.UniqueKey)
}

func TestFacadeJobOptions_DelayOption(t *testing.T) {
	q, store := setupTestStorage(t)
	ctx := context.Background()

	q.Register("facade-delay-job", func(_ context.Context, _ string) error {
		return nil
	})

	before := time.Now()
	id, err := q.Enqueue(ctx, "facade-delay-job", "x", jobs.Delay(5*time.Minute))
	require.NoError(t, err)

	job, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job)
	// RunAt should be after now
	assert.True(t, job.RunAt.After(before))
}

// ---------------------------------------------------------------------------
// TestFacadeDeterminism - Determinism option is non-nil and applies correctly
// ---------------------------------------------------------------------------

func TestFacadeDeterminism_NonNil(t *testing.T) {
	assert.NotNil(t, jobs.Determinism(jobs.ExplicitCheckpoints))
	assert.NotNil(t, jobs.Determinism(jobs.Strict))
	assert.NotNil(t, jobs.Determinism(jobs.BestEffort))
}

func TestFacadeDeterminism_AppliesMode(t *testing.T) {
	opts := jobs.NewOptions()
	require.NotNil(t, opts)

	opt := jobs.Determinism(jobs.Strict)
	opt.Apply(opts)
	assert.Equal(t, jobs.Strict, opts.Determinism)
}

// ---------------------------------------------------------------------------
// TestFacadeWorkerCreation - Worker constructors and options
// ---------------------------------------------------------------------------

func TestFacadeWorkerCreation_NewWorkerNonNil(t *testing.T) {
	q, _ := setupTestStorage(t)
	w := jobs.NewWorker(q)
	assert.NotNil(t, w)
}

func TestFacadeWorkerCreation_ConcurrencyOption(t *testing.T) {
	opt := jobs.Concurrency(10)
	assert.NotNil(t, opt)
}

func TestFacadeWorkerCreation_WithSchedulerOption(t *testing.T) {
	opt := jobs.WithScheduler(true)
	assert.NotNil(t, opt)
}

func TestFacadeWorkerCreation_WorkerQueueOption(t *testing.T) {
	opt := jobs.WorkerQueue("critical", jobs.Concurrency(4))
	assert.NotNil(t, opt)
}

func TestFacadeWorkerCreation_PollIntervalOption(t *testing.T) {
	opt := jobs.WithPollInterval(200 * time.Millisecond)
	assert.NotNil(t, opt)
}

func TestFacadeWorkerCreation_StaleLockIntervalOption(t *testing.T) {
	opt := jobs.WithStaleLockInterval(10 * time.Minute)
	assert.NotNil(t, opt)
}

func TestFacadeWorkerCreation_StaleLockAgeOption(t *testing.T) {
	opt := jobs.WithStaleLockAge(30 * time.Minute)
	assert.NotNil(t, opt)
}

func TestFacadeWorkerCreation_NewWorkerWithAllOptions(t *testing.T) {
	q, _ := setupTestStorage(t)
	w := jobs.NewWorker(q,
		jobs.Concurrency(2),
		jobs.WithScheduler(false),
		jobs.WorkerQueue("extra", jobs.Concurrency(1)),
		jobs.WithPollInterval(100*time.Millisecond),
		jobs.WithStaleLockInterval(5*time.Minute),
		jobs.WithStaleLockAge(45*time.Minute),
	)
	assert.NotNil(t, w)
}

// ---------------------------------------------------------------------------
// TestFacadeRetryOptions - Retry configuration builders
// ---------------------------------------------------------------------------

func TestFacadeRetryOptions_WithStorageRetry(t *testing.T) {
	cfg := jobs.DefaultRetryConfig()
	opt := jobs.WithStorageRetry(cfg)
	assert.NotNil(t, opt)
}

func TestFacadeRetryOptions_WithDequeueRetry(t *testing.T) {
	cfg := jobs.DefaultRetryConfig()
	opt := jobs.WithDequeueRetry(cfg)
	assert.NotNil(t, opt)
}

func TestFacadeRetryOptions_WithRetryAttempts(t *testing.T) {
	opt := jobs.WithRetryAttempts(3)
	assert.NotNil(t, opt)
}

func TestFacadeRetryOptions_DisableRetry(t *testing.T) {
	opt := jobs.DisableRetry()
	assert.NotNil(t, opt)
}

func TestFacadeRetryOptions_DefaultRetryConfig(t *testing.T) {
	cfg := jobs.DefaultRetryConfig()
	assert.Greater(t, cfg.MaxAttempts, 0)
	assert.Greater(t, cfg.InitialBackoff, time.Duration(0))
	assert.Greater(t, cfg.MaxBackoff, time.Duration(0))
	assert.Greater(t, cfg.BackoffMultiplier, 0.0)
}

// ---------------------------------------------------------------------------
// TestFacadeScheduleBuilders - Schedule constructors return valid schedules
// ---------------------------------------------------------------------------

func TestFacadeScheduleBuilders_Every(t *testing.T) {
	s := jobs.Every(time.Minute)
	require.NotNil(t, s)
	next := s.Next(time.Now())
	assert.True(t, next.After(time.Now()))
}

func TestFacadeScheduleBuilders_Daily(t *testing.T) {
	s := jobs.Daily(9, 0)
	require.NotNil(t, s)
	next := s.Next(time.Now())
	assert.False(t, next.IsZero())
}

func TestFacadeScheduleBuilders_Weekly(t *testing.T) {
	s := jobs.Weekly(time.Monday, 9, 0)
	require.NotNil(t, s)
	next := s.Next(time.Now())
	assert.False(t, next.IsZero())
}

func TestFacadeScheduleBuilders_Cron(t *testing.T) {
	s := jobs.Cron("* * * * *")
	require.NotNil(t, s)
	next := s.Next(time.Now())
	assert.False(t, next.IsZero())
}

// ---------------------------------------------------------------------------
// TestFacadeErrorHelpers - NoRetry and RetryAfter wrappers
// ---------------------------------------------------------------------------

func TestFacadeErrorHelpers_NoRetry(t *testing.T) {
	base := errors.New("something failed")
	wrapped := jobs.NoRetry(base)
	require.NotNil(t, wrapped)
	assert.ErrorContains(t, wrapped, "something failed")

	var nre *jobs.NoRetryError
	assert.True(t, errors.As(wrapped, &nre))
	assert.Equal(t, base, nre.Unwrap())
}

func TestFacadeErrorHelpers_RetryAfter(t *testing.T) {
	base := errors.New("transient error")
	wrapped := jobs.RetryAfter(5*time.Second, base)
	require.NotNil(t, wrapped)
	assert.ErrorContains(t, wrapped, "transient error")

	var rae *jobs.RetryAfterError
	assert.True(t, errors.As(wrapped, &rae))
	assert.Equal(t, 5*time.Second, rae.Delay)
	assert.Equal(t, base, rae.Unwrap())
}

// ---------------------------------------------------------------------------
// TestFacadeSecurityHelpers - Validation and sanitization helpers
// ---------------------------------------------------------------------------

func TestFacadeSecurityHelpers_ValidateJobTypeName(t *testing.T) {
	assert.NoError(t, jobs.ValidateJobTypeName("send-email"))
	assert.NoError(t, jobs.ValidateJobTypeName("processOrder"))
	assert.NoError(t, jobs.ValidateJobTypeName("job.v2"))

	assert.Error(t, jobs.ValidateJobTypeName(""))
	assert.Error(t, jobs.ValidateJobTypeName("123starts-with-digit"))
	assert.Error(t, jobs.ValidateJobTypeName("has space"))

	// Name that is too long
	long := strings.Repeat("a", jobs.MaxJobTypeNameLength+1)
	assert.Error(t, jobs.ValidateJobTypeName(long))
}

func TestFacadeSecurityHelpers_ValidateQueueName(t *testing.T) {
	assert.NoError(t, jobs.ValidateQueueName("default"))
	assert.NoError(t, jobs.ValidateQueueName("high-priority"))

	assert.Error(t, jobs.ValidateQueueName(""))
	assert.Error(t, jobs.ValidateQueueName("has space"))

	// Name that is too long
	long := strings.Repeat("q", jobs.MaxQueueNameLength+1)
	assert.Error(t, jobs.ValidateQueueName(long))
}

func TestFacadeSecurityHelpers_SanitizeErrorMessage(t *testing.T) {
	// Normal message passes through
	msg := jobs.SanitizeErrorMessage("something went wrong")
	assert.Equal(t, "something went wrong", msg)

	// Long message is truncated
	long := strings.Repeat("x", jobs.MaxErrorMessageLength+100)
	truncated := jobs.SanitizeErrorMessage(long)
	assert.LessOrEqual(t, len([]rune(truncated)), jobs.MaxErrorMessageLength)

	// Empty string returns empty
	assert.Equal(t, "", jobs.SanitizeErrorMessage(""))
}

func TestFacadeSecurityHelpers_ClampRetries(t *testing.T) {
	assert.Equal(t, 5, jobs.ClampRetries(5))
	assert.Equal(t, 0, jobs.ClampRetries(-1))
	assert.Equal(t, jobs.MaxRetries, jobs.ClampRetries(jobs.MaxRetries+1))
}

func TestFacadeSecurityHelpers_ClampConcurrency(t *testing.T) {
	assert.Equal(t, 5, jobs.ClampConcurrency(5))
	assert.Equal(t, 1, jobs.ClampConcurrency(0))
	assert.Equal(t, 1, jobs.ClampConcurrency(-5))
	assert.Equal(t, jobs.MaxConcurrency, jobs.ClampConcurrency(jobs.MaxConcurrency+1))
}

// ---------------------------------------------------------------------------
// TestFacadePoolConfigs - Connection pool configuration constructors
// ---------------------------------------------------------------------------

func TestFacadePoolConfigs_Default(t *testing.T) {
	cfg := jobs.DefaultPoolConfig()
	assert.Greater(t, cfg.MaxOpenConns, 0)
	assert.Greater(t, cfg.MaxIdleConns, 0)
	assert.Greater(t, cfg.ConnMaxLifetime, time.Duration(0))
	assert.Greater(t, cfg.ConnMaxIdleTime, time.Duration(0))
}

func TestFacadePoolConfigs_HighConcurrency(t *testing.T) {
	cfg := jobs.HighConcurrencyPoolConfig()
	assert.Greater(t, cfg.MaxOpenConns, 0)
}

func TestFacadePoolConfigs_LowLatency(t *testing.T) {
	cfg := jobs.LowLatencyPoolConfig()
	assert.Greater(t, cfg.MaxOpenConns, 0)
}

func TestFacadePoolConfigs_ResourceConstrained(t *testing.T) {
	cfg := jobs.ResourceConstrainedPoolConfig()
	assert.Greater(t, cfg.MaxOpenConns, 0)
}

func TestFacadePoolConfigs_PoolOptionBuilders(t *testing.T) {
	assert.NotNil(t, jobs.MaxOpenConns(25))
	assert.NotNil(t, jobs.MaxIdleConns(10))
	assert.NotNil(t, jobs.ConnMaxLifetime(5*time.Minute))
	assert.NotNil(t, jobs.ConnMaxIdleTime(time.Minute))
}

func TestFacadePoolConfigs_NewGormStorageWithPool(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store, err := jobs.NewGormStorageWithPool(db,
		jobs.MaxOpenConns(5),
		jobs.MaxIdleConns(2),
	)
	require.NoError(t, err)
	assert.NotNil(t, store)
}

func TestFacadePoolConfigs_ConfigurePool(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	err = jobs.ConfigurePool(db, jobs.MaxOpenConns(10))
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// TestFacadeFanOut - Fan-out helper functions
// ---------------------------------------------------------------------------

func TestFacadeFanOut_SubCreatesSubJob(t *testing.T) {
	sj := jobs.Sub("process-item", 42)
	assert.Equal(t, "process-item", sj.Type)
	assert.Equal(t, 42, sj.Args)
}

func TestFacadeFanOut_SubWithOptions(t *testing.T) {
	sj := jobs.Sub("process-item", "arg",
		jobs.QueueOpt("high"),
		jobs.Priority(10),
		jobs.Retries(4),
	)
	assert.Equal(t, "high", sj.Queue)
	assert.Equal(t, 10, sj.Priority)
	assert.Equal(t, 4, sj.Retries)
}

func TestFacadeFanOut_ValuesExtractsSuccesses(t *testing.T) {
	results := []fanout.Result[int]{
		{Index: 0, Value: 1, Err: nil},
		{Index: 1, Value: 2, Err: errors.New("fail")},
		{Index: 2, Value: 3, Err: nil},
	}
	vals := jobs.Values(results)
	assert.Equal(t, []int{1, 3}, vals)
}

func TestFacadeFanOut_ValuesEmpty(t *testing.T) {
	results := []fanout.Result[string]{}
	vals := jobs.Values(results)
	assert.Empty(t, vals)
}

func TestFacadeFanOut_PartitionSplits(t *testing.T) {
	err1 := errors.New("first failure")
	results := []fanout.Result[int]{
		{Index: 0, Value: 10, Err: nil},
		{Index: 1, Value: 0, Err: err1},
		{Index: 2, Value: 20, Err: nil},
	}
	successes, failures := jobs.Partition(results)
	assert.Equal(t, []int{10, 20}, successes)
	assert.Equal(t, []error{err1}, failures)
}

func TestFacadeFanOut_AllSucceeded(t *testing.T) {
	allGood := []fanout.Result[string]{
		{Index: 0, Value: "a", Err: nil},
		{Index: 1, Value: "b", Err: nil},
	}
	assert.True(t, jobs.AllSucceeded(allGood))

	withFailure := []fanout.Result[string]{
		{Index: 0, Value: "a", Err: nil},
		{Index: 1, Value: "", Err: errors.New("oops")},
	}
	assert.False(t, jobs.AllSucceeded(withFailure))
}

func TestFacadeFanOut_AllSucceededEmpty(t *testing.T) {
	empty := []fanout.Result[int]{}
	assert.True(t, jobs.AllSucceeded(empty))
}

func TestFacadeFanOut_SuccessCount(t *testing.T) {
	results := []fanout.Result[int]{
		{Index: 0, Value: 1, Err: nil},
		{Index: 1, Value: 0, Err: errors.New("bad")},
		{Index: 2, Value: 3, Err: nil},
	}
	assert.Equal(t, 2, jobs.SuccessCount(results))
}

func TestFacadeFanOut_StrategyOptions(t *testing.T) {
	assert.NotNil(t, jobs.FailFast())
	assert.NotNil(t, jobs.CollectAll())
	assert.NotNil(t, jobs.Threshold(0.8))
}

func TestFacadeFanOut_AdditionalFanOutOptions(t *testing.T) {
	assert.NotNil(t, jobs.WithFanOutQueue("batch"))
	assert.NotNil(t, jobs.WithFanOutPriority(5))
	assert.NotNil(t, jobs.WithFanOutRetries(3))
	assert.NotNil(t, jobs.WithFanOutTimeout(30*time.Second))
	assert.NotNil(t, jobs.CancelOnParentFailure())
}

// ---------------------------------------------------------------------------
// TestFacadeContextHelpers - Context extraction helpers
// ---------------------------------------------------------------------------

func TestFacadeContextHelpers_JobFromContextNilWithoutJob(t *testing.T) {
	ctx := context.Background()
	job := jobs.JobFromContext(ctx)
	assert.Nil(t, job)
}

func TestFacadeContextHelpers_JobIDFromContextEmptyWithoutJob(t *testing.T) {
	ctx := context.Background()
	id := jobs.JobIDFromContext(ctx)
	assert.Empty(t, id)
}

// ---------------------------------------------------------------------------
// TestFacadeNewOptions - NewOptions returns defaults
// ---------------------------------------------------------------------------

func TestFacadeNewOptions_ReturnsDefaults(t *testing.T) {
	opts := jobs.NewOptions()
	require.NotNil(t, opts)
	assert.Equal(t, "default", opts.Queue)
	assert.Equal(t, jobs.DefaultJobRetries, opts.MaxRetries)
}

// ---------------------------------------------------------------------------
// TestFacadeConstants - Status and strategy constants are defined
// ---------------------------------------------------------------------------

func TestFacadeConstants_StatusValues(t *testing.T) {
	assert.Equal(t, jobs.JobStatus("pending"), jobs.StatusPending)
	assert.Equal(t, jobs.JobStatus("running"), jobs.StatusRunning)
	assert.Equal(t, jobs.JobStatus("completed"), jobs.StatusCompleted)
	assert.Equal(t, jobs.JobStatus("failed"), jobs.StatusFailed)
	assert.Equal(t, jobs.JobStatus("retrying"), jobs.StatusRetrying)
	assert.Equal(t, jobs.JobStatus("waiting"), jobs.StatusWaiting)
	assert.Equal(t, jobs.JobStatus("cancelled"), jobs.StatusCancelled)
	assert.Equal(t, jobs.JobStatus("paused"), jobs.StatusPaused)
}

func TestFacadeConstants_FanOutStrategyValues(t *testing.T) {
	assert.NotEqual(t, jobs.StrategyFailFast, jobs.StrategyCollectAll)
	assert.NotEqual(t, jobs.StrategyCollectAll, jobs.StrategyThreshold)
}

func TestFacadeConstants_FanOutStatusValues(t *testing.T) {
	assert.NotEqual(t, jobs.FanOutPending, jobs.FanOutCompleted)
	assert.NotEqual(t, jobs.FanOutCompleted, jobs.FanOutFailed)
}

// ---------------------------------------------------------------------------
// TestFacadeIsSuspendError - IsSuspendError recognises non-suspend errors
// ---------------------------------------------------------------------------

func TestFacadeIsSuspendError_ReturnsFalseForRegularError(t *testing.T) {
	err := errors.New("not a suspend")
	assert.False(t, jobs.IsSuspendError(err))
}

func TestFacadeIsSuspendError_ReturnsFalseForNil(t *testing.T) {
	assert.False(t, jobs.IsSuspendError(nil))
}
