package jobs_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var facadeTestCounter int

func setupFacadeTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	facadeTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_facade_test_%d_%d.db", os.Getpid(), facadeTestCounter)
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	q := jobs.New(store)
	return q, store
}

// TestFacade_WorkerFactoryInitialized verifies that importing the jobs package
// properly initializes the WorkerFactory in the queue package.
func TestFacade_WorkerFactoryInitialized(t *testing.T) {
	// The queue.WorkerFactory should be set by jobs package init()
	assert.NotNil(t, queue.WorkerFactory, "WorkerFactory should be initialized by facade init()")
}

// TestFacade_NewWorkerFromQueue tests that queue.NewWorker() works via the factory.
func TestFacade_NewWorkerFromQueue(t *testing.T) {
	q, _ := setupFacadeTestQueue(t)

	// This should not panic - the factory should be initialized
	worker := q.NewWorker()
	require.NotNil(t, worker)
}

// TestFacade_NewWorkerWithOptions tests that options are passed through the factory.
func TestFacade_NewWorkerWithOptions(t *testing.T) {
	q, _ := setupFacadeTestQueue(t)

	// Create worker with options
	worker := q.NewWorker(
		jobs.WorkerQueue("high", jobs.Concurrency(5)),
		jobs.WithScheduler(true),
	)
	require.NotNil(t, worker)
}

// TestFacade_DirectWorkerCreation tests jobs.NewWorker() directly.
func TestFacade_DirectWorkerCreation(t *testing.T) {
	q, _ := setupFacadeTestQueue(t)

	// Using the facade's NewWorker function directly
	worker := jobs.NewWorker(q, jobs.Concurrency(3))
	require.NotNil(t, worker)
}

// TestFacade_WorkerProcessesJob verifies the worker created via factory actually works.
func TestFacade_WorkerProcessesJob(t *testing.T) {
	q, _ := setupFacadeTestQueue(t)

	var processed atomic.Bool
	q.Register("facade-test-job", func(ctx context.Context, msg string) error {
		processed.Store(true)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := q.Enqueue(ctx, "facade-test-job", "hello")
	require.NoError(t, err)

	// Create worker via queue.NewWorker() which uses the factory
	worker := q.NewWorker()
	go worker.Start(ctx)

	// Wait for processing
	for i := 0; i < 50; i++ {
		if processed.Load() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, processed.Load(), "Job should be processed by worker created via factory")
}

// TestFacade_TypeAliases verifies that type aliases work correctly.
func TestFacade_TypeAliases(t *testing.T) {
	// Verify that types are properly aliased
	var job *jobs.Job
	assert.Nil(t, job)

	var checkpoint *jobs.Checkpoint
	assert.Nil(t, checkpoint)

	var status jobs.JobStatus = jobs.StatusPending
	assert.Equal(t, jobs.StatusPending, status)

	// Verify constants are accessible
	assert.Equal(t, jobs.StatusPending, jobs.JobStatus("pending"))
	assert.Equal(t, jobs.StatusRunning, jobs.JobStatus("running"))
	assert.Equal(t, jobs.StatusCompleted, jobs.JobStatus("completed"))
	assert.Equal(t, jobs.StatusFailed, jobs.JobStatus("failed"))
}

// TestFacade_ErrorTypes verifies error types are properly exported.
func TestFacade_ErrorTypes(t *testing.T) {
	assert.NotNil(t, jobs.ErrInvalidJobTypeName)
	assert.NotNil(t, jobs.ErrJobTypeNameTooLong)
	assert.NotNil(t, jobs.ErrInvalidQueueName)
	assert.NotNil(t, jobs.ErrQueueNameTooLong)
	assert.NotNil(t, jobs.ErrJobArgsTooLarge)
	assert.NotNil(t, jobs.ErrJobNotOwned)
	assert.NotNil(t, jobs.ErrDuplicateJob)
}

// TestFacade_SecurityConstants verifies security constants are exported.
func TestFacade_SecurityConstants(t *testing.T) {
	assert.Greater(t, jobs.MaxJobTypeNameLength, 0)
	assert.Greater(t, jobs.MaxJobArgsSize, 0)
	assert.Greater(t, jobs.MaxRetries, 0)
	assert.Greater(t, jobs.MaxConcurrency, 0)
	assert.Greater(t, jobs.MaxErrorMessageLength, 0)
	assert.Greater(t, jobs.MaxQueueNameLength, 0)
	assert.Greater(t, jobs.MaxUniqueKeyLength, 0)
}

// TestFacade_HelperFunctions verifies helper functions are exported.
func TestFacade_HelperFunctions(t *testing.T) {
	// ValidateJobTypeName
	assert.NoError(t, jobs.ValidateJobTypeName("valid-name"))
	assert.Error(t, jobs.ValidateJobTypeName(""))

	// ValidateQueueName
	assert.NoError(t, jobs.ValidateQueueName("valid-queue"))
	assert.Error(t, jobs.ValidateQueueName(""))

	// SanitizeErrorMessage
	sanitized := jobs.SanitizeErrorMessage("test error")
	assert.NotEmpty(t, sanitized)

	// ClampRetries
	assert.Equal(t, 5, jobs.ClampRetries(5))
	assert.Equal(t, jobs.MaxRetries, jobs.ClampRetries(1000))

	// ClampConcurrency
	assert.Equal(t, 5, jobs.ClampConcurrency(5))
	assert.Equal(t, jobs.MaxConcurrency, jobs.ClampConcurrency(10000))
}

// TestFacade_OptionFunctions verifies option functions are exported.
func TestFacade_OptionFunctions(t *testing.T) {
	// These should not panic - just verify they're callable
	_ = jobs.QueueOpt("test")
	_ = jobs.Priority(10)
	_ = jobs.Retries(5)
	_ = jobs.Delay(time.Second)
	_ = jobs.At(time.Now())
	_ = jobs.Unique("key")
	_ = jobs.Determinism(jobs.Strict)

	// Worker options
	_ = jobs.Concurrency(5)
	_ = jobs.WithScheduler(true)
	_ = jobs.WorkerQueue("test", jobs.Concurrency(3))
}

// TestFacade_ScheduleFunctions verifies schedule functions are exported.
func TestFacade_ScheduleFunctions(t *testing.T) {
	// These should not panic - just verify they're callable
	_ = jobs.Every(time.Minute)
	_ = jobs.Daily(9, 0)
	_ = jobs.Weekly(time.Monday, 9, 0)
	_ = jobs.Cron("* * * * *")
}
