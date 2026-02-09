package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var integrationTestCounter int

func setupIntegrationQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	integrationTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_integration_test_%d_%d.db", os.Getpid(), integrationTestCounter)
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

	queue := jobs.New(store)
	return queue, store
}

func TestIntegration_WorkflowWithNestedCalls(t *testing.T) {
	queue, store := setupIntegrationQueue(t)

	// Track execution counts
	var chargeCount, shipCount, notifyCount atomic.Int32

	queue.Register("charge", func(ctx context.Context, amount int) (string, error) {
		chargeCount.Add(1)
		return "receipt-123", nil
	})

	queue.Register("ship", func(ctx context.Context, items []string) error {
		shipCount.Add(1)
		return nil
	})

	queue.Register("notify", func(ctx context.Context, email string) error {
		notifyCount.Add(1)
		return nil
	})

	type Order struct {
		Amount int
		Items  []string
		Email  string
	}

	queue.Register("process-order", func(ctx context.Context, order Order) error {
		receipt, err := jobs.Call[string](ctx, "charge", order.Amount)
		if err != nil {
			return err
		}
		_ = receipt // Use receipt in real code

		if _, err := jobs.Call[any](ctx, "ship", order.Items); err != nil {
			return err
		}

		_, err = jobs.Call[any](ctx, "notify", order.Email)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	order := Order{
		Amount: 100,
		Items:  []string{"item1", "item2"},
		Email:  "test@example.com",
	}

	jobID, err := queue.Enqueue(ctx, "process-order", order)
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	time.Sleep(1 * time.Second)

	assert.Equal(t, int32(1), chargeCount.Load())
	assert.Equal(t, int32(1), shipCount.Load())
	assert.Equal(t, int32(1), notifyCount.Load())

	// Verify job completed
	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusCompleted, job.Status)

	// Verify checkpoints saved
	checkpoints, err := store.GetCheckpoints(context.Background(), jobID)
	require.NoError(t, err)
	assert.Len(t, checkpoints, 3)
}

func TestIntegration_RetryOnFailure(t *testing.T) {
	queue, store := setupIntegrationQueue(t)

	var attempts atomic.Int32
	queue.Register("flaky", func(ctx context.Context, _ struct{}) error {
		count := attempts.Add(1)
		if count < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "flaky", struct{}{}, jobs.Retries(5))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for retries - use a loop instead of sleep
	for i := 0; i < 50; i++ {
		time.Sleep(200 * time.Millisecond)
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(3))

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}

func TestIntegration_Priorities(t *testing.T) {
	queue, _ := setupIntegrationQueue(t)

	var order []string
	var mu sync.Mutex

	queue.Register("task", func(ctx context.Context, name string) error {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()

	// Enqueue in reverse priority order
	_, err := queue.Enqueue(ctx, "task", "low", jobs.Priority(1))
	require.NoError(t, err)
	_, err = queue.Enqueue(ctx, "task", "medium", jobs.Priority(50))
	require.NoError(t, err)
	_, err = queue.Enqueue(ctx, "task", "high", jobs.Priority(100))
	require.NoError(t, err)

	workerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Single worker with concurrency 1 to ensure sequential processing
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(1)))
	go worker.Start(workerCtx)

	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// High priority should run first
	require.GreaterOrEqual(t, len(order), 1)
	assert.Equal(t, "high", order[0])
}

func TestIntegration_MultipleQueues(t *testing.T) {
	queue, _ := setupIntegrationQueue(t)

	var criticalCount, defaultCount atomic.Int32

	queue.Register("critical-task", func(ctx context.Context, _ struct{}) error {
		criticalCount.Add(1)
		return nil
	})

	queue.Register("default-task", func(ctx context.Context, _ struct{}) error {
		defaultCount.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Enqueue to different queues
	_, err := queue.Enqueue(ctx, "critical-task", struct{}{}, jobs.QueueOpt("critical"))
	require.NoError(t, err)
	_, err = queue.Enqueue(ctx, "default-task", struct{}{})
	require.NoError(t, err)

	// Worker only processes "critical" queue
	worker := queue.NewWorker(jobs.WorkerQueue("critical", jobs.Concurrency(1)))
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)

	// Only critical job should have run
	assert.Equal(t, int32(1), criticalCount.Load())
	assert.Equal(t, int32(0), defaultCount.Load())
}

func TestIntegration_HooksAndEvents(t *testing.T) {
	queue, _ := setupIntegrationQueue(t)

	var startCalls, completeCalls atomic.Int32

	queue.OnJobStart(func(ctx context.Context, job *jobs.Job) {
		startCalls.Add(1)
	})

	queue.OnJobComplete(func(ctx context.Context, job *jobs.Job) {
		completeCalls.Add(1)
	})

	queue.Register("simple-task", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "simple-task", struct{}{})
	require.NoError(t, err)

	// Subscribe to events
	events := queue.Events()

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for events
	var startedEvent, completedEvent bool
	timeout := time.After(1 * time.Second)

EventLoop:
	for {
		select {
		case e := <-events:
			switch e.(type) {
			case *jobs.JobStarted:
				startedEvent = true
			case *jobs.JobCompleted:
				completedEvent = true
			}
			if startedEvent && completedEvent {
				break EventLoop
			}
		case <-timeout:
			break EventLoop
		}
	}

	assert.True(t, startedEvent, "Should receive JobStarted event")
	assert.True(t, completedEvent, "Should receive JobCompleted event")
	assert.Equal(t, int32(1), startCalls.Load())
	assert.Equal(t, int32(1), completeCalls.Load())
}

func TestIntegration_DelayedJob(t *testing.T) {
	queue, store := setupIntegrationQueue(t)

	var executed atomic.Bool
	queue.Register("delayed-task", func(ctx context.Context, _ struct{}) error {
		executed.Store(true)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	startTime := time.Now()
	jobID, err := queue.Enqueue(ctx, "delayed-task", struct{}{}, jobs.Delay(500*time.Millisecond))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Should not execute immediately
	time.Sleep(200 * time.Millisecond)
	assert.False(t, executed.Load(), "Job should not have executed yet")

	// Wait for delay to pass
	time.Sleep(600 * time.Millisecond)

	// Check if job completed
	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)

	// Give it more time if still pending
	if job.Status != jobs.StatusCompleted {
		time.Sleep(500 * time.Millisecond)
		job, _ = store.GetJob(context.Background(), jobID)
	}

	assert.True(t, executed.Load(), "Job should have executed")
	if job.Status == jobs.StatusCompleted && job.StartedAt != nil {
		assert.True(t, job.StartedAt.Sub(startTime) >= 400*time.Millisecond, "Job should have run after delay")
	}
}
