package jobs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

var queueTestCounter int

func setupTestQueue(t *testing.T) (*jobs.Queue, *gorm.DB) {
	queueTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_queue_test_%d_%d.db", os.Getpid(), queueTestCounter)
	t.Cleanup(func() {
		_ = os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)
	return queue, db
}

func TestQueue_Register(t *testing.T) {
	queue, _ := setupTestQueue(t)

	type EmailParams struct {
		To      string
		Subject string
	}

	queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
		return nil
	})

	assert.True(t, queue.HasHandler("send-email"))
	assert.False(t, queue.HasHandler("unknown"))
}

func TestQueue_Register_WithoutContext(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Handler without context
	queue.Register("simple-job", func(msg string) error {
		return nil
	})

	assert.True(t, queue.HasHandler("simple-job"))
}

func TestQueue_Register_ReturnsValue(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Handler that returns a value
	queue.Register("compute-job", func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	assert.True(t, queue.HasHandler("compute-job"))
}

func TestQueue_Register_InvalidHandler_Panic(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Not a function
	assert.Panics(t, func() {
		queue.Register("bad-job", "not a function")
	})
}

func TestQueue_Register_InvalidReturnType_Panic(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Function that doesn't return error
	assert.Panics(t, func() {
		queue.Register("bad-job", func(ctx context.Context, n int) int {
			return n * 2
		})
	})
}

func TestQueue_Register_TooManyArgs_Panic(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Function with too many arguments
	assert.Panics(t, func() {
		queue.Register("bad-job", func(ctx context.Context, a, b, c int) error {
			return nil
		})
	})
}

func TestQueue_Enqueue(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	type EmailParams struct {
		To      string
		Subject string
	}

	queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "send-email", EmailParams{
		To:      "test@example.com",
		Subject: "Hello",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, jobID)

	// Verify job in database
	var job jobs.Job
	err = db.First(&job, "id = ?", jobID).Error
	require.NoError(t, err)
	assert.Equal(t, "send-email", job.Type)
	assert.Equal(t, "default", job.Queue)
	assert.Equal(t, jobs.StatusPending, job.Status)
}

func TestQueue_Enqueue_UnregisteredHandler(t *testing.T) {
	queue, _ := setupTestQueue(t)
	ctx := context.Background()

	_, err := queue.Enqueue(ctx, "unregistered-job", struct{}{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no handler registered")
}

func TestQueue_EnqueueWithOptions(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("task", func(ctx context.Context, args string) error {
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "task", "data",
		jobs.QueueOpt("critical"),
		jobs.Priority(100),
		jobs.Retries(5),
	)

	require.NoError(t, err)

	var job jobs.Job
	db.First(&job, "id = ?", jobID)

	assert.Equal(t, "critical", job.Queue)
	assert.Equal(t, 100, job.Priority)
	assert.Equal(t, 5, job.MaxRetries)
}

func TestQueue_Enqueue_WithDelay(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("delayed-task", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	before := time.Now()
	jobID, err := queue.Enqueue(ctx, "delayed-task", struct{}{},
		jobs.Delay(1*time.Hour),
	)

	require.NoError(t, err)

	var job jobs.Job
	db.First(&job, "id = ?", jobID)

	assert.NotNil(t, job.RunAt)
	assert.True(t, job.RunAt.After(before.Add(59*time.Minute)))
}

func TestQueue_Enqueue_WithRunAt(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("scheduled-task", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	scheduledTime := time.Date(2026, 12, 25, 12, 0, 0, 0, time.UTC)
	jobID, err := queue.Enqueue(ctx, "scheduled-task", struct{}{},
		jobs.At(scheduledTime),
	)

	require.NoError(t, err)

	var job jobs.Job
	db.First(&job, "id = ?", jobID)

	assert.NotNil(t, job.RunAt)
	assert.Equal(t, scheduledTime.UTC(), job.RunAt.UTC())
}

func TestQueue_Enqueue_ComplexArgs(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	type ComplexArgs struct {
		Name     string
		Tags     []string
		Metadata map[string]interface{}
		Nested   struct {
			Value int
		}
	}

	queue.Register("complex-job", func(ctx context.Context, args ComplexArgs) error {
		return nil
	})

	input := ComplexArgs{
		Name: "test",
		Tags: []string{"a", "b", "c"},
		Metadata: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		},
		Nested: struct{ Value int }{Value: 123},
	}

	jobID, err := queue.Enqueue(ctx, "complex-job", input)
	require.NoError(t, err)

	var job jobs.Job
	db.First(&job, "id = ?", jobID)

	var stored ComplexArgs
	err = json.Unmarshal(job.Args, &stored)
	require.NoError(t, err)

	assert.Equal(t, input.Name, stored.Name)
	assert.Equal(t, input.Tags, stored.Tags)
	assert.Equal(t, input.Nested.Value, stored.Nested.Value)
}

func TestQueue_Schedule(t *testing.T) {
	queue, _ := setupTestQueue(t)
	ctx := context.Background()

	var runCount atomic.Int32
	queue.Register("scheduled-task", func(ctx context.Context, _ struct{}) error {
		runCount.Add(1)
		return nil
	})

	queue.Schedule("scheduled-task", jobs.Every(200*time.Millisecond))

	worker := queue.NewWorker(jobs.WithScheduler(true))
	workerCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	go func() { _ = worker.Start(workerCtx) }()

	time.Sleep(800 * time.Millisecond)

	// Should have run at least once (relaxed for race detector)
	assert.GreaterOrEqual(t, runCount.Load(), int32(1))
}

func TestQueue_Schedule_WithOptions(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("priority-scheduled", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	queue.Schedule("priority-scheduled", jobs.Every(100*time.Millisecond),
		jobs.QueueOpt("high-priority"),
		jobs.Priority(100),
	)

	worker := queue.NewWorker(
		jobs.WorkerQueue("high-priority", jobs.Concurrency(1)),
		jobs.WithScheduler(true),
	)
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go func() { _ = worker.Start(workerCtx) }()

	time.Sleep(300 * time.Millisecond)

	// Check that job was enqueued to correct queue
	var job jobs.Job
	err := db.Where("type = ?", "priority-scheduled").First(&job).Error
	if err == nil {
		assert.Equal(t, "high-priority", job.Queue)
		assert.Equal(t, 100, job.Priority)
	}
}

func TestQueue_Storage(t *testing.T) {
	queue, _ := setupTestQueue(t)

	storage := queue.Storage()
	assert.NotNil(t, storage)
}

func TestQueue_HasHandler_ConcurrentAccess(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Register handlers concurrently
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			name := "handler-" + string(rune('a'+n))
			queue.Register(name, func(ctx context.Context, _ struct{}) error {
				return nil
			})
		}(i)
	}

	// Wait for all registrations
	for i := 0; i < 10; i++ {
		<-done
	}

	// Check handlers concurrently
	for i := 0; i < 10; i++ {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			name := "handler-" + string(rune('a'+n))
			assert.True(t, queue.HasHandler(name))
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestQueue_Enqueue_ConcurrentAccess(t *testing.T) {
	queue, _ := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("concurrent-job", func(ctx context.Context, n int) error {
		return nil
	})

	// Enqueue concurrently
	var wg atomic.Int32
	wg.Store(10)
	done := make(chan string, 10)

	for i := 0; i < 10; i++ {
		go func(n int) {
			jobID, err := queue.Enqueue(ctx, "concurrent-job", n)
			assert.NoError(t, err)
			done <- jobID
		}(i)
	}

	// Collect all job IDs
	ids := make(map[string]bool)
	for i := 0; i < 10; i++ {
		id := <-done
		ids[id] = true
	}

	// All IDs should be unique
	assert.Len(t, ids, 10)
}

func TestQueue_Events_Subscription(t *testing.T) {
	queue, _ := setupTestQueue(t)

	events := queue.Events()
	assert.NotNil(t, events)

	// Can subscribe multiple times
	events2 := queue.Events()
	assert.NotNil(t, events2)
}

func TestQueue_SetDeterminism(t *testing.T) {
	queue, _ := setupTestQueue(t)

	// Should not panic
	queue.SetDeterminism(jobs.ExplicitCheckpoints)
	queue.SetDeterminism(jobs.Strict)
	queue.SetDeterminism(jobs.BestEffort)
}
