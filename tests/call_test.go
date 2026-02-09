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

var callTestCounter int

func setupCallTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	callTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_call_test_%d_%d.db", os.Getpid(), callTestCounter)
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

func TestCall_ExecutesNestedJob(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	var nestedCalled atomic.Bool
	queue.Register("nested", func(ctx context.Context, msg string) (string, error) {
		nestedCalled.Store(true)
		return "result: " + msg, nil
	})

	var parentResult string
	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[string](ctx, "nested", "hello")
		if err != nil {
			return err
		}
		parentResult = result
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "parent", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	var job *jobs.Job
	for i := 0; i < 50; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, nestedCalled.Load())
	assert.Equal(t, "result: hello", parentResult)
}

func TestCall_ReturnsCheckpointedResult(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	var callCount atomic.Int32
	queue.Register("nested", func(ctx context.Context, n int) (int, error) {
		callCount.Add(1)
		return n * 2, nil
	})

	// Simulate pre-existing checkpoint
	jobID := "test-job"
	checkpoint := &jobs.Checkpoint{
		ID:        "cp-1",
		JobID:     jobID,
		CallIndex: 0,
		CallType:  "nested",
		Result:    []byte(`100`),
	}
	err := store.SaveCheckpoint(context.Background(), checkpoint)
	require.NoError(t, err)

	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[int](ctx, "nested", 5)
		require.NoError(t, err)
		assert.Equal(t, 100, result) // Should get checkpointed result
		return nil
	})

	// Create job with pre-existing checkpoints
	job := &jobs.Job{
		ID:     jobID,
		Type:   "parent",
		Args:   []byte(`{}`),
		Queue:  "default",
		Status: jobs.StatusPending,
	}
	err = store.Enqueue(context.Background(), job)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)

	// Nested should not have been called (checkpoint existed)
	assert.Equal(t, int32(0), callCount.Load())
}

func TestCall_PropagatesError(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	queue.Register("failing", func(ctx context.Context, _ struct{}) (string, error) {
		return "", errors.New("nested call failed")
	})

	var capturedErr error
	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.Call[string](ctx, "failing", struct{}{})
		capturedErr = err
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "parent", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for failure
	for i := 0; i < 50; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.NotNil(t, capturedErr)
	assert.Contains(t, capturedErr.Error(), "nested call failed")
}

func TestCall_UnknownHandler(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	var callErr error
	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.Call[string](ctx, "unknown-handler", struct{}{})
		callErr = err
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "parent", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for failure
	for i := 0; i < 50; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.NotNil(t, callErr)
	assert.Contains(t, callErr.Error(), "no handler")
}

func TestCall_MultipleNestedCalls(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	queue.Register("add", func(ctx context.Context, n int) (int, error) {
		return n + 10, nil
	})

	queue.Register("multiply", func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	var finalResult int
	queue.Register("calculate", func(ctx context.Context, start int) error {
		added, err := jobs.Call[int](ctx, "add", start)
		if err != nil {
			return err
		}

		multiplied, err := jobs.Call[int](ctx, "multiply", added)
		if err != nil {
			return err
		}

		finalResult = multiplied
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "calculate", 5)
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	for i := 0; i < 50; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// (5 + 10) * 2 = 30
	assert.Equal(t, 30, finalResult)
}

func TestCall_WithComplexTypes(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	type Input struct {
		Name  string
		Value int
	}

	type Output struct {
		Result  string
		Success bool
	}

	queue.Register("process", func(ctx context.Context, input Input) (Output, error) {
		return Output{
			Result:  input.Name + " processed",
			Success: input.Value > 0,
		}, nil
	})

	var output Output
	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[Output](ctx, "process", Input{Name: "test", Value: 42})
		if err != nil {
			return err
		}
		output = result
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "parent", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	for i := 0; i < 50; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, "test processed", output.Result)
	assert.True(t, output.Success)
}

func TestCall_VoidReturn(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	var sideEffect atomic.Int32
	queue.Register("side-effect", func(ctx context.Context, value int) error {
		sideEffect.Store(int32(value))
		return nil
	})

	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.Call[any](ctx, "side-effect", 123)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "parent", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	for i := 0; i < 50; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int32(123), sideEffect.Load())
}

func TestCall_NestedCallsInSequence(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	var mu sync.Mutex
	var callOrder []string

	queue.Register("step1", func(ctx context.Context, _ struct{}) (string, error) {
		mu.Lock()
		callOrder = append(callOrder, "step1")
		mu.Unlock()
		return "one", nil
	})

	queue.Register("step2", func(ctx context.Context, _ struct{}) (string, error) {
		mu.Lock()
		callOrder = append(callOrder, "step2")
		mu.Unlock()
		return "two", nil
	})

	queue.Register("step3", func(ctx context.Context, _ struct{}) (string, error) {
		mu.Lock()
		callOrder = append(callOrder, "step3")
		mu.Unlock()
		return "three", nil
	})

	queue.Register("workflow", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.Call[string](ctx, "step1", struct{}{})
		if err != nil {
			return err
		}
		_, err = jobs.Call[string](ctx, "step2", struct{}{})
		if err != nil {
			return err
		}
		_, err = jobs.Call[string](ctx, "step3", struct{}{})
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "workflow", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	for i := 0; i < 50; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"step1", "step2", "step3"}, callOrder)
}

func TestCall_CheckpointedErrorReturned(t *testing.T) {
	queue, store := setupCallTestQueue(t)

	var callCount atomic.Int32
	queue.Register("failing", func(ctx context.Context, _ struct{}) (string, error) {
		callCount.Add(1)
		return "", errors.New("original error")
	})

	// Simulate pre-existing checkpoint with error
	jobID := "test-error-job"
	checkpoint := &jobs.Checkpoint{
		ID:        "cp-err",
		JobID:     jobID,
		CallIndex: 0,
		CallType:  "failing",
		Error:     "checkpointed error",
	}
	err := store.SaveCheckpoint(context.Background(), checkpoint)
	require.NoError(t, err)

	var capturedErr error
	queue.Register("parent-error", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.Call[string](ctx, "failing", struct{}{})
		capturedErr = err
		return err
	})

	// Create job with pre-existing checkpoints
	job := &jobs.Job{
		ID:     jobID,
		Type:   "parent-error",
		Args:   []byte(`{}`),
		Queue:  "default",
		Status: jobs.StatusPending,
	}
	err = store.Enqueue(context.Background(), job)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for failure
	for i := 0; i < 50; i++ {
		updatedJob, _ := store.GetJob(context.Background(), jobID)
		if updatedJob != nil && updatedJob.Status == jobs.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Should not have called the handler (used checkpoint)
	assert.Equal(t, int32(0), callCount.Load())
	// Should have got the checkpointed error
	assert.NotNil(t, capturedErr)
	assert.Contains(t, capturedErr.Error(), "checkpointed error")
}

func TestCall_OutsideJobHandler(t *testing.T) {
	// Calling jobs.Call outside of a job handler should return an error
	// This tests the getJobContext() nil return path
	ctx := context.Background()

	_, err := jobs.Call[string](ctx, "any-handler", "args")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be used within a job handler")
}
