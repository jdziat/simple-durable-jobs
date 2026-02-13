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

func TestCall_ResumesAfterWorkerKilled(t *testing.T) {
	// This test verifies that when a job with multiple Call[T] steps is
	// interrupted mid-execution and restarted, it resumes from the last
	// completed checkpoint rather than re-executing completed steps.

	queue, store := setupCallTestQueue(t)

	// Track how many times each step is actually executed
	var step1Executions atomic.Int32
	var step2Executions atomic.Int32
	var step3Executions atomic.Int32

	// Channel to signal when step2 starts (so we can kill the worker)
	step2Started := make(chan struct{}, 1)

	queue.Register("step1", func(ctx context.Context, input int) (int, error) {
		step1Executions.Add(1)
		return input + 10, nil
	})

	queue.Register("step2", func(ctx context.Context, input int) (int, error) {
		count := step2Executions.Add(1)
		if count == 1 {
			// First execution - signal and then block until context cancelled
			select {
			case step2Started <- struct{}{}:
			default:
			}
			// Wait for context to be cancelled (simulating crash during execution)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		// Second execution (after resume) - complete normally
		return input * 2, nil
	})

	queue.Register("step3", func(ctx context.Context, input int) (int, error) {
		step3Executions.Add(1)
		return input + 5, nil
	})

	var finalResult atomic.Int32
	queue.Register("multi-step-workflow", func(ctx context.Context, start int) error {
		// Step 1: add 10
		result1, err := jobs.Call[int](ctx, "step1", start)
		if err != nil {
			return err
		}

		// Step 2: multiply by 2 (this is where we'll kill the worker)
		result2, err := jobs.Call[int](ctx, "step2", result1)
		if err != nil {
			return err
		}

		// Step 3: add 5
		result3, err := jobs.Call[int](ctx, "step3", result2)
		if err != nil {
			return err
		}

		finalResult.Store(int32(result3))
		return nil
	})

	// Enqueue the job
	jobID, err := queue.Enqueue(context.Background(), "multi-step-workflow", 5)
	require.NoError(t, err)

	// Start first worker
	ctx1, cancel1 := context.WithCancel(context.Background())
	worker1 := queue.NewWorker()
	go worker1.Start(ctx1)

	// Wait for step2 to start, then kill the worker
	select {
	case <-step2Started:
		// Kill the worker immediately while step2 is running
		cancel1()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for step2 to start")
	}

	// Give worker time to shut down
	time.Sleep(200 * time.Millisecond)

	// Verify job is not completed yet (should be running or pending after context cancel)
	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.NotEqual(t, jobs.StatusCompleted, job.Status, "job should not be completed yet")

	// Verify step1 checkpoint was saved
	checkpoints, err := store.GetCheckpoints(context.Background(), jobID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(checkpoints), 1, "should have at least step1 checkpoint")

	// Record current execution counts
	step1BeforeResume := step1Executions.Load()

	// Release stale locks so job can be picked up again
	// This simulates what would happen in production when a worker dies
	// Use negative duration to force release all locks (locked_until < now + 1 hour)
	released, err := store.ReleaseStaleLocks(context.Background(), -time.Hour)
	require.NoError(t, err)
	t.Logf("Released %d stale locks", released)

	// Start second worker to resume the job
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	worker2 := queue.NewWorker()
	go worker2.Start(ctx2)

	// Wait for job to complete
	for i := 0; i < 100; i++ {
		job, _ = store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.Equal(t, jobs.StatusCompleted, job.Status, "job should be completed")

	// Verify step1 was NOT re-executed (was checkpointed)
	assert.Equal(t, step1BeforeResume, step1Executions.Load(),
		"step1 should not be re-executed after resume (was checkpointed)")

	// Step2 should have been called twice (once failed, once succeeded)
	assert.Equal(t, int32(2), step2Executions.Load(),
		"step2 should have executed twice (once interrupted, once completed)")

	// Step3 should have run exactly once
	assert.Equal(t, int32(1), step3Executions.Load(),
		"step3 should have executed exactly once")

	// Verify final result is correct: (5 + 10) * 2 + 5 = 35
	assert.Equal(t, int32(35), finalResult.Load())

	// Verify all checkpoints were saved
	checkpoints, err = store.GetCheckpoints(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, 3, len(checkpoints), "should have all 3 checkpoints")
}

func TestCall_MultipleResumesPreservesCheckpoints(t *testing.T) {
	// Test that a job can be interrupted and resumed multiple times
	// and checkpoints accumulate correctly

	queue, store := setupCallTestQueue(t)

	var stepExecutions [5]atomic.Int32
	stepReached := make(chan int, 10)
	var interruptAtStep atomic.Int32
	interruptAtStep.Store(-1) // -1 = don't interrupt

	for i := 0; i < 5; i++ {
		stepNum := i
		queue.Register(fmt.Sprintf("step%d", stepNum), func(ctx context.Context, _ struct{}) (int, error) {
			count := stepExecutions[stepNum].Add(1)
			stepReached <- stepNum

			// If this is the step we should interrupt at, and it's the first execution
			if int32(stepNum) == interruptAtStep.Load() && count == 1 {
				<-ctx.Done()
				return 0, ctx.Err()
			}

			return stepNum * 10, nil
		})
	}

	queue.Register("five-step-workflow", func(ctx context.Context, _ struct{}) error {
		for i := 0; i < 5; i++ {
			_, err := jobs.Call[int](ctx, fmt.Sprintf("step%d", i), struct{}{})
			if err != nil {
				return err
			}
		}
		return nil
	})

	jobID, err := queue.Enqueue(context.Background(), "five-step-workflow", struct{}{})
	require.NoError(t, err)

	// Helper to run worker until a specific step starts, then kill it
	runUntilStep := func(targetStep int) {
		interruptAtStep.Store(int32(targetStep))

		ctx, cancel := context.WithCancel(context.Background())
		worker := queue.NewWorker()
		go worker.Start(ctx)

		// Wait for target step to start
		for {
			select {
			case step := <-stepReached:
				if step >= targetStep {
					cancel()
					time.Sleep(100 * time.Millisecond) // Let worker shut down
					// Release locks so job can be picked up again (negative duration = force release)
					store.ReleaseStaleLocks(context.Background(), -time.Hour)
					return
				}
			case <-time.After(5 * time.Second):
				cancel()
				return
			}
		}
	}

	// Run until step 1, then kill
	runUntilStep(1)
	checkpoints, _ := store.GetCheckpoints(context.Background(), jobID)
	step0ExecAfterFirst := stepExecutions[0].Load()
	t.Logf("After first run: %d checkpoints, step0 executed %d times", len(checkpoints), step0ExecAfterFirst)

	// Run until step 3, then kill
	runUntilStep(3)
	step0ExecAfterSecond := stepExecutions[0].Load()
	t.Logf("After second run: step0 executed %d times (should still be %d)", step0ExecAfterSecond, step0ExecAfterFirst)

	// Step 0 should NOT have been re-executed
	assert.Equal(t, step0ExecAfterFirst, step0ExecAfterSecond,
		"step0 should not re-execute after being checkpointed")

	// Run to completion (no interrupt)
	interruptAtStep.Store(-1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	for i := 0; i < 100; i++ {
		job, _ := store.GetJob(context.Background(), jobID)
		if job != nil && job.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	job, _ := store.GetJob(context.Background(), jobID)
	require.Equal(t, jobs.StatusCompleted, job.Status)

	// Verify step0 still only executed once
	assert.Equal(t, int32(1), stepExecutions[0].Load(),
		"step0 should have executed exactly once across all resumes")

	// All steps should have completed
	checkpoints, _ = store.GetCheckpoints(context.Background(), jobID)
	assert.Equal(t, 5, len(checkpoints), "should have all 5 checkpoints")
}
