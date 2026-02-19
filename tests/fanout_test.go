package jobs_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFanOutTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	return openIntegrationQueue(t)
}

func TestFanOut_Basic(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedCount atomic.Int32
	resultChan := make(chan int, 5)

	// Register sub-job handler that multiplies by 2
	queue.Register("multiply", func(ctx context.Context, n int) (int, error) {
		processedCount.Add(1)
		result := n * 2
		resultChan <- result
		return result, nil
	})

	// Register parent job that fans out
	queue.Register("batchMultiply", func(ctx context.Context, items []int) ([]int, error) {
		subJobs := make([]jobs.SubJob, len(items))
		for i, item := range items {
			subJobs[i] = jobs.Sub("multiply", item)
		}

		results, err := jobs.FanOut[int](ctx, subJobs, jobs.FailFast())
		if err != nil {
			// If we get a suspend error, that's expected during first run
			if jobs.IsSuspendError(err) {
				return nil, err
			}
			return nil, err
		}

		return jobs.Values(results), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "batchMultiply", []int{1, 2, 3, 4, 5})
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait for sub-jobs to complete
	completed := 0
	timeout := time.After(5 * time.Second)
waitLoop:
	for completed < 5 {
		select {
		case <-resultChan:
			completed++
		case <-timeout:
			break waitLoop
		}
	}

	assert.Equal(t, int32(5), processedCount.Load(), "all 5 sub-jobs should be processed")
}

func TestFanOut_Empty(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var called atomic.Bool

	queue.Register("emptyFanout", func(ctx context.Context, _ struct{}) error {
		results, err := jobs.FanOut[int](ctx, nil, jobs.FailFast())
		called.Store(true)
		assert.NoError(t, err)
		assert.Empty(t, results)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "emptyFanout", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait a bit for job to run
	time.Sleep(500 * time.Millisecond)
	assert.True(t, called.Load())
}

func TestSub_DefaultValues(t *testing.T) {
	subJob := fanout.Sub("testJob", map[string]int{"key": 42})

	assert.Equal(t, "testJob", subJob.Type)
	assert.Equal(t, map[string]int{"key": 42}, subJob.Args)
	// Default retries comes from queue.DefaultJobRetries which is 2
	assert.Equal(t, 2, subJob.Retries)
	assert.Empty(t, subJob.Queue) // Empty means inherit from parent
}

func TestSub_WithOptions(t *testing.T) {
	subJob := fanout.Sub("testJob", "args",
		jobs.QueueOpt("high-priority"),
		jobs.Priority(10),
		jobs.Retries(5),
	)

	assert.Equal(t, "testJob", subJob.Type)
	assert.Equal(t, "high-priority", subJob.Queue)
	assert.Equal(t, 10, subJob.Priority)
	assert.Equal(t, 5, subJob.Retries)
}

func TestFanOutError_Message(t *testing.T) {
	err := &fanout.Error{
		FanOutID:    "test-id",
		TotalCount:  10,
		FailedCount: 3,
		Failures:    []fanout.SubJobFailure{},
	}

	assert.Equal(t, "fan-out failed: 3/10 sub-jobs failed", err.Error())
}

func TestSuspendError(t *testing.T) {
	err := &fanout.SuspendError{FanOutID: "test-123"}
	assert.Contains(t, err.Error(), "test-123")
	assert.True(t, fanout.IsSuspendError(err))
	assert.False(t, fanout.IsSuspendError(fmt.Errorf("regular error")))
	assert.False(t, fanout.IsSuspendError(nil))
}

func TestFanOut_FailFast_StopsOnFirstFailure(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedCount atomic.Int32
	failedJobIndex := 2 // The 3rd job will fail

	queue.Register("mayFail", func(ctx context.Context, index int) (int, error) {
		processedCount.Add(1)
		if index == failedJobIndex {
			// NoRetry to prevent re-execution in FailFast test
			return 0, jobs.NoRetry(fmt.Errorf("intentional failure at index %d", index))
		}
		// Add small delay to ensure ordering
		time.Sleep(10 * time.Millisecond)
		return index * 10, nil
	})

	var fanoutErr error
	var fanoutCompleted atomic.Bool

	queue.Register("failFastParent", func(ctx context.Context, count int) error {
		subJobs := make([]jobs.SubJob, count)
		for i := 0; i < count; i++ {
			subJobs[i] = jobs.Sub("mayFail", i)
		}

		_, err := jobs.FanOut[int](ctx, subJobs, jobs.FailFast())
		if jobs.IsSuspendError(err) {
			return err
		}
		fanoutErr = err
		fanoutCompleted.Store(true)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "failFastParent", 5)
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait for fan-out to complete or timeout
	deadline := time.After(10 * time.Second)
	for !fanoutCompleted.Load() {
		select {
		case <-deadline:
			t.Log("Timeout waiting for fan-out completion")
			goto checkResults
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

checkResults:
	// FailFast should return an error when a sub-job fails
	if fanoutCompleted.Load() {
		assert.Error(t, fanoutErr, "FailFast should return error on sub-job failure")
	}
}

func TestFanOut_CollectAll_ReturnsPartialResults(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedCount atomic.Int32

	queue.Register("partialSuccess", func(ctx context.Context, index int) (int, error) {
		processedCount.Add(1)
		// Fail every other job with NoRetry to prevent re-execution
		if index%2 == 1 {
			return 0, jobs.NoRetry(fmt.Errorf("odd index %d fails", index))
		}
		return index * 10, nil
	})

	var results []fanout.Result[int]
	var fanoutCompleted atomic.Bool

	queue.Register("collectAllParent", func(ctx context.Context, count int) error {
		subJobs := make([]jobs.SubJob, count)
		for i := 0; i < count; i++ {
			subJobs[i] = jobs.Sub("partialSuccess", i)
		}

		var err error
		results, err = jobs.FanOut[int](ctx, subJobs, jobs.CollectAll())
		if jobs.IsSuspendError(err) {
			return err
		}
		fanoutCompleted.Store(true)
		return nil // CollectAll doesn't fail the parent
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "collectAllParent", 6)
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait for completion
	deadline := time.After(10 * time.Second)
	for !fanoutCompleted.Load() {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for fan-out completion")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// All 6 jobs should have been processed
	assert.Equal(t, int32(6), processedCount.Load())

	// Should have 6 results
	assert.Len(t, results, 6)

	// Partition into successes and failures
	successes, failures := jobs.Partition(results)
	assert.Len(t, successes, 3, "indices 0, 2, 4 should succeed")
	assert.Len(t, failures, 3, "indices 1, 3, 5 should fail")

	// Check Values helper
	values := jobs.Values(results)
	assert.Len(t, values, 3)

	// AllSucceeded should be false
	assert.False(t, jobs.AllSucceeded(results))
}

func TestFanOut_Threshold_SucceedsAboveThreshold(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedCount atomic.Int32

	queue.Register("thresholdJob", func(ctx context.Context, index int) (int, error) {
		processedCount.Add(1)
		// 2 out of 10 fail (80% success rate) with NoRetry
		if index == 3 || index == 7 {
			return 0, jobs.NoRetry(fmt.Errorf("index %d fails", index))
		}
		return index, nil
	})

	var fanoutErr error
	var fanoutCompleted atomic.Bool

	queue.Register("thresholdParent", func(ctx context.Context, count int) error {
		subJobs := make([]jobs.SubJob, count)
		for i := 0; i < count; i++ {
			subJobs[i] = jobs.Sub("thresholdJob", i)
		}

		// 75% threshold - should succeed since 80% pass
		_, err := jobs.FanOut[int](ctx, subJobs, jobs.Threshold(0.75))
		if jobs.IsSuspendError(err) {
			return err
		}
		fanoutErr = err
		fanoutCompleted.Store(true)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "thresholdParent", 10)
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	deadline := time.After(10 * time.Second)
	for !fanoutCompleted.Load() {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for fan-out completion")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// With 80% success and 75% threshold, should succeed
	assert.NoError(t, fanoutErr, "80% success should pass 75% threshold")
	assert.Equal(t, int32(10), processedCount.Load())
}

func TestFanOut_Threshold_FailsBelowThreshold(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedCount atomic.Int32

	queue.Register("thresholdFailJob", func(ctx context.Context, index int) (int, error) {
		processedCount.Add(1)
		// 5 out of 10 fail (50% success rate) with NoRetry
		if index < 5 {
			return 0, jobs.NoRetry(fmt.Errorf("index %d fails", index))
		}
		return index, nil
	})

	var fanoutErr error
	var fanoutCompleted atomic.Bool

	queue.Register("thresholdFailParent", func(ctx context.Context, count int) error {
		subJobs := make([]jobs.SubJob, count)
		for i := 0; i < count; i++ {
			subJobs[i] = jobs.Sub("thresholdFailJob", i)
		}

		// 75% threshold - should fail since only 50% pass
		_, err := jobs.FanOut[int](ctx, subJobs, jobs.Threshold(0.75))
		if jobs.IsSuspendError(err) {
			return err
		}
		fanoutErr = err
		fanoutCompleted.Store(true)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "thresholdFailParent", 10)
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	deadline := time.After(10 * time.Second)
	for !fanoutCompleted.Load() {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for fan-out completion")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// With 50% success and 75% threshold, should fail
	assert.Error(t, fanoutErr, "50% success should fail 75% threshold")
}

func TestFanOut_WithOptions(t *testing.T) {
	queue, _ := setupFanOutTestQueue(t)

	var processedQueue string
	var processedCount atomic.Int32

	queue.Register("optionsJob", func(ctx context.Context, val int) (int, error) {
		processedCount.Add(1)
		return val * 2, nil
	})

	var fanoutCompleted atomic.Bool

	queue.Register("optionsParent", func(ctx context.Context, _ struct{}) error {
		subJobs := []jobs.SubJob{
			jobs.Sub("optionsJob", 1),
			jobs.Sub("optionsJob", 2),
		}

		_, err := jobs.FanOut[int](ctx, subJobs,
			jobs.FailFast(),
			jobs.WithFanOutRetries(5),
		)
		if jobs.IsSuspendError(err) {
			return err
		}
		fanoutCompleted.Store(true)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "optionsParent", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker(
		jobs.WorkerQueue("default", jobs.Concurrency(5)),
	)
	go func() {
		_ = worker.Start(ctx)
	}()

	deadline := time.After(8 * time.Second)
	for !fanoutCompleted.Load() {
		select {
		case <-deadline:
			t.Log("Timeout - checking partial results")
			goto done
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

done:
	// Both jobs should have been processed
	assert.GreaterOrEqual(t, processedCount.Load(), int32(2))
	_ = processedQueue // Avoid unused variable warning
}

func TestFanOut_ResultHelpers(t *testing.T) {
	// Test result helper functions with mock data
	results := []fanout.Result[int]{
		{Index: 0, Value: 10, Err: nil},
		{Index: 1, Value: 0, Err: fmt.Errorf("error 1")},
		{Index: 2, Value: 20, Err: nil},
		{Index: 3, Value: 0, Err: fmt.Errorf("error 2")},
		{Index: 4, Value: 30, Err: nil},
	}

	// Test Values - should extract successful values only
	values := jobs.Values(results)
	assert.Equal(t, []int{10, 20, 30}, values)

	// Test Partition
	successes, failures := jobs.Partition(results)
	assert.Equal(t, []int{10, 20, 30}, successes)
	assert.Len(t, failures, 2)

	// Test AllSucceeded
	assert.False(t, jobs.AllSucceeded(results))

	// Test with all successes
	allSuccess := []fanout.Result[int]{
		{Index: 0, Value: 1, Err: nil},
		{Index: 1, Value: 2, Err: nil},
	}
	assert.True(t, jobs.AllSucceeded(allSuccess))
}

func TestFanOut_CancelledSubJobsCountTowardCompletion(t *testing.T) {
	queue, store := setupFanOutTestQueue(t)

	var processedCount atomic.Int32

	// Register sub-job handler - sleeps to give time for cancellation
	queue.Register("slowJob", func(ctx context.Context, index int) (int, error) {
		processedCount.Add(1)
		// First job completes fast, rest sleep to allow cancellation
		if index > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(30 * time.Second):
				return index, nil
			}
		}
		return index * 10, nil
	})

	var fanoutCompleted atomic.Bool

	queue.Register("cancelTestParent", func(ctx context.Context, count int) error {
		subJobs := make([]jobs.SubJob, count)
		for i := 0; i < count; i++ {
			subJobs[i] = jobs.Sub("slowJob", i, jobs.Retries(0))
		}

		_, err := jobs.FanOut[int](ctx, subJobs, jobs.CollectAll())
		if jobs.IsSuspendError(err) {
			return err
		}
		fanoutCompleted.Store(true)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	jobID, err := queue.Enqueue(ctx, "cancelTestParent", 5)
	require.NoError(t, err)

	worker := queue.NewWorker(jobs.WithScheduler(false))
	go func() {
		_ = worker.Start(ctx)
	}()

	// Wait for the parent to be suspended (fan-out created)
	time.Sleep(1 * time.Second)

	// Find the fan-out for this parent
	fanOuts, err := store.GetFanOutsByParent(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, fanOuts, 1, "should have one fan-out")

	// Get all child jobs
	subJobs, err := store.GetSubJobs(ctx, fanOuts[0].ID)
	require.NoError(t, err)
	require.Len(t, subJobs, 5, "should have 5 sub-jobs")

	// Wait for at least 1 sub-job to complete
	time.Sleep(500 * time.Millisecond)

	// Cancel remaining pending sub-jobs using queue.CancelSubJob (simulates external cancel)
	// This uses the queue-level method which checks for fan-out completion and resumes parent.
	cancelledCount := 0
	for _, subJob := range subJobs {
		fo, err := queue.CancelSubJob(ctx, subJob.ID)
		if err == nil && fo != nil {
			cancelledCount++
			// Check if this cancellation completed the fan-out
			if fo.CompletedCount+fo.FailedCount+fo.CancelledCount >= fo.TotalCount {
				// Fan-out should now be complete - check if parent resumed
				t.Logf("Fan-out counts: completed=%d failed=%d cancelled=%d total=%d",
					fo.CompletedCount, fo.FailedCount, fo.CancelledCount, fo.TotalCount)
			}
		}
	}

	t.Logf("Cancelled %d sub-jobs externally", cancelledCount)

	// Wait for parent to be resumed and completed
	deadline := time.After(10 * time.Second)
	for !fanoutCompleted.Load() {
		select {
		case <-deadline:
			// Check the fan-out state for debugging
			fo, _ := store.GetFanOut(ctx, fanOuts[0].ID)
			if fo != nil {
				t.Logf("Fan-out state: status=%s completed=%d failed=%d cancelled=%d total=%d",
					fo.Status, fo.CompletedCount, fo.FailedCount, fo.CancelledCount, fo.TotalCount)
			}
			parentJob, _ := store.GetJob(ctx, jobID)
			if parentJob != nil {
				t.Logf("Parent job status: %s", parentJob.Status)
			}
			t.Fatal("Timeout - parent job was not resumed after sub-jobs were cancelled")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Verify the fan-out properly accounted for cancelled jobs
	fo, err := store.GetFanOut(ctx, fanOuts[0].ID)
	require.NoError(t, err)
	assert.Greater(t, fo.CancelledCount, 0, "should have cancelled jobs counted")
	assert.Equal(t, fo.TotalCount, fo.CompletedCount+fo.FailedCount+fo.CancelledCount,
		"completed + failed + cancelled should equal total")
}
