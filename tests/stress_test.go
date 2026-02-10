package jobs_test

import (
	"context"
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

func TestStress_ThousandJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dbPath := fmt.Sprintf("/tmp/jobs_stress_test_%d.db", os.Getpid())
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	// Use WAL mode for better concurrent performance
	db, err := gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)

	const numJobs = 1000
	var processed atomic.Int64
	var processingTimes sync.Map

	queue.Register("stress-task", func(ctx context.Context, id int) error {
		processed.Add(1)
		processingTimes.Store(id, time.Now())
		return nil
	})

	ctx := context.Background()

	// Enqueue jobs
	t.Log("Enqueueing", numJobs, "jobs...")
	enqueueStart := time.Now()

	for i := 0; i < numJobs; i++ {
		_, err := queue.Enqueue(ctx, "stress-task", i)
		require.NoError(t, err)
	}

	enqueueDuration := time.Since(enqueueStart)
	t.Logf("Enqueued %d jobs in %v (%.0f jobs/sec)", numJobs, enqueueDuration, float64(numJobs)/enqueueDuration.Seconds())

	// Start workers
	// Note: SQLite has limited concurrency for writes. For production, use PostgreSQL.
	// Extended timeout to account for polling overhead from fan-out feature
	workerCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	numWorkers := 1
	concurrencyPerWorker := 5

	t.Logf("Starting %d workers with concurrency %d each...", numWorkers, concurrencyPerWorker)
	processStart := time.Now()

	for i := 0; i < numWorkers; i++ {
		worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(concurrencyPerWorker)))
		go worker.Start(workerCtx)
	}

	// Wait for all jobs to complete
	lastLog := time.Now()
	for {
		count := processed.Load()
		if count >= numJobs {
			break
		}

		if time.Since(lastLog) > 5*time.Second {
			t.Logf("Progress: %d/%d (%.1f%%)", count, numJobs, float64(count)/float64(numJobs)*100)
			lastLog = time.Now()
		}

		select {
		case <-workerCtx.Done():
			t.Fatalf("Timeout waiting for jobs to complete. Processed: %d/%d", count, numJobs)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	processDuration := time.Since(processStart)
	totalProcessed := processed.Load()

	t.Logf("Processed %d jobs in %v (%.0f jobs/sec)", totalProcessed, processDuration, float64(totalProcessed)/processDuration.Seconds())

	// Verify all jobs completed
	assert.Equal(t, int64(numJobs), totalProcessed)

	// Check database state
	completedJobs, err := store.GetJobsByStatus(context.Background(), jobs.StatusCompleted, numJobs+100)
	require.NoError(t, err)
	assert.Len(t, completedJobs, numJobs, "All jobs should be marked completed in DB")

	// Report stats
	t.Log("--- STRESS TEST RESULTS ---")
	t.Logf("Total jobs:       %d", numJobs)
	t.Logf("Workers:          %d", numWorkers)
	t.Logf("Concurrency:      %d per worker (%d total)", concurrencyPerWorker, numWorkers*concurrencyPerWorker)
	t.Logf("Enqueue time:     %v", enqueueDuration)
	t.Logf("Process time:     %v", processDuration)
	t.Logf("Enqueue rate:     %.0f jobs/sec", float64(numJobs)/enqueueDuration.Seconds())
	t.Logf("Process rate:     %.0f jobs/sec", float64(numJobs)/processDuration.Seconds())
}

func TestStress_WorkflowsWithCheckpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dbPath := fmt.Sprintf("/tmp/jobs_stress_workflow_%d.db", os.Getpid())
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)

	const numWorkflows = 100
	var completedWorkflows atomic.Int64
	var totalSteps atomic.Int64

	// Register step handlers
	queue.Register("step1", func(ctx context.Context, id int) (string, error) {
		totalSteps.Add(1)
		return fmt.Sprintf("step1-result-%d", id), nil
	})

	queue.Register("step2", func(ctx context.Context, prev string) (string, error) {
		totalSteps.Add(1)
		return prev + "-step2", nil
	})

	queue.Register("step3", func(ctx context.Context, prev string) (string, error) {
		totalSteps.Add(1)
		return prev + "-step3", nil
	})

	// Register workflow
	queue.Register("workflow", func(ctx context.Context, id int) error {
		result1, err := jobs.Call[string](ctx, "step1", id)
		if err != nil {
			return err
		}

		result2, err := jobs.Call[string](ctx, "step2", result1)
		if err != nil {
			return err
		}

		_, err = jobs.Call[string](ctx, "step3", result2)
		if err != nil {
			return err
		}

		completedWorkflows.Add(1)
		return nil
	})

	ctx := context.Background()

	// Enqueue workflows
	t.Log("Enqueueing", numWorkflows, "workflows...")
	enqueueStart := time.Now()

	for i := 0; i < numWorkflows; i++ {
		_, err := queue.Enqueue(ctx, "workflow", i)
		require.NoError(t, err)
	}

	enqueueDuration := time.Since(enqueueStart)
	t.Logf("Enqueued %d workflows in %v", numWorkflows, enqueueDuration)

	// Start workers
	// Note: Using single worker due to SQLite concurrency limitations
	workerCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	numWorkers := 1
	t.Logf("Starting %d workers...", numWorkers)

	processStart := time.Now()

	for i := 0; i < numWorkers; i++ {
		worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(3)))
		go worker.Start(workerCtx)
	}

	// Wait for completion
	lastLog := time.Now()
	for {
		count := completedWorkflows.Load()
		if count >= numWorkflows {
			break
		}

		if time.Since(lastLog) > 5*time.Second {
			t.Logf("Progress: %d/%d workflows (%.1f%%)", count, numWorkflows, float64(count)/float64(numWorkflows)*100)
			lastLog = time.Now()
		}

		select {
		case <-workerCtx.Done():
			t.Fatalf("Timeout. Completed: %d/%d workflows", count, numWorkflows)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	processDuration := time.Since(processStart)

	t.Log("--- WORKFLOW STRESS TEST RESULTS ---")
	t.Logf("Workflows:        %d", numWorkflows)
	t.Logf("Steps per workflow: 3")
	t.Logf("Total steps:      %d", totalSteps.Load())
	t.Logf("Process time:     %v", processDuration)
	t.Logf("Workflow rate:    %.0f workflows/sec", float64(numWorkflows)/processDuration.Seconds())
	t.Logf("Step rate:        %.0f steps/sec", float64(totalSteps.Load())/processDuration.Seconds())

	// Verify checkpoints were created
	var checkpointCount int64
	db.Raw("SELECT COUNT(*) FROM checkpoints").Scan(&checkpointCount)
	t.Logf("Checkpoints:      %d", checkpointCount)

	assert.Equal(t, int64(numWorkflows), completedWorkflows.Load())
	assert.Equal(t, int64(numWorkflows*3), totalSteps.Load(), "Each workflow should have 3 steps")
	assert.Equal(t, int64(numWorkflows*3), checkpointCount, "Each step should create a checkpoint")
}

func TestStress_MixedPriorities(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dbPath := fmt.Sprintf("/tmp/jobs_stress_priorities_%d.db", os.Getpid())
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)

	const numJobs = 1000
	var highPriorityOrder []int
	var lowPriorityOrder []int
	var mu sync.Mutex
	var orderIndex atomic.Int64

	queue.Register("priority-task", func(ctx context.Context, args struct {
		ID       int
		Priority int
	}) error {
		idx := orderIndex.Add(1)
		mu.Lock()
		if args.Priority == 100 {
			highPriorityOrder = append(highPriorityOrder, int(idx))
		} else {
			lowPriorityOrder = append(lowPriorityOrder, int(idx))
		}
		mu.Unlock()
		return nil
	})

	ctx := context.Background()

	// Enqueue mixed priority jobs
	t.Log("Enqueueing", numJobs, "jobs with mixed priorities...")

	for i := 0; i < numJobs; i++ {
		priority := 1 // low
		if i%10 == 0 {
			priority = 100 // high (10% of jobs)
		}
		_, err := queue.Enqueue(ctx, "priority-task", struct {
			ID       int
			Priority int
		}{ID: i, Priority: priority}, jobs.Priority(priority))
		require.NoError(t, err)
	}

	// Start single worker with concurrency 1 to see priority ordering
	// Extended timeout: 1000 jobs at ~10 jobs/sec = ~100s, plus overhead
	workerCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(1)))
	go worker.Start(workerCtx)

	// Wait for completion
	for orderIndex.Load() < numJobs {
		select {
		case <-workerCtx.Done():
			t.Fatalf("Timeout. Processed: %d/%d", orderIndex.Load(), numJobs)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	mu.Lock()
	highCount := len(highPriorityOrder)
	lowCount := len(lowPriorityOrder)

	// Check that high priority jobs generally ran earlier
	avgHighOrder := float64(0)
	for _, o := range highPriorityOrder {
		avgHighOrder += float64(o)
	}
	avgHighOrder /= float64(len(highPriorityOrder))

	avgLowOrder := float64(0)
	for _, o := range lowPriorityOrder {
		avgLowOrder += float64(o)
	}
	avgLowOrder /= float64(len(lowPriorityOrder))
	mu.Unlock()

	t.Log("--- PRIORITY STRESS TEST RESULTS ---")
	t.Logf("Total jobs:       %d", numJobs)
	t.Logf("High priority:    %d (10%%)", highCount)
	t.Logf("Low priority:     %d (90%%)", lowCount)
	t.Logf("Avg high order:   %.1f", avgHighOrder)
	t.Logf("Avg low order:    %.1f", avgLowOrder)

	// High priority jobs should have lower average order (ran earlier)
	assert.Less(t, avgHighOrder, avgLowOrder, "High priority jobs should run before low priority on average")
}
