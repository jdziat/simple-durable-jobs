package storage

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// skipIfNotPostgres skips the test when TEST_DATABASE_URL is not set.
func skipIfNotPostgres(t *testing.T) {
	t.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping PostgreSQL-specific test")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Dequeue: FOR UPDATE SKIP LOCKED
// ──────────────────────────────────────────────────────────────────────────────

func TestDequeue_PostgreSQL_ForUpdateSkipLocked(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	// Enqueue two jobs on the same queue.
	job1 := newTestJob("work", "task")
	job1.Priority = 10
	require.NoError(t, s.Enqueue(ctx, job1))

	job2 := newTestJob("work", "task")
	job2.Priority = 10
	require.NoError(t, s.Enqueue(ctx, job2))

	// Two goroutines dequeue concurrently — each should get a distinct job.
	var (
		mu       sync.Mutex
		results  []*core.Job
		errs     []error
		wg       sync.WaitGroup
	)

	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := s.Dequeue(ctx, []string{"work"}, "worker-concurrent")
			mu.Lock()
			defer mu.Unlock()
			results = append(results, j)
			errs = append(errs, err)
		}()
	}
	wg.Wait()

	require.Len(t, errs, 2)
	for _, err := range errs {
		assert.NoError(t, err)
	}
	require.Len(t, results, 2)

	// Both results should be non-nil and have distinct IDs.
	require.NotNil(t, results[0], "first dequeue should return a job")
	require.NotNil(t, results[1], "second dequeue should return a job")
	assert.NotEqual(t, results[0].ID, results[1].ID,
		"concurrent dequeues must return different jobs")
}

func TestDequeue_PostgreSQL_NoPendingJobs(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	job, err := s.Dequeue(ctx, []string{"empty-queue"}, "worker")
	assert.NoError(t, err)
	assert.Nil(t, job, "dequeue on empty queue should return nil")
}

func TestDequeue_PostgreSQL_RespectsQueuePause(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	j := newTestJob("paused-q", "task")
	require.NoError(t, s.Enqueue(ctx, j))
	require.NoError(t, s.PauseQueue(ctx, "paused-q"))

	job, err := s.Dequeue(ctx, []string{"paused-q"}, "worker")
	assert.NoError(t, err)
	assert.Nil(t, job, "should not dequeue from a paused queue")
}

func TestDequeue_PostgreSQL_SkipsLockedJobs(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	// Enqueue one job and dequeue it to lock it.
	j := newTestJob("lock-q", "task")
	require.NoError(t, s.Enqueue(ctx, j))

	got, err := s.Dequeue(ctx, []string{"lock-q"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)

	// Second dequeue should find nothing — the only job is running.
	got2, err := s.Dequeue(ctx, []string{"lock-q"}, "worker-2")
	assert.NoError(t, err)
	assert.Nil(t, got2, "should not dequeue an already-running job")
}

func TestDequeue_PostgreSQL_PriorityOrdering(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	low := newTestJob("prio-q", "low-prio")
	low.Priority = 1
	require.NoError(t, s.Enqueue(ctx, low))

	high := newTestJob("prio-q", "high-prio")
	high.Priority = 100
	require.NoError(t, s.Enqueue(ctx, high))

	got, err := s.Dequeue(ctx, []string{"prio-q"}, "worker")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "high-prio", got.Type, "higher priority job should be dequeued first")
}

// ──────────────────────────────────────────────────────────────────────────────
// EnqueueUnique: FOR UPDATE locking
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueueUnique_PostgreSQL_ForUpdate(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	// Run concurrent unique enqueues with the same key — exactly one should succeed.
	const concurrency = 5
	var (
		mu      sync.Mutex
		successes int
		duplicates int
		errs      []error
		wg        sync.WaitGroup
	)

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job := newTestJob("unique-q", "unique-task")
			err := s.EnqueueUnique(ctx, job, "unique-key-pg")
			mu.Lock()
			defer mu.Unlock()
			switch err {
			case nil:
				successes++
			case core.ErrDuplicateJob:
				duplicates++
			default:
				errs = append(errs, err)
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		assert.NoError(t, err, "unexpected error during concurrent EnqueueUnique")
	}
	assert.Equal(t, 1, successes, "exactly one enqueue should succeed")
	assert.Equal(t, concurrency-1, duplicates, "remaining should be duplicates")
}

func TestEnqueueUnique_PostgreSQL_AllowsAfterCompletion(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newTestStorage(t)

	// Enqueue a unique job and complete it.
	job1 := newTestJob("unique-q", "unique-reuse")
	require.NoError(t, s.EnqueueUnique(ctx, job1, "reuse-key"))

	dequeued, err := s.Dequeue(ctx, []string{"unique-q"}, "worker")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	require.NoError(t, s.Complete(ctx, dequeued.ID, "worker"))

	// A new job with the same unique key should now succeed.
	job2 := newTestJob("unique-q", "unique-reuse-2")
	err = s.EnqueueUnique(ctx, job2, "reuse-key")
	assert.NoError(t, err, "should allow re-enqueue after prior job completed")
}

// ──────────────────────────────────────────────────────────────────────────────
// IsSQLite detection
// ──────────────────────────────────────────────────────────────────────────────

func TestNewGormStorage_IsNotSQLite_PostgreSQL(t *testing.T) {
	skipIfNotPostgres(t)

	db := openTestDB(t)
	s := NewGormStorage(db)
	assert.False(t, s.IsSQLite(), "PostgreSQL connection should not be detected as SQLite")
}
