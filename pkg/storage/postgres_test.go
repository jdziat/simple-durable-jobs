package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
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
	s := newPostgresTestStorage(t)

	// Enqueue two jobs on the same queue.
	job1 := newTestJob("work", "task")
	job1.Priority = 10
	require.NoError(t, s.Enqueue(ctx, job1))

	job2 := newTestJob("work", "task")
	job2.Priority = 10
	require.NoError(t, s.Enqueue(ctx, job2))

	// Two goroutines dequeue concurrently — each should get a distinct job.
	var (
		mu      sync.Mutex
		results []*core.Job
		errs    []error
		wg      sync.WaitGroup
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
	s := newPostgresTestStorage(t)

	job, err := s.Dequeue(ctx, []string{"empty-queue"}, "worker")
	assert.NoError(t, err)
	assert.Nil(t, job, "dequeue on empty queue should return nil")
}

func TestDequeue_PostgreSQL_RespectsQueuePause(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newPostgresTestStorage(t)

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
	s := newPostgresTestStorage(t)

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
	s := newPostgresTestStorage(t)

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

func TestCancelSubJobs_PostgreSQL_ConcurrentCompletionKeepsFanOutCountsConsistent(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newPostgresTestStorage(t)

	const subQueue = "cancel-race-q"
	seedTestJob(t, ctx, s, testUUID("parent"), core.StatusWaiting)
	fanOut := &core.FanOut{ID: testUUID("fo-cancel-race"), ParentJobID: testUUID("parent"), TotalCount: 3}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	subs := []*core.Job{
		{Type: "sub", Queue: subQueue, FanOutID: &fanOut.ID, FanOutIndex: 0},
		{Type: "sub", Queue: subQueue, FanOutID: &fanOut.ID, FanOutIndex: 1},
		{Type: "sub", Queue: subQueue, FanOutID: &fanOut.ID, FanOutIndex: 2},
	}
	require.NoError(t, s.EnqueueBatch(ctx, subs))

	alreadyCompleted, err := s.Dequeue(ctx, []string{subQueue}, "worker-completed")
	require.NoError(t, err)
	require.NotNil(t, alreadyCompleted)
	require.NoError(t, s.Complete(ctx, alreadyCompleted.ID, "worker-completed"))
	_, err = s.IncrementFanOutCompleted(ctx, fanOut.ID)
	require.NoError(t, err)

	racing, err := s.Dequeue(ctx, []string{subQueue}, "worker-racing")
	require.NoError(t, err)
	require.NotNil(t, racing)

	type hookKey struct{}
	type updateHook struct {
		ready   chan struct{}
		proceed chan struct{}
		once    sync.Once
	}
	hook := &updateHook{
		ready:   make(chan struct{}),
		proceed: make(chan struct{}),
	}
	const callbackName = "cancel_sub_jobs_race_pause"
	require.NoError(t, s.DB().Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		h, ok := tx.Statement.Context.Value(hookKey{}).(*updateHook)
		if !ok || tx.Statement.Table != "jobs" {
			return
		}
		h.once.Do(func() { close(h.ready) })
		<-h.proceed
	}))
	t.Cleanup(func() {
		_ = s.DB().Callback().Update().Remove(callbackName)
	})

	cancelCtx := context.WithValue(ctx, hookKey{}, hook)
	cancelDone := make(chan error, 1)
	go func() {
		_, err := s.CancelSubJobs(cancelCtx, fanOut.ID)
		cancelDone <- err
	}()

	select {
	case <-hook.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("CancelSubJobs did not reach the hooked job update")
	}

	completeDone := make(chan error, 1)
	go func() {
		if err := s.Complete(ctx, racing.ID, "worker-racing"); err != nil {
			completeDone <- err
			return
		}
		_, err := s.IncrementFanOutCompleted(ctx, fanOut.ID)
		completeDone <- err
	}()

	var completeErr error
	completedBeforeCancel := false
	select {
	case completeErr = <-completeDone:
		completedBeforeCancel = true
	case <-time.After(50 * time.Millisecond):
	}

	close(hook.proceed)
	require.NoError(t, <-cancelDone)

	if !completedBeforeCancel {
		completeErr = <-completeDone
	}
	if completeErr != nil {
		require.ErrorIs(t, completeErr, core.ErrJobNotOwned)
	}

	completedRow, err := s.GetJob(ctx, alreadyCompleted.ID)
	require.NoError(t, err)
	require.NotNil(t, completedRow)
	assert.Equal(t, core.StatusCompleted, completedRow.Status)

	updatedFanOut, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, updatedFanOut)
	sum := updatedFanOut.CompletedCount + updatedFanOut.FailedCount + updatedFanOut.CancelledCount
	assert.LessOrEqual(t, sum, updatedFanOut.TotalCount)

	if completeErr == nil {
		racingRow, err := s.GetJob(ctx, racing.ID)
		require.NoError(t, err)
		require.NotNil(t, racingRow)
		assert.Equal(t, core.StatusCompleted, racingRow.Status)
	}
}

func TestCompleteWithResult_PostgreSQL_ConcurrentTerminalCountsPostCommit(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newPostgresTestStorage(t)

	fo := createP2BFanOut(t, ctx, s, core.FanOutPending)
	sub1 := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-1")
	sub2 := createRunningP2BJob(t, ctx, s, &fo.ID, "worker-2")

	type hookKey struct{}
	type barrier struct {
		mu      sync.Mutex
		count   int
		ready   chan struct{}
		proceed chan struct{}
		once    sync.Once
	}
	wait := func(b *barrier) {
		b.mu.Lock()
		b.count++
		if b.count == 2 {
			b.once.Do(func() { close(b.ready) })
		}
		b.mu.Unlock()
		<-b.proceed
	}
	hook := struct {
		update barrier
		count  barrier
	}{
		update: barrier{ready: make(chan struct{}), proceed: make(chan struct{})},
		count:  barrier{ready: make(chan struct{}), proceed: make(chan struct{})},
	}

	updateCallback := "terminal_count_post_commit_update"
	require.NoError(t, s.DB().Callback().Update().After("gorm:update").Register(updateCallback, func(tx *gorm.DB) {
		h, ok := tx.Statement.Context.Value(hookKey{}).(*struct {
			update barrier
			count  barrier
		})
		if !ok || tx.Statement.Table != "jobs" {
			return
		}
		wait(&h.update)
	}))
	queryCallback := "terminal_count_post_commit_count"
	require.NoError(t, s.DB().Callback().Query().After("gorm:query").Register(queryCallback, func(tx *gorm.DB) {
		h, ok := tx.Statement.Context.Value(hookKey{}).(*struct {
			update barrier
			count  barrier
		})
		if !ok || tx.Statement.Table != "jobs" || !strings.Contains(tx.Statement.SQL.String(), "COUNT(*)") {
			return
		}
		wait(&h.count)
	}))
	t.Cleanup(func() {
		_ = s.DB().Callback().Update().Remove(updateCallback)
		_ = s.DB().Callback().Query().Remove(queryCallback)
	})

	start := make(chan struct{})
	results := make(chan *core.FanOut, 2)
	errs := make(chan error, 2)
	for _, tc := range []struct {
		jobID    core.UUID
		workerID string
		result   []byte
	}{
		{jobID: sub1.ID, workerID: "worker-1", result: []byte(`{"i":1}`)},
		{jobID: sub2.ID, workerID: "worker-2", result: []byte(`{"i":2}`)},
	} {
		go func() {
			<-start
			fo, err := s.CompleteWithResult(context.WithValue(ctx, hookKey{}, &hook), tc.jobID, tc.workerID, tc.result)
			results <- fo
			errs <- err
		}()
	}

	close(start)
	select {
	case <-hook.update.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("terminal job updates did not reach the concurrency barrier")
	}
	close(hook.update.proceed)

	select {
	case <-hook.count.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("fan-out count queries did not reach the concurrency barrier")
	}
	close(hook.count.proceed)

	for range 2 {
		require.NoError(t, <-errs)
		require.NotNil(t, <-results)
	}

	final, err := s.GetFanOut(ctx, fo.ID)
	require.NoError(t, err)
	require.NotNil(t, final)
	assert.Equal(t, core.FanOutCompleted, final.Status)
	assert.Equal(t, 2, final.CompletedCount)
}

// ──────────────────────────────────────────────────────────────────────────────
// EnqueueUnique: FOR UPDATE locking
// ──────────────────────────────────────────────────────────────────────────────

func TestEnqueueUnique_PostgreSQL_ForUpdate(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newPostgresTestStorage(t)

	// Run concurrent unique enqueues with the same key — exactly one should succeed.
	const concurrency = 5
	var (
		mu         sync.Mutex
		successes  int
		duplicates int
		errs       []error
		wg         sync.WaitGroup
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

func TestEnqueueUnique_PostgreSQL_Concurrent_NoDuplicates(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newPostgresTestStorage(t)

	const concurrency = 20
	const key = "unique-key-pg-concurrent"

	start := make(chan struct{})
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job := newTestJob("unique-q", "unique-task")
			<-start
			errs <- s.EnqueueUnique(ctx, job, key)
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	successes := 0
	duplicates := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, core.ErrDuplicateJob):
			duplicates++
		default:
			require.NoError(t, err)
		}
	}

	assert.Equal(t, 1, successes, "exactly one concurrent enqueue should succeed")
	assert.Equal(t, concurrency-1, duplicates, "all remaining enqueues should report duplicates")

	var count int64
	require.NoError(t, s.DB().Model(&core.Job{}).
		Where("unique_key = ?", key).
		Count(&count).Error)
	assert.EqualValues(t, 1, count, "concurrent EnqueueUnique produced duplicate rows")
}

func TestEnqueueUnique_PostgreSQL_AllowsAfterCompletion(t *testing.T) {
	skipIfNotPostgres(t)

	ctx := context.Background()
	s := newPostgresTestStorage(t)

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

	s := newPostgresTestStorage(t)
	assert.False(t, s.IsSQLite(), "PostgreSQL connection should not be detected as SQLite")
}
