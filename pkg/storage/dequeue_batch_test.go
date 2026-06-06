package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

func TestDequeueBatch_CorrectnessAndPausedQueues(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour).UTC()

	seed := []struct {
		id       string
		queue    string
		priority int
		created  time.Time
	}{
		{"job-01", "critical", 10, base.Add(0 * time.Second)},
		{"job-02", "default", 10, base.Add(1 * time.Second)},
		{"job-03", "critical", 9, base.Add(2 * time.Second)},
		{"job-04", "default", 8, base.Add(3 * time.Second)},
		{"job-05", "critical", 8, base.Add(4 * time.Second)},
		{"job-06", "default", 5, base.Add(5 * time.Second)},
		{"job-07", "critical", 4, base.Add(6 * time.Second)},
		{"job-08", "default", 3, base.Add(7 * time.Second)},
		{"job-09", "critical", 2, base.Add(8 * time.Second)},
		{"job-10", "default", 1, base.Add(9 * time.Second)},
	}
	for _, item := range seed {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID:        item.id,
			Type:      "work",
			Queue:     item.queue,
			Priority:  item.priority,
			Status:    core.StatusPending,
			CreatedAt: item.created,
		}).Error)
	}
	require.NoError(t, s.Enqueue(ctx, &core.Job{ID: "paused-01", Type: "work", Queue: "paused", Priority: 100}))
	require.NoError(t, s.PauseQueue(ctx, "paused"))

	got, err := s.DequeueBatch(ctx, []string{"default", "critical", "paused"}, "w1", 5)
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, []string{"job-01", "job-02", "job-03", "job-04", "job-05"}, jobIDs(got))

	for _, job := range got {
		assert.Equal(t, core.StatusRunning, job.Status)
		assert.Equal(t, "w1", job.LockedBy)
		assert.Equal(t, 1, job.Attempt)
		assert.NotNil(t, job.StartedAt)
		assert.NotNil(t, job.LockedUntil)
		stored, err := s.GetJob(ctx, job.ID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, core.StatusRunning, stored.Status)
		assert.Equal(t, "w1", stored.LockedBy)
		assert.Equal(t, 1, stored.Attempt)
	}

	got, err = s.DequeueBatch(ctx, []string{"default", "critical", "paused"}, "w1", 5)
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, []string{"job-06", "job-07", "job-08", "job-09", "job-10"}, jobIDs(got))

	got, err = s.DequeueBatch(ctx, []string{"default", "critical", "paused"}, "w1", 5)
	require.NoError(t, err)
	assert.Empty(t, got)

	paused, err := s.GetJob(ctx, "paused-01")
	require.NoError(t, err)
	require.NotNil(t, paused)
	assert.Equal(t, core.StatusPending, paused.Status)
	assert.Empty(t, paused.LockedBy)
}

func TestDequeueBatch_ConcurrentCallersDoNotDoubleDispatch(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()
	const totalJobs = 40
	const callers = 8
	const batchSize = 5

	for i := 0; i < totalJobs; i++ {
		require.NoError(t, s.Enqueue(ctx, &core.Job{
			ID:       fmt.Sprintf("job-%02d", i),
			Type:     "work",
			Queue:    "work",
			Priority: totalJobs - i,
		}))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan string, totalJobs)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			jobs, err := s.DequeueBatch(ctx, []string{"work"}, fmt.Sprintf("worker-%d", i), batchSize)
			if err != nil {
				errs <- err
				return
			}
			for _, job := range jobs {
				results <- job.ID
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	seen := map[string]bool{}
	for id := range results {
		require.False(t, seen[id], "job %s returned more than once", id)
		seen[id] = true
	}
	require.Len(t, seen, totalJobs)
}

func TestDequeueBatch_LimitClampAndEmptyLimit(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	got, err := s.DequeueBatch(ctx, []string{"default"}, "w1", 0)
	require.NoError(t, err)
	assert.Empty(t, got)

	require.NoError(t, s.Enqueue(ctx, &core.Job{Type: "work", Queue: "default"}))
	got, err = s.DequeueBatch(ctx, []string{"default"}, "w1", maxDequeueBatch+1)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func jobIDs(jobs []*core.Job) []string {
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.ID)
	}
	return ids
}
