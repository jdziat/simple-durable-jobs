package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func TestDequeueBatch_CorrectnessAndPausedQueues(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour) // local repr, matching autoCreateTime; see P3 (COALESCE eligibility is tz-lexical on sqlite)

	seed := []struct {
		id       core.UUID
		queue    string
		priority int
		created  time.Time
	}{
		{core.NewID(), "critical", 10, base.Add(0 * time.Second)},
		{core.NewID(), "default", 10, base.Add(1 * time.Second)},
		{core.NewID(), "critical", 9, base.Add(2 * time.Second)},
		{core.NewID(), "default", 8, base.Add(3 * time.Second)},
		{core.NewID(), "critical", 8, base.Add(4 * time.Second)},
		{core.NewID(), "default", 5, base.Add(5 * time.Second)},
		{core.NewID(), "critical", 4, base.Add(6 * time.Second)},
		{core.NewID(), "default", 3, base.Add(7 * time.Second)},
		{core.NewID(), "critical", 2, base.Add(8 * time.Second)},
		{core.NewID(), "default", 1, base.Add(9 * time.Second)},
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
	pausedID := core.NewID()
	require.NoError(t, s.Enqueue(ctx, &core.Job{ID: pausedID, Type: "work", Queue: "paused", Priority: 100}))
	require.NoError(t, s.PauseQueue(ctx, "paused"))

	got, err := s.DequeueBatch(ctx, []string{"default", "critical", "paused"}, "w1", 5)
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, []string{
		string(seed[0].id),
		string(seed[1].id),
		string(seed[2].id),
		string(seed[3].id),
		string(seed[4].id),
	}, jobIDs(got))

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
	assert.Equal(t, []string{
		string(seed[5].id),
		string(seed[6].id),
		string(seed[7].id),
		string(seed[8].id),
		string(seed[9].id),
	}, jobIDs(got))

	got, err = s.DequeueBatch(ctx, []string{"default", "critical", "paused"}, "w1", 5)
	require.NoError(t, err)
	assert.Empty(t, got)

	paused, err := s.GetJob(ctx, pausedID)
	require.NoError(t, err)
	require.NotNil(t, paused)
	assert.Equal(t, core.StatusPending, paused.Status)
	assert.Empty(t, paused.LockedBy)
}

func TestDequeueBatchPerQueue_SkipsQueuesAtBudgetWithoutOverClaiming(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour) // local repr, matching autoCreateTime; see P3 (COALESCE eligibility is tz-lexical on sqlite)

	seed := []struct {
		id       core.UUID
		queue    string
		priority int
		created  time.Time
	}{
		{core.NewID(), "hot", 100, base.Add(0 * time.Second)},
		{core.NewID(), "hot", 99, base.Add(1 * time.Second)},
		{core.NewID(), "hot", 98, base.Add(2 * time.Second)},
		{core.NewID(), "idle", 90, base.Add(3 * time.Second)},
		{core.NewID(), "idle", 89, base.Add(4 * time.Second)},
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

	got, err := s.DequeueBatchPerQueue(ctx, "w1", map[string]int{"hot": 1, "idle": 2})
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []string{string(seed[0].id), string(seed[3].id), string(seed[4].id)}, jobIDs(got))

	for _, id := range []core.UUID{seed[1].id, seed[2].id} {
		stored, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, core.StatusPending, stored.Status)
		assert.Empty(t, stored.LockedBy)
		assert.Equal(t, 0, stored.Attempt)
	}
	for _, id := range []core.UUID{seed[0].id, seed[3].id, seed[4].id} {
		stored, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, core.StatusRunning, stored.Status)
		assert.Equal(t, "w1", stored.LockedBy)
		assert.Equal(t, 1, stored.Attempt)
	}
}

func TestDequeueBatch_ConcurrentCallersDoNotDoubleDispatch(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()
	const totalJobs = 40
	const callers = 8
	const batchSize = 5

	for i := 0; i < totalJobs; i++ {
		require.NoError(t, s.Enqueue(ctx, &core.Job{
			ID:       core.NewID(),
			Type:     "work",
			Queue:    "work",
			Priority: totalJobs - i,
		}))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan core.UUID, totalJobs)
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
	seen := map[core.UUID]bool{}
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
		ids = append(ids, string(job.ID))
	}
	return ids
}
