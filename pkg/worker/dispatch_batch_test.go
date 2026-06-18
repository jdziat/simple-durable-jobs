package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
)

type batchMockStorage struct {
	*mockStorage
	mu         sync.Mutex
	batchJobs  []*core.Job
	batchLimit int
	batchCalls int
}

func (m *batchMockStorage) DequeueBatch(_ context.Context, _ []string, _ string, limit int) ([]*core.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchLimit = limit
	m.batchCalls++
	return m.batchJobs, nil
}

type perQueueMockStorage struct {
	*batchMockStorage
	perQueueJobs    []*core.Job
	perQueueBudgets map[string]int
	perQueueCalls   int
}

func (m *perQueueMockStorage) DequeueBatchPerQueue(_ context.Context, _ string, budgets map[string]int) ([]*core.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.perQueueCalls++
	m.perQueueBudgets = make(map[string]int, len(budgets))
	for queueName, budget := range budgets {
		m.perQueueBudgets[queueName] = budget
	}
	return m.perQueueJobs, nil
}

type releaseStateStorage struct {
	*mockStorage
	mu   sync.Mutex
	jobs map[string]*core.Job
}

func (s *releaseStateStorage) Release(_ context.Context, jobID core.UUID, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[string(jobID)]
	if job == nil {
		return core.ErrJobNotOwned
	}
	if job.LockedBy != workerID || job.Status != core.StatusRunning {
		return core.ErrJobNotOwned
	}
	job.Status = core.StatusPending
	job.LockedBy = ""
	job.LockedUntil = nil
	job.StartedAt = nil
	if job.Attempt > 0 {
		job.Attempt--
	}
	return s.mockStorage.Release(context.Background(), jobID, workerID)
}

func TestWorkerBatchDequeueRequestsFreeCapacityBoundedByOption(t *testing.T) {
	store := &batchMockStorage{
		mockStorage: &mockStorage{},
		batchJobs: []*core.Job{
			{ID: "job-1", Type: "work", Queue: "default"},
			{ID: "job-2", Type: "work", Queue: "default"},
		},
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(5)),
		WithDequeueBatchSize(3),
		DisableRetry(),
	)
	w.trackQueueJob("running-1", "default")

	jobs, err := w.dequeueAvailableJobs(context.Background(), []string{"default"}, 5)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	assert.Equal(t, 3, store.batchLimit)
	assert.Equal(t, 1, store.batchCalls)
}

func TestWorkerBatchDequeuePrefersPerQueueBudgets(t *testing.T) {
	store := &perQueueMockStorage{
		batchMockStorage: &batchMockStorage{mockStorage: &mockStorage{}},
		perQueueJobs: []*core.Job{
			{ID: "job-1", Type: "work", Queue: "hot"},
			{ID: "job-2", Type: "work", Queue: "idle"},
		},
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("hot", Concurrency(3)),
		WorkerQueue("idle", Concurrency(2)),
		WithDequeueBatchSize(2),
		DisableRetry(),
	)
	w.trackQueueJob("running-1", "hot")
	w.trackQueueJob("running-2", "hot")

	jobs, err := w.dequeueAvailableJobs(context.Background(), []string{"hot", "idle"}, 5)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	assert.Equal(t, 1, store.perQueueCalls)
	assert.Equal(t, 0, store.batchCalls)
	assert.Equal(t, map[string]int{"hot": 1, "idle": 1}, store.perQueueBudgets)
}

func TestWorkerDequeueQueueBudgetsIncludesEveryQueueWhenSlotsConstrained(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}),
		WorkerQueue("alpha", Concurrency(10)),
		WorkerQueue("beta", Concurrency(10)),
		WorkerQueue("gamma", Concurrency(10)),
		DisableRetry(),
	)

	budgets := w.dequeueQueueBudgets(w.queuesWithCapacity(), 5)

	require.Len(t, budgets, 3)
	total := 0
	for _, queueName := range []string{"alpha", "beta", "gamma"} {
		budget, ok := budgets[queueName]
		require.True(t, ok, "queue %q was omitted", queueName)
		assert.Positive(t, budget, "queue %q had no budget", queueName)
		assert.LessOrEqual(t, budget, 10)
		total += budget
	}
	assert.Equal(t, 5, total)
}

func TestWorkerBatchDequeueFallsBackWhenStorageLacksCapability(t *testing.T) {
	store := &mockStorage{
		dequeueFunc: func(_ context.Context, _ []string, _ string) (*core.Job, error) {
			return &core.Job{ID: "job-1", Type: "work", Queue: "default"}, nil
		},
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(4)),
		WithDequeueBatchSize(4),
		DisableRetry(),
	)

	jobs, err := w.dequeueAvailableJobs(context.Background(), []string{"default"}, 4)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, core.UUID("job-1"), jobs[0].ID)
}

func TestWorkerBatchDispatchReleasesUndeliveredJobsOnShutdown(t *testing.T) {
	now := time.Now()
	jobs := []*core.Job{
		{ID: "job-1", Type: "work", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-1", LockedUntil: &now, StartedAt: &now, Attempt: 1},
		{ID: "job-2", Type: "work", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-1", LockedUntil: &now, StartedAt: &now, Attempt: 1},
		{ID: "job-3", Type: "work", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-1", LockedUntil: &now, StartedAt: &now, Attempt: 1},
	}
	store := &releaseStateStorage{
		mockStorage: &mockStorage{},
		jobs: map[string]*core.Job{
			"job-1": jobs[0],
			"job-2": jobs[1],
			"job-3": jobs[2],
		},
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(3)),
		WithDequeueBatchSize(3),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobsChan := make(chan *core.Job, 1)
	cancelled := make(chan struct{})
	go func() {
		for {
			if len(jobsChan) == 1 {
				cancel()
				close(cancelled)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	w.dispatchDequeuedJobs(ctx, jobsChan, jobs)
	<-cancelled

	delivered := <-jobsChan
	assert.Equal(t, core.UUID("job-1"), delivered.ID)
	assert.Equal(t, core.StatusRunning, jobs[0].Status)
	assert.Equal(t, "worker-1", jobs[0].LockedBy)

	for _, job := range jobs[1:] {
		assert.Equal(t, core.StatusPending, job.Status)
		assert.Empty(t, job.LockedBy)
		assert.Nil(t, job.LockedUntil)
		assert.Nil(t, job.StartedAt)
		assert.Equal(t, 0, job.Attempt)
	}
	assert.ElementsMatch(t, []core.UUID{core.UUID("job-2"), core.UUID("job-3")}, store.getReleasedJobIDs())
	assert.Equal(t, 1, int(w.queueRunning["default"].Load()))
}

func TestWithDequeueBatchSizeClamps(t *testing.T) {
	var low WorkerConfig
	WithDequeueBatchSize(0).ApplyWorker(&low)
	assert.Equal(t, 1, low.DequeueBatchSize)

	var high WorkerConfig
	WithDequeueBatchSize(maxDequeueBatch + 1).ApplyWorker(&high)
	assert.Equal(t, maxDequeueBatch, high.DequeueBatchSize)

	w := NewWorker(queue.New(&mockStorage{}))
	assert.Equal(t, 50, w.config.DequeueBatchSize)
}
