package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
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

type releaseStateStorage struct {
	*mockStorage
	mu   sync.Mutex
	jobs map[string]*core.Job
}

func (s *releaseStateStorage) Release(_ context.Context, jobID, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[jobID]
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
	assert.Equal(t, "job-1", jobs[0].ID)
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
	assert.Equal(t, "job-1", delivered.ID)
	assert.Equal(t, core.StatusRunning, jobs[0].Status)
	assert.Equal(t, "worker-1", jobs[0].LockedBy)

	for _, job := range jobs[1:] {
		assert.Equal(t, core.StatusPending, job.Status)
		assert.Empty(t, job.LockedBy)
		assert.Nil(t, job.LockedUntil)
		assert.Nil(t, job.StartedAt)
		assert.Equal(t, 0, job.Attempt)
	}
	assert.ElementsMatch(t, []string{"job-2", "job-3"}, store.getReleasedJobIDs())
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
	assert.Equal(t, 1, w.config.DequeueBatchSize)
}
