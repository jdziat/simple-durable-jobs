package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
)

type capMockStorage struct {
	*releaseStateStorage
	mu    sync.Mutex
	slots map[string]map[string]bool
}

func newCapMockStorage(jobs []*core.Job) *capMockStorage {
	byID := make(map[string]*core.Job, len(jobs))
	for _, job := range jobs {
		byID[job.ID] = job
	}
	return &capMockStorage{
		releaseStateStorage: &releaseStateStorage{
			mockStorage: &mockStorage{},
			jobs:        byID,
		},
		slots: map[string]map[string]bool{},
	}
}

func (s *capMockStorage) TryAcquireConcurrencySlot(_ context.Context, slotName, jobID, _ string, limit int, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slots[slotName] == nil {
		s.slots[slotName] = map[string]bool{}
	}
	if s.slots[slotName][jobID] {
		return true, nil
	}
	if len(s.slots[slotName]) >= limit {
		return false, nil
	}
	s.slots[slotName][jobID] = true
	return true, nil
}

func (s *capMockStorage) ReleaseConcurrencySlot(_ context.Context, slotName, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.slots[slotName], jobID)
	return nil
}

func (s *capMockStorage) slotCount(slotName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slots[slotName])
}

func runningJob(id, customer string) *core.Job {
	now := time.Now()
	return &core.Job{
		ID:          id,
		Type:        "work",
		Queue:       "default",
		Status:      core.StatusRunning,
		LockedBy:    "worker-1",
		LockedUntil: &now,
		StartedAt:   &now,
		Attempt:     1,
		UniqueKey:   customer,
	}
}

func TestWorkerConcurrencyCapPerKeyAdmission(t *testing.T) {
	jobs := []*core.Job{
		runningJob("job-a1", "a"),
		runningJob("job-a2", "a"),
		runningJob("job-a3", "a"),
		runningJob("job-b1", "b"),
	}
	store := newCapMockStorage(jobs)
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(4)),
		ConcurrencyCap("customer", 2, CapKey(func(job *core.Job) string {
			return job.UniqueKey
		})),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, len(jobs))
	w.dispatchDequeuedJobs(context.Background(), jobsChan, jobs)

	delivered := make([]string, 0, len(jobsChan))
	for len(jobsChan) > 0 {
		delivered = append(delivered, (<-jobsChan).ID)
	}
	assert.ElementsMatch(t, []string{"job-a1", "job-a2", "job-b1"}, delivered)
	assert.ElementsMatch(t, []string{"job-a3"}, store.getReleasedJobIDs())
	assert.Equal(t, 2, store.slotCount("customer:a"))
	assert.Equal(t, 1, store.slotCount("customer:b"))

	released := jobs[2]
	assert.Equal(t, core.StatusPending, released.Status)
	assert.Empty(t, released.LockedBy)
	assert.Nil(t, released.LockedUntil)
	assert.Nil(t, released.StartedAt)
	assert.Equal(t, 0, released.Attempt)
}

func TestWorkerConcurrencyCapReleasesOnCompletion(t *testing.T) {
	job := runningJob("job-1", "a")
	store := newCapMockStorage([]*core.Job{job})
	q := queue.New(store)
	q.Register("work", func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(1)),
		ConcurrencyCap("customer", 1, CapKey(func(job *core.Job) string {
			return job.UniqueKey
		})),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	require.True(t, w.tryAcquireConcurrencySlots(context.Background(), job))
	assert.Equal(t, 1, store.slotCount("customer:a"))

	job.Args = []byte(`{}`)
	w.processJob(context.Background(), job)
	assert.Equal(t, 0, store.slotCount("customer:a"))
}
