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

type capMockStorage struct {
	*releaseStateStorage
	mu      sync.Mutex
	slots   map[string]map[string]bool
	renewed []string
}

func newCapMockStorage(jobs []*core.Job) *capMockStorage {
	byID := make(map[string]*core.Job, len(jobs))
	for _, job := range jobs {
		byID[string(job.ID)] = job
	}
	return &capMockStorage{
		releaseStateStorage: &releaseStateStorage{
			mockStorage: &mockStorage{},
			jobs:        byID,
		},
		slots: map[string]map[string]bool{},
	}
}

func (s *capMockStorage) TryAcquireConcurrencySlot(_ context.Context, slotName string, jobID core.UUID, _ string, limit int, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slots[slotName] == nil {
		s.slots[slotName] = map[string]bool{}
	}
	if s.slots[slotName][string(jobID)] {
		return true, nil
	}
	if len(s.slots[slotName]) >= limit {
		return false, nil
	}
	s.slots[slotName][string(jobID)] = true
	return true, nil
}

func (s *capMockStorage) ReleaseConcurrencySlot(_ context.Context, slotName string, jobID core.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.slots[slotName], string(jobID))
	return nil
}

// RenewConcurrencySlot is renew-only: it refreshes an already-held slot and
// never creates one, mirroring GormStorage. It records each (slotName, jobID)
// renewal so a test can assert renewConcurrencySlots drove it.
func (s *capMockStorage) RenewConcurrencySlot(_ context.Context, slotName string, jobID core.UUID, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewed = append(s.renewed, slotName+":"+string(jobID))
	if s.slots[slotName] != nil && s.slots[slotName][string(jobID)] {
		return true, nil
	}
	return false, nil
}

func (s *capMockStorage) renewals() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.renewed...)
}

func (s *capMockStorage) slotCount(slotName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slots[slotName])
}

func runningJob(id, customer string) *core.Job {
	now := time.Now()
	return &core.Job{
		ID:          core.UUID(id),
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
		delivered = append(delivered, string((<-jobsChan).ID))
	}
	assert.ElementsMatch(t, []string{"job-a1", "job-a2", "job-b1"}, delivered)
	assert.ElementsMatch(t, []core.UUID{core.UUID("job-a3")}, store.getReleasedJobIDs())
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

func TestConcurrencySlotTTLFloorsToHeartbeatWindow(t *testing.T) {
	store := newCapMockStorage(nil)

	small := NewWorker(queue.New(store), WithLockDuration(5*time.Second))
	require.Equal(t, 2*time.Minute, small.heartbeatInterval)
	assert.Equal(t, 3*small.heartbeatInterval, small.concurrencySlotTTL())

	largeTTL := 10 * time.Minute
	large := NewWorker(queue.New(store), WithLockDuration(largeTTL))
	assert.Equal(t, largeTTL, large.concurrencySlotTTL())
}

func TestWorkerConcurrencyCapPanickingKeyBouncesAndReleasesSlots(t *testing.T) {
	job := runningJob("job-1", "a")
	store := newCapMockStorage([]*core.Job{job})
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(1)),
		ConcurrencyCap("customer", 1, CapKey(func(job *core.Job) string {
			return job.UniqueKey
		})),
		ConcurrencyCap("panic-cap", 1, CapKey(func(*core.Job) string {
			panic("bad cap key")
		})),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 1)
	assert.NotPanics(t, func() {
		dispatched, released := w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job})
		assert.Equal(t, 0, dispatched)
		assert.Equal(t, 1, released)
	})

	assert.Empty(t, jobsChan, "job with panicking CapKey must be bounced, not dispatched")
	assert.ElementsMatch(t, []core.UUID{job.ID}, store.getReleasedJobIDs())
	assert.Equal(t, 0, store.slotCount("customer:a"), "already-acquired slots must be released after CapKey panic")
	assert.Equal(t, 0, store.slotCount("panic-cap"), "panicking cap must not leak a slot")
	assert.Equal(t, core.StatusPending, job.Status)
	assert.Empty(t, job.LockedBy)
}

// TestRenewConcurrencySlots covers the heartbeat-driven renewal path:
// renewConcurrencySlots must renew (never recreate) every slot a live job
// holds, and skip cleanly when the job holds none.
func TestRenewConcurrencySlots(t *testing.T) {
	job := runningJob("job-1", "a")
	store := newCapMockStorage([]*core.Job{job})
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(1)),
		ConcurrencyCap("customer", 1, CapKey(func(job *core.Job) string { return job.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	// Acquire the slot and record it the way dispatch does, then renew.
	ok, err := store.TryAcquireConcurrencySlot(context.Background(), "customer:a", "job-1", "worker-1", 1, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	w.slotJobIDMu.Lock()
	w.slotJobID["job-1"] = []string{"customer:a"}
	w.slotJobIDMu.Unlock()

	w.renewConcurrencySlots(context.Background(), "job-1")
	assert.Equal(t, []string{"customer:a:job-1"}, store.renewals(), "the held slot must be renewed exactly once")
	assert.Equal(t, 1, store.slotCount("customer:a"), "renewal must not drop or duplicate the slot")

	// A job holding no slots is a no-op (no renewal attempted).
	w.renewConcurrencySlots(context.Background(), "job-unknown")
	assert.Equal(t, []string{"customer:a:job-1"}, store.renewals())
}
