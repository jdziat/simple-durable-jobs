package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

type deterministicDequeueStorage struct {
	*mockStorage
	mu   sync.Mutex
	jobs []*core.Job
}

func newDeterministicDequeueStorage(n int) *deterministicDequeueStorage {
	jobs := make([]*core.Job, 0, n)
	for i := 0; i < n; i++ {
		jobs = append(jobs, &core.Job{
			ID:     fmt.Sprintf("job-%04d", i),
			Type:   "work",
			Queue:  "default",
			Args:   []byte(`{}`),
			Status: core.StatusRunning,
		})
	}
	return &deterministicDequeueStorage{
		mockStorage: &mockStorage{},
		jobs:        jobs,
	}
}

func (s *deterministicDequeueStorage) Dequeue(_ context.Context, _ []string, _ string) (*core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) == 0 {
		return nil, nil
	}
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	return job, nil
}

func newDeterministicDispatchWorker(n int) (*Worker, chan *core.Job) {
	store := newDeterministicDequeueStorage(n)
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(n)),
		WithDequeueBatchSize(1),
		DisableRetry(),
	)
	return w, make(chan *core.Job, n)
}

func runDrainOnTicks(t testing.TB, n int) (ticks int, dispatched int) {
	t.Helper()
	w, jobsChan := newDeterministicDispatchWorker(n)
	w.drainDequeuedJobs(context.Background(), jobsChan, n)
	return 1, len(jobsChan)
}

func runDrainOffTicks(t testing.TB, n int) (ticks int, dispatched int) {
	t.Helper()
	w, jobsChan := newDeterministicDispatchWorker(n)
	ctx := context.Background()

	for dispatched < n {
		availableQueues := w.queuesWithCapacity()
		require.NotEmpty(t, availableQueues)
		jobs, err := w.dequeueAvailableJobs(ctx, availableQueues, n)
		require.NoError(t, err)
		sent, _ := w.dispatchDequeuedJobs(ctx, jobsChan, jobs)
		ticks++
		if sent == 0 {
			break
		}
		dispatched += sent
	}
	return ticks, dispatched
}

func TestWorkConservingDrainProcessesMoreJobsPerTick(t *testing.T) {
	const totalJobs = 50

	drainOnTicks, drainOnJobs := runDrainOnTicks(t, totalJobs)
	drainOffTicks, drainOffJobs := runDrainOffTicks(t, totalJobs)

	require.Equal(t, totalJobs, drainOnJobs)
	require.Equal(t, totalJobs, drainOffJobs)
	assert.Less(t, drainOnTicks, drainOffTicks,
		"work-conserving drain should finish the same deterministic workload in fewer ticks")
	assert.Greater(t, drainOnJobs/drainOnTicks, drainOffJobs/drainOffTicks,
		"work-conserving drain should process more jobs per tick at the same DequeueBatchSize")
}

func BenchmarkWorkConservingDrain(b *testing.B) {
	const totalJobs = 100
	for i := 0; i < b.N; i++ {
		_, dispatched := runDrainOnTicks(b, totalJobs)
		if dispatched != totalJobs {
			b.Fatalf("dispatched %d jobs, want %d", dispatched, totalJobs)
		}
	}
}

func BenchmarkSingleDequeuePerTick(b *testing.B) {
	const totalJobs = 100
	for i := 0; i < b.N; i++ {
		_, dispatched := runDrainOffTicks(b, totalJobs)
		if dispatched != totalJobs {
			b.Fatalf("dispatched %d jobs, want %d", dispatched, totalJobs)
		}
	}
}
