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

func TestTokenBucketBurstAndRefill(t *testing.T) {
	now := time.Unix(100, 0)
	bucket := newTokenBucket(2, 3, now)
	require.NotNil(t, bucket)

	assert.True(t, bucket.tryConsume(now))
	assert.True(t, bucket.tryConsume(now))
	assert.True(t, bucket.tryConsume(now))
	assert.False(t, bucket.tryConsume(now))

	assert.True(t, bucket.tryConsume(now.Add(500*time.Millisecond)), "2/sec refills one token in 500ms")
	assert.False(t, bucket.tryConsume(now.Add(500*time.Millisecond)))

	assert.True(t, bucket.tryConsume(now.Add(time.Second)))
}

func TestQueueRateLimitSkipsDequeueWhenBucketEmpty(t *testing.T) {
	var dequeueCalls int
	store := &mockStorage{
		dequeueFunc: func(_ context.Context, _ []string, _ string) (*core.Job, error) {
			dequeueCalls++
			return nil, nil
		},
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("limited", Concurrency(1)),
		WithQueueRateLimit("limited", 1, 1),
		DisableRetry(),
	)

	assert.True(t, w.tryConsumeQueueRateLimit("limited"))
	available := w.queuesWithCapacity()
	assert.Empty(t, available)

	if len(available) > 0 {
		_, _ = w.dequeueAvailableJobs(context.Background(), available, 1)
	}
	assert.Equal(t, 0, dequeueCalls)
}

func TestQueueRateLimitDoesNotConsumeUnconfiguredQueues(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}),
		WorkerQueue("default", Concurrency(1)),
		WorkerQueue("other", Concurrency(1)),
		WithQueueRateLimit("default", 1, 1),
		DisableRetry(),
	)

	assert.True(t, w.tryConsumeQueueRateLimit("other"))
	assert.True(t, w.tryConsumeQueueRateLimit("other"))
	assert.True(t, w.tryConsumeQueueRateLimit("other"))
}

type rateMockStorage struct {
	*releaseStateStorage
	mu            sync.Mutex
	remaining     int
	seenNames     []string
	consumedNames []string
}

func newRateMockStorage(job *core.Job, denies int) *rateMockStorage {
	return &rateMockStorage{
		releaseStateStorage: &releaseStateStorage{
			mockStorage: &mockStorage{},
			jobs:        map[string]*core.Job{job.ID: job},
		},
		remaining: denies,
	}
}

func (s *rateMockStorage) TryConsumeRate(_ context.Context, limitName string, _ float64, _ time.Duration, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seenNames = append(s.seenNames, limitName)
	if s.remaining > 0 {
		s.remaining--
		return false, nil
	}
	s.consumedNames = append(s.consumedNames, limitName)
	return true, nil
}

func TestWorkerRateLimitPerKeyAdmissionReleasesDeniedJob(t *testing.T) {
	job := runningJob("job-1", "tenant-a")
	store := newRateMockStorage(job, 1)
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(1)),
		RateLimit("llm", 1, RateLimitKey(func(job *core.Job) string {
			return job.UniqueKey
		})),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 1)
	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job})

	assert.Empty(t, jobsChan)
	assert.Equal(t, []string{"job-1"}, store.getReleasedJobIDs())
	assert.Equal(t, core.StatusPending, job.Status)
	assert.Equal(t, 0, job.Attempt)
	assert.Equal(t, []string{"llm:tenant-a"}, store.seenNames)
}

func TestWorkerQueueRateLimitDenialDoesNotConsumeFleetRateLimit(t *testing.T) {
	job1 := runningJob("job-1", "tenant-a")
	job1.Queue = "x"
	job2 := runningJob("job-2", "tenant-b")
	job2.Queue = "x"
	store := newRateMockStorage(job1, 0)
	store.jobs[job2.ID] = job2
	w := NewWorker(queue.New(store),
		WorkerQueue("x", Concurrency(2)),
		RateLimit("fleet", 100),
		WithQueueRateLimit("x", 1, 1),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 2)
	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job1, job2})

	require.Len(t, jobsChan, 1)
	assert.Equal(t, "job-1", (<-jobsChan).ID)
	assert.Equal(t, []string{"job-2"}, store.getReleasedJobIDs())
	assert.LessOrEqual(t, len(store.seenNames), 1)
	assert.Equal(t, []string{"fleet"}, store.seenNames)
}

func TestWorkerQueueRateLimitRefundsTokenAfterFleetDeny(t *testing.T) {
	job1 := runningJob("job-1", "tenant-a")
	job1.Queue = "x"
	job2 := runningJob("job-2", "tenant-b")
	job2.Queue = "x"
	store := newRateMockStorage(job1, 1)
	store.jobs[job2.ID] = job2
	w := NewWorker(queue.New(store),
		WorkerQueue("x", Concurrency(1)),
		RateLimit("fleet", 1),
		WithQueueRateLimit("x", 1, 1),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 1)
	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job1})
	require.Empty(t, jobsChan)
	assert.Equal(t, []string{"job-1"}, store.getReleasedJobIDs())

	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job2})
	require.Len(t, jobsChan, 1, "refunded queue token should admit the next job immediately")
	assert.Equal(t, "job-2", (<-jobsChan).ID)
	// The first fleet check is denied and does not increment the fixed-window
	// counter; only the successfully dispatched second job consumes a fleet unit.
	assert.Equal(t, []string{"fleet"}, store.consumedNames)
	assert.Equal(t, []string{"fleet", "fleet"}, store.seenNames)
}

func TestWorkerConcurrencySlotDenialDoesNotConsumeFleetRateLimit(t *testing.T) {
	job1 := runningJob("job-1", "tenant-a")
	job2 := runningJob("job-2", "tenant-b")
	store := &capRateMockStorage{
		capMockStorage: newCapMockStorage([]*core.Job{job1, job2}),
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(2)),
		ConcurrencyCap("global", 1),
		RateLimit("fleet", 100),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 2)
	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job1, job2})

	require.Len(t, jobsChan, 1)
	assert.Equal(t, "job-1", (<-jobsChan).ID)
	assert.Equal(t, []string{"job-2"}, store.getReleasedJobIDs())
	assert.Equal(t, []string{"fleet"}, store.seenNames, "fleet rate is the last gate, so slot-denied jobs are never checked")
	assert.Equal(t, []string{"fleet"}, store.consumedNames, "only the dispatched job consumes a fleet unit")
}

type capRateMockStorage struct {
	*capMockStorage
	rateMu        sync.Mutex
	seenNames     []string
	consumedNames []string
}

func (s *capRateMockStorage) TryConsumeRate(_ context.Context, limitName string, _ float64, _ time.Duration, _ time.Time) (bool, error) {
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	s.seenNames = append(s.seenNames, limitName)
	s.consumedNames = append(s.consumedNames, limitName)
	return true, nil
}

func TestWorkerRateLimitUnsupportedStorageIsNoop(t *testing.T) {
	job := runningJob("job-1", "tenant-a")
	store := &releaseStateStorage{
		mockStorage: &mockStorage{},
		jobs:        map[string]*core.Job{job.ID: job},
	}
	w := NewWorker(queue.New(store),
		WorkerQueue("default", Concurrency(1)),
		RateLimit("llm", 1),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 1)
	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job})

	require.Len(t, jobsChan, 1)
	assert.Equal(t, "job-1", (<-jobsChan).ID)
	assert.Empty(t, store.getReleasedJobIDs())
}

func TestWorkerRateLimitThrottleDoesNotBurnAttemptsOrFailHooks(t *testing.T) {
	job := runningJob("job-1", "tenant-a")
	job.Args = []byte(`{}`)
	store := newRateMockStorage(job, 3)
	q := queue.New(store)
	q.Register("work", func(context.Context, struct{}) error { return nil })
	var failures int
	q.OnJobFail(func(context.Context, *core.Job, error) {
		failures++
	})
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(1)),
		RateLimit("llm", 1),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"

	jobsChan := make(chan *core.Job, 1)
	for i := 0; i < 3; i++ {
		job.Status = core.StatusRunning
		job.LockedBy = "worker-1"
		job.Attempt = 1
		w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job})
		require.Empty(t, jobsChan)
		assert.Equal(t, 0, job.Attempt)
	}

	job.Status = core.StatusRunning
	job.LockedBy = "worker-1"
	job.Attempt = 1
	w.dispatchDequeuedJobs(context.Background(), jobsChan, []*core.Job{job})
	require.Len(t, jobsChan, 1)
	admitted := <-jobsChan
	assert.Equal(t, 1, admitted.Attempt)

	w.processJob(context.Background(), admitted)
	assert.Equal(t, 0, failures)
	assert.Len(t, store.getReleasedJobIDs(), 3)
}

func TestRateLimitInvalidOptionsAreNoop(t *testing.T) {
	var cfg WorkerConfig
	RateLimit("", 1).ApplyWorker(&cfg)
	RateLimit("x", 0).ApplyWorker(&cfg)
	WithQueueRateLimit("", 1, 1).ApplyWorker(&cfg)
	WithQueueRateLimit("q", 0, 1).ApplyWorker(&cfg)
	WithQueueRateLimit("q", 1, 0).ApplyWorker(&cfg)

	assert.Empty(t, cfg.RateLimits)
	assert.Empty(t, cfg.QueueRateLimits)
}
