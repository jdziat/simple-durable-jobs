package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// These two tests were written by an adversarial reviewer against my own round-11
// rework, and both failed on it. They are kept as-is in substance because a repro
// that caught a live defect is worth more than one rewritten to look tidy.
//
// gateCapStorage blocks inside TryAcquireConcurrencySlot AFTER the row has been
// joined, so a test can land a concurrent release in the window between "the DB
// row is joined" and "record() publishes this run into w.slotJobID".
type gateCapStorage struct {
	*capMockStorage
	gateFor string
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (s *gateCapStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error) {
	ok, err := s.capMockStorage.TryAcquireConcurrencySlot(ctx, slotName, jobID, workerID, limit, ttl)
	if slotName == s.gateFor && ok {
		s.once.Do(func() {
			close(s.entered)
			<-s.proceed
		})
	}
	return ok, err
}

// TestTenX_AcquireRecordWindowDropsALiveRow: run #2 has JOINED the row in the
// database but has not yet been published into w.slotJobID. Run #1's release
// scans the map, sees no other holder, and deletes the row run #2 is relying on.

func TestTryAcquireConcurrencySlots_PublishesOwnershipBeforeJoiningTheRow(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &gateCapStorage{
		capMockStorage: base,
		gateFor:        "customer:acme",
		entered:        make(chan struct{}),
		proceed:        make(chan struct{}),
	}
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	// Run #1 holds the row (acquired before the gate arms — drain the gate).
	tok1 := w.nextRunToken.Add(1)
	go func() { <-store.entered; close(store.proceed) }()
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, base.slotCount("customer:acme"))

	// Re-arm the gate for run #2.
	store.once = sync.Once{}
	store.entered = make(chan struct{})
	store.proceed = make(chan struct{})

	tok2 := w.nextRunToken.Add(1)
	done := make(chan bool, 1)
	go func() { done <- w.tryAcquireConcurrencySlots(ctx, job, tok2) }()

	<-store.entered // run #2 has JOINED the row; record() has not run yet.
	w.releaseConcurrencySlots(ctx, job.ID, tok1)
	close(store.proceed)
	require.True(t, <-done, "run #2 believes it acquired the cap slot")

	assert.Equal(t, 1, base.slotCount("customer:acme"),
		"run #2 joined this row before run #1 released; deleting it under a run that "+
			"believes it holds the fleet cap is the over-admission the fence exists to prevent")

	// Demonstrate the consequence: a DIFFERENT job with the same cap key is now
	// admitted past a limit of 1 while run #2 is still executing.
	other := runningJob("job-2", "acme")
	base.jobs["job-2"] = other
	tok3 := w.nextRunToken.Add(1)
	admitted := w.tryAcquireConcurrencySlots(ctx, other, tok3)
	assert.False(t, admitted,
		"a second job under the same cap key must NOT be admitted while run #2 holds the slot")
}

type gatedErrStorage struct {
	*capMockStorage
	failOn   string
	failFrom int
	failCall int
	arm      chan struct{}
	release  chan struct{}
	armed    bool
	mu       sync.Mutex
}

func (s *gatedErrStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error) {
	if slotName == s.failOn {
		s.mu.Lock()
		s.failCall++
		n := s.failCall
		park := s.armed
		if park {
			s.armed = false
		}
		s.mu.Unlock()
		if n >= s.failFrom {
			// Park HERE: run #2 has already recorded cap A and is inside the DB
			// call for cap B. This is where a slow/erroring second acquire sits.
			if park {
				close(s.arm)
				<-s.release
			}
			return false, errors.New("transient db error")
		}
	}
	return s.capMockStorage.TryAcquireConcurrencySlot(ctx, slotName, jobID, workerID, limit, ttl)
}

func TestReleaseConcurrencySlots_HandoverMergesNamesTheSuccessorDoesNotHold(t *testing.T) {
	job := runningJob("job-1", "acme")
	base := newCapMockStorage([]*core.Job{job})
	store := &gatedErrStorage{
		capMockStorage: base,
		failOn:         "region:acme",
		failFrom:       2,
		arm:            make(chan struct{}),
		release:        make(chan struct{}),
	}
	q := queue.New(store)
	q.Register(job.Type, func(context.Context, struct{}) error { return nil })
	w := NewWorker(q,
		WorkerQueue("default", Concurrency(3)),
		ConcurrencyCap("customer", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		ConcurrencyCap("region", 1, CapKey(func(j *core.Job) string { return j.UniqueKey })),
		DisableRetry(),
	)
	w.config.WorkerID = "worker-1"
	ctx := context.Background()

	tok1 := w.nextRunToken.Add(1)
	require.True(t, w.tryAcquireConcurrencySlots(ctx, job, tok1))
	require.Equal(t, 1, base.slotCount("customer:acme"))
	require.Equal(t, 1, base.slotCount("region:acme"))

	store.mu.Lock()
	store.armed = true
	store.mu.Unlock()
	tok2 := w.nextRunToken.Add(1)
	done := make(chan bool, 1)
	go func() { done <- w.tryAcquireConcurrencySlots(ctx, job, tok2) }()

	<-store.arm
	w.releaseConcurrencySlots(ctx, job.ID, tok1) // run #1's deferred cleanup lands
	close(store.release)

	require.False(t, <-done, "run #2's acquire must be refused: cap B errored")

	assert.Zero(t, base.slotCount("customer:acme"), "cap A is released by the rollback")
	assert.Zero(t, base.slotCount("region:acme"),
		"NOBODY holds cap B: run #1 handed the job id over and DROPPED this name, run #2 never "+
			"recorded it. The row survives to its TTL (45 min default), holding one slot of a "+
			"FLEET-WIDE cap against every worker in the deployment")

	w.slotJobIDMu.Lock()
	n := len(w.slotJobID)
	w.slotJobIDMu.Unlock()
	assert.Zero(t, n, "and no bookkeeping entry may outlive either run")
}
