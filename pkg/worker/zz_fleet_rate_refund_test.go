package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/require"
)

// newRateLimitedWorker builds a worker with one whole-number fleet rate limit
// against the supplied storage, so tryConsumeRateLimits takes the windowed path.
func newRateLimitedWorker(t *testing.T, store core.Storage) *Worker {
	t.Helper()
	return NewWorker(queue.New(store), DisableRetry(), RateLimit("fleet", 100))
}

// recordingRateStorage records every fleet-rate consume and release, and whether
// the release arrived on a context that was already cancelled.
type recordingRateStorage struct {
	*mockStorage
	mu        sync.Mutex
	consumed  int
	released  int
	relCtxErr []error
}

func (s *recordingRateStorage) TryConsumeRate(context.Context, string, float64, time.Duration, time.Time) (bool, error) {
	return true, nil
}

func (s *recordingRateStorage) TryConsumeRateWindow(_ context.Context, _ string, _ float64, _ time.Duration, now time.Time) (bool, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumed++
	return true, now, nil
}

func (s *recordingRateStorage) ReleaseRateAt(ctx context.Context, _ string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released++
	s.relCtxErr = append(s.relCtxErr, ctx.Err())
	return nil
}

func (s *recordingRateStorage) counts() (int, int, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumed, s.released, append([]error(nil), s.relCtxErr...)
}

// The fleet-rate refund used to issue its DB write on the CALLER's context. The
// refund exists for two situations: a later limit denied, and the dispatcher is
// shutting down — and the second means that context is ALREADY cancelled, so the
// write failed exactly when it was needed. The unit then stayed consumed by a job
// that never ran, and the fleet under-admitted for the rest of the window.
//
// Detaching it is the same fix, and for the same reason, as the concurrency-slot
// release. This asserts the release reaches storage on a LIVE context even when the
// caller's is already cancelled.
func TestFleetRateRefund_IssuedOnADetachedContext(t *testing.T) {
	store := &recordingRateStorage{mockStorage: &mockStorage{}}
	w := newRateLimitedWorker(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	job := &core.Job{ID: core.NewID(), Type: "t", Queue: "default"}

	ok, _, refund := w.tryConsumeRateLimits(ctx, job)
	require.True(t, ok, "premise: the limits must admit the job, or there is nothing to refund")

	// The dispatcher is shutting down: the caller's context is cancelled BEFORE the
	// refund runs. This is the ordinary shape, not a contrived one.
	cancel()
	refund()

	consumed, released, ctxErrs := store.counts()
	require.Positive(t, consumed, "premise: a unit must have been consumed")
	require.Equal(t, consumed, released,
		"every consumed fleet-rate unit must be refunded when the job is released unrun")
	for _, err := range ctxErrs {
		require.NoError(t, err,
			"the refund reached storage on an already-cancelled context, so in production the write fails precisely when the shutdown bail-out needs it")
	}
}

// A job released back to pending by the shutdown bail-out must give back EVERY
// admission unit it took. The queue token was already refunded there; the fleet
// unit was not, so a bail-out permanently spent fleet budget on a job that never
// ran.
func TestDispatch_ShutdownBailOutRefundsTheFleetRateUnit(t *testing.T) {
	store := &recordingRateStorage{mockStorage: &mockStorage{}}
	w := newRateLimitedWorker(t, store)

	// The context must be LIVE when the rate limit is consumed and cancelled only
	// once the send blocks — that is the production shape, and it is the only way
	// to reach the ctx.Done() arm of the select. Pre-cancelling instead trips the
	// earlier ctx.Err() guard and never consumes anything, which the premise below
	// would then catch.
	jobsChan := make(chan dispatchedJob) // unbuffered, nobody reads
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	jobs := []*core.Job{{ID: core.NewID(), Type: "t", Queue: "default"}}
	_, released := w.dispatchDequeuedJobs(ctx, jobsChan, jobs)
	require.Equal(t, 1, released, "premise: the job must take the shutdown bail-out")

	consumed, refunded, _ := store.counts()
	require.Positive(t, consumed,
		"premise: a fleet-rate unit must have been consumed, or comparing 0 refunds to 0 consumes passes vacuously")
	require.Equal(t, consumed, refunded,
		"the shutdown bail-out released the job to pending UNRUN but kept its fleet-rate unit; the fleet then under-admits for the rest of the window")
}
