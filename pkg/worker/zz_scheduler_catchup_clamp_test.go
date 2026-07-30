package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// outageClaimStorage fails every claim until downUntil, then records the BOUNDARY
// of each successful claim. Recording the boundary rather than the wall-clock time
// of the fire is what makes the assertion independent of the failure backoff: it
// asks "which missed boundaries were replayed", not "when did the fires happen".
type outageClaimStorage struct {
	*mockStorage
	mu        sync.Mutex
	downUntil time.Time
	fired     []time.Time
}

func (s *outageClaimStorage) ClaimScheduledFire(_ context.Context, _ string, boundary time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Now().Before(s.downUntil) {
		return false, errors.New("storage is down")
	}
	s.fired = append(s.fired, boundary)
	return true, nil
}

func (s *outageClaimStorage) SeedScheduledFire(_ context.Context, _ string, anchor time.Time) (time.Time, error) {
	return anchor, nil
}

func (s *outageClaimStorage) firedBoundaries() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.fired...)
}

// seedLastRun collapses a backlog of missed boundaries to a SINGLE catch-up fire,
// and its own doc states that as the contract ("causing exactly one catch-up fire,
// after which natural cadence resumes"). It was only ever reached from
// establishScheduleBase, which runs once per schedule per process — so the contract
// held on cold start and not afterwards.
//
// After that first sight the durable cursor is never re-read and lastRun is
// advanced one boundary per successful fire, so a storage outage the worker
// SURVIVES leaves the cursor stale by outage/period boundaries. The scheduler then
// fired every one of them, one per 100ms tick — at 10 Hz, each a real Enqueue. The
// genuine-failure backoff made it worse: more boundaries elapse while it waits.
//
// This is a CALL-SITE test on purpose. seedLastRun itself was already correct and
// already covered; testing it again would be the "helper tested while its call site
// is free" trap that this campaign has hit repeatedly. What was missing is that the
// warm path never called it.
func TestRunScheduler_SurvivedOutageDoesNotReplayEveryMissedBoundary(t *testing.T) {
	const period = time.Second
	const outage = 3500 * time.Millisecond

	store := &outageClaimStorage{mockStorage: &mockStorage{}, downUntil: time.Now().Add(outage)}
	recovery := store.downUntil

	q := queue.New(store)
	q.Register("digest", func(context.Context, struct{}) error { return nil })
	require.NoError(t, q.Schedule("digest", nil, schedule.Every(period)))

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), outage+2500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.runScheduler(ctx) }()
	<-done

	fired := store.firedBoundaries()
	require.NotEmpty(t, fired,
		"the scheduler never fired at all after recovery; this test proves nothing unless it does")

	var replayed []time.Time
	for _, b := range fired {
		if b.Before(recovery) {
			replayed = append(replayed, b)
		}
	}
	for _, b := range replayed {
		t.Logf("replayed a boundary that elapsed during the outage: %s before recovery",
			recovery.Sub(b).Round(time.Millisecond))
	}
	assert.LessOrEqual(t, len(replayed), 1,
		"a survived outage replayed %d of the boundaries it missed; the documented contract is at most ONE catch-up fire, and each replay is a real Enqueue issued at the 100ms tick rate",
		len(replayed))
}

// The clamp must not suppress ORDINARY firing: a schedule that is merely due, with
// no backlog, must still fire on cadence. Without this, "never fire" would pass the
// test above.
func TestRunScheduler_ClampDoesNotSuppressOrdinaryFiring(t *testing.T) {
	store := &outageClaimStorage{mockStorage: &mockStorage{}} // downUntil zero: never down
	q := queue.New(store)
	q.Register("digest", func(context.Context, struct{}) error { return nil })
	require.NoError(t, q.Schedule("digest", nil, schedule.Every(300*time.Millisecond)))

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.runScheduler(ctx) }()
	<-done

	fired := store.firedBoundaries()
	assert.GreaterOrEqual(t, len(fired), 2,
		"a healthy 300ms schedule must keep firing on cadence over ~1.5s; got %d fires, so the catch-up clamp is suppressing ordinary work", len(fired))
}
