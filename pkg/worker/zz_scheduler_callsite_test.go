package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These drive runScheduler ITSELF, because extracting a predicate and unit-testing
// the predicate does not test its USE.
//
// FALSE-GREEN TRAP, and exactly what the previous round of this work shipped:
// scheduleIsBackingOff and scheduleNeverFires each had a thorough table test, and
// BOTH call sites could be changed to `if false && ...` with the entire package
// still green. golangci-lint's `unused` does not catch a dropped call site either,
// because the tests still reference the function. The only way to cover a call site
// is to observe the behaviour it produces.

// countingClaimStorage records every claim attempt the scheduler makes and fails
// them all, so a missing backoff shows up as attempt volume.
type countingClaimStorage struct {
	*mockStorage
	attempts atomic.Int64
}

func (s *countingClaimStorage) ClaimScheduledFire(context.Context, string, time.Time) (bool, error) {
	s.attempts.Add(1)
	return false, errors.New("storage is down")
}

func (s *countingClaimStorage) SeedScheduledFire(_ context.Context, _ string, anchor time.Time) (time.Time, error) {
	return anchor, nil
}

func TestRunScheduler_BackoffCallSiteThrottlesAFailingSchedule(t *testing.T) {
	store := &countingClaimStorage{mockStorage: &mockStorage{}}
	q := queue.New(store)
	q.Register("failing", func(context.Context, struct{}) error { return nil })
	// Every 10ms, so an unthrottled loop would attempt on every 100ms tick.
	require.NoError(t, q.Schedule("failing", nil, schedule.Every(10*time.Millisecond)))

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.runScheduler(ctx) }()
	<-done

	attempts := store.attempts.Load()
	require.Positive(t, attempts, "the scheduler must have tried at least once, or this proves nothing")
	// Measured: 4 attempts with the backoff, 14-15 without, so 8 sits clear of both
	// and leaves headroom for a loaded CI machine ticking at a different rate.
	assert.LessOrEqual(t, attempts, int64(8),
		"a persistently failing schedule must BACK OFF (100ms doubling to 30s), not retry on every "+
			"100ms tick — got %d attempts in ~1.5s, which is the un-throttled rate", attempts)
}

// unsatisfiableStorage fails loudly if a claim is ever attempted: an unsatisfiable
// cron must never reach the claim at all.
type unsatisfiableStorage struct {
	*mockStorage
	attempts atomic.Int64
}

func (s *unsatisfiableStorage) ClaimScheduledFire(context.Context, string, time.Time) (bool, error) {
	s.attempts.Add(1)
	return false, nil
}

func (s *unsatisfiableStorage) SeedScheduledFire(_ context.Context, _ string, anchor time.Time) (time.Time, error) {
	return anchor, nil
}

func TestRunScheduler_NeverFiresCallSiteSkipsAndLogsOnce(t *testing.T) {
	store := &unsatisfiableStorage{mockStorage: &mockStorage{}}
	q := queue.New(store)
	q.Register("never", func(context.Context, struct{}) error { return nil })

	// February 30th never occurs, so cron finds no match within five years and
	// Next returns the zero time — which reads as "due" without the guard.
	never, err := schedule.Cron("0 0 30 2 *")
	require.NoError(t, err)
	require.NoError(t, q.Schedule("never", nil, never))

	var buf bytes.Buffer
	w := NewWorker(q)
	w.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.runScheduler(ctx) }()
	<-done

	assert.Zero(t, store.attempts.Load(),
		"a schedule that can never fire must not run a claim transaction at all — without the "+
			"guard the zero time is 'due' on every tick, forever and silently")
	assert.Equal(t, 1, strings.Count(buf.String(), "never fires"),
		"and it must be logged exactly once, not once per tick")
}

// orphanStorage makes every heartbeat report that the job is owned elsewhere,
// which drives runHeartbeat to its orphan-abandon branch.
type orphanStorage struct{ *mockStorage }

func (orphanStorage) Heartbeat(context.Context, core.UUID, string) error {
	return core.ErrJobNotOwned
}

// TestRunHeartbeat_OrphanCancelsOnlyItsOwnRun covers the call site, not the helper.
//
// Reverting `w.cancelRun(job.ID, runToken)` to `w.CancelJob(job.ID)` — the exact
// defect fixed one round earlier — left the whole package green, because the only
// coverage was a unit test of cancelRun itself.
func TestRunHeartbeat_OrphanCancelsOnlyItsOwnRun(t *testing.T) {
	q := queue.New(orphanStorage{mockStorage: &mockStorage{}})
	w := NewWorker(q)
	w.heartbeatInterval = 5 * time.Millisecond
	w.config.StorageRetry.MaxAttempts = 1

	jobID := core.NewID()
	var laterRunCancelled atomic.Bool

	// Run #1 is the one whose heartbeat will orphan; run #2 has since taken the id.
	w.runningJobsMu.Lock()
	staleToken := w.nextRunToken.Add(1)
	liveToken := w.nextRunToken.Add(1)
	w.runningJobs[jobID] = runningJobEntry{
		cancel: func() { laterRunCancelled.Store(true) },
		token:  liveToken,
	}
	w.runningJobsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runHeartbeat(ctx, &core.Job{ID: jobID, Type: "t", Queue: "default"}, staleToken)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runHeartbeat never abandoned the orphaned job")
	}

	assert.False(t, laterRunCancelled.Load(),
		"an orphaned heartbeat belonging to an EARLIER run must not cancel the healthy run that "+
			"replaced it — that run would then fail and burn an attempt it never earned")
}
