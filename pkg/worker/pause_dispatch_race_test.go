package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// TestDrain_ReleasesJobClaimedWhilePausing is the deterministic regression for
// the pause-dispatch race behind the flaky TestPauseIntegration_PauseWorkerStopsNewJobs
// (39/100 fails on Postgres, 0/100 after the fix). drainDequeuedJobs checks pause
// at the top of each iteration, but a Pause() that lands DURING the dequeue
// round-trip (a window widened by network latency on Postgres/MySQL) would let
// the worker DISPATCH the job it just claimed — so a job enqueued right after
// Pause() slips through a "graceful"-paused worker.
//
// The dequeue hook reproduces that window exactly: it pauses the worker and then
// returns a claimable job, so the worker returns from dequeueAvailableJobs while
// already paused. The fix re-checks IsPaused() after the dequeue and RELEASES the
// batch instead of dispatching. Fail-first: without the recheck the handler runs
// (processed == 1) and the job is never released.
func TestDrain_ReleasesJobClaimedWhilePausing(t *testing.T) {
	var processed atomic.Int32
	job := &core.Job{ID: core.NewID(), Type: "tracked", Queue: "default", Status: core.StatusRunning, Attempt: 1}

	m := &mockStorage{}
	q := queue.New(m)
	q.Register("tracked", func(context.Context, struct{}) error {
		processed.Add(1)
		return nil
	})

	w := NewWorker(q, WithPollInterval(10*time.Millisecond))

	var once sync.Once
	m.dequeueFunc = func(context.Context, []string, string) (*core.Job, error) {
		var out *core.Job
		once.Do(func() {
			w.Pause(core.PauseModeGraceful) // pause lands mid-dequeue
			out = job                       // ...and a claimable job comes back
		})
		return out, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	// The job claimed during the pause window must be RELEASED back to pending,
	// never dispatched to a handler.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.releasedJobIDs) == 1 && m.releasedJobIDs[0] == job.ID
	}, 2*time.Second, 5*time.Millisecond, "a job claimed while pausing must be released, not dispatched")

	// And it must stay unprocessed.
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, int32(0), processed.Load(), "a graceful-paused worker must not run a job it claimed while pausing")
}
