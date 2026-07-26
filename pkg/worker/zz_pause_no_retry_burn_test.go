package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pause(PauseModeAggressive) cancels running handler contexts but wrote nothing
// durable, so the handler's context.Canceled fell into the ordinary failure
// path: Fail() burned an attempt, JobRetrying/JobFailed were emitted, and at the
// default MaxRetries — with the attempt already advanced — the job was
// permanently DEAD-LETTERED. By an operation the README presents as the
// reversible half of Pause/Resume.
//
// FALSE-GREEN TRAP: asserting the job stopped running passes with the bug
// present, because a dead-lettered job is not running either. The discriminating
// observations are that Fail was NEVER called and that Release WAS.
func TestPause_AggressiveReleasesInsteadOfFailing(t *testing.T) {
	var failCalls atomic.Int64
	store := &mockStorage{}
	store.failFunc = func(context.Context, core.UUID, string, string, *time.Time) error {
		failCalls.Add(1)
		return nil
	}

	q := queue.New(store)

	started := make(chan struct{})
	q.Register("slow", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-ctx.Done() // observe the pause cancellation
		return ctx.Err()
	})

	job := &core.Job{
		ID: core.NewID(), Type: "slow", Queue: "default",
		Status: core.StatusRunning, Attempt: 1, MaxRetries: 2,
	}

	w := NewWorker(q)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); w.processJob(ctx, job) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	w.Pause(core.PauseModeAggressive)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processJob never returned after the pause")
	}

	assert.Zero(t, failCalls.Load(),
		"an aggressive pause must NOT travel the failure path — that is what burned the attempt "+
			"and, at the default MaxRetries, dead-lettered a job that was merely paused")

	store.mu.Lock()
	releasedIDs := append([]core.UUID(nil), store.releasedJobIDs...)
	store.mu.Unlock()
	assert.Contains(t, releasedIDs, job.ID,
		"the paused job must be RELEASED so Resume simply re-dispatches it")
}

// TestPause_AggressiveConsumesTheMarkExactlyOnce guards the leak. A job released
// by a pause is re-dispatched on resume; if the mark survived, a GENUINE failure
// on that later run would be silently swallowed as "just a pause" and released
// forever, turning a dead-letter bug into an infinite-retry bug.
func TestPause_AggressiveConsumesTheMarkExactlyOnce(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}))
	id := core.NewID()

	w.runningJobsMu.Lock()
	w.pauseCancelled[id] = struct{}{}
	w.runningJobsMu.Unlock()

	assert.True(t, w.takePauseCancelled(id), "the first read must see the mark")
	assert.False(t, w.takePauseCancelled(id),
		"the mark must be consumed — a surviving mark would mask a real failure on the job's next run")
}

// TestResume_ClearsUnconsumedPauseMarks covers the other leak path: a job that
// finished before observing its cancellation leaves a mark nobody consumes.
func TestResume_ClearsUnconsumedPauseMarks(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}))
	id := core.NewID()

	w.runningJobsMu.Lock()
	w.pauseCancelled[id] = struct{}{}
	w.runningJobsMu.Unlock()

	w.Resume()

	assert.False(t, w.takePauseCancelled(id),
		"Resume must clear stale marks so one cannot outlive its pause")
}

// TestPause_AggressiveDoesNotStopTheHeartbeat pins the lease. The heartbeat used
// to return early under an aggressive pause, dropping the lease of a job that is
// still running — and a handler mid-I/O may not observe cancellation for some
// time. Once the lease lapses the stale-lock reaper hands the job to a peer while
// the original handler is still executing it: a pause causing double-execution.
func TestPause_AggressiveDoesNotStopTheHeartbeat(t *testing.T) {
	var beats atomic.Int64
	store := &mockStorage{}
	store.heartbeatFunc = func(context.Context, core.UUID, string) error {
		beats.Add(1)
		return nil
	}

	q := queue.New(store)
	started := make(chan struct{})
	release := make(chan struct{})
	q.Register("stubborn", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-release // deliberately ignores cancellation
		return nil
	})

	job := &core.Job{ID: core.NewID(), Type: "stubborn", Queue: "default", Status: core.StatusRunning}

	w := NewWorker(q)
	// No exported option for this; set the internal cadence directly so the test
	// observes several beats inside its budget.
	w.heartbeatInterval = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go w.processJob(ctx, job)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	w.Pause(core.PauseModeAggressive)
	before := beats.Load()

	require.Eventually(t, func() bool { return beats.Load() > before }, 5*time.Second, 20*time.Millisecond,
		"a still-running job must keep its lease through an aggressive pause; dropping it lets the "+
			"stale-lock reaper hand the job to a peer while this handler is still executing it")

	close(release)
}
