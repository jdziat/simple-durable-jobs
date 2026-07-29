package worker

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Admission state (the per-queue counter and the concurrency slots) is registered
// in dispatchDequeuedJobs and released either there, on a bail-out, or by
// processJobRun's defers. Every path must balance: a missed release leaks the
// counter UP until the queue refuses all work, and a double release leaks it DOWN
// so the cap over-admits.
//
// FALSE-GREEN TRAP: asserting only the happy path passes with every bail-out
// broken. Each bail-out reason is driven separately, and the assertion is that the
// counter and the slot map are BOTH back to empty.
func assertAdmissionDrained(t *testing.T, w *Worker, queueName string) {
	t.Helper()
	if counter, ok := w.queueRunning[queueName]; ok {
		assert.Equal(t, int32(0), counter.Load(),
			"per-queue counter must return to zero, or the queue eventually refuses every job")
	}
	w.queueJobIDMu.Lock()
	nq := len(w.queueJobID)
	w.queueJobIDMu.Unlock()
	assert.Zero(t, nq, "no run→queue entry may outlive its run")

	w.slotJobIDMu.Lock()
	ns := len(w.slotJobID)
	w.slotJobIDMu.Unlock()
	assert.Zero(t, ns, "no run→slots entry may outlive its run")
}

func TestDispatch_AdmissionBalancesOnEveryBailOut(t *testing.T) {
	newJobs := func(n int, q string) []*core.Job {
		out := make([]*core.Job, n)
		for i := range out {
			out[i] = &core.Job{ID: core.NewID(), Type: "t", Queue: q, Status: core.StatusRunning}
		}
		return out
	}

	t.Run("queue cap refuses beyond the limit", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(2)))
		ch := make(chan dispatchedJob, 10)
		dispatched, released := w.dispatchDequeuedJobs(context.Background(), ch, newJobs(5, "default"))
		assert.Equal(t, 2, dispatched)
		assert.Equal(t, 3, released, "the three over the cap must be released, not dropped")

		// Drain what was admitted, exactly as processLoop's defers would.
		close(ch)
		for dj := range ch {
			w.untrackQueueJob(dj.token)
			w.releaseConcurrencySlots(context.Background(), dj.job.ID, dj.token)
		}
		assertAdmissionDrained(t, w, "default")
	})

	t.Run("a cancelled context on send releases what it admitted", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(10)))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ch := make(chan dispatchedJob) // unbuffered: the send cannot proceed
		dispatched, released := w.dispatchDequeuedJobs(ctx, ch, newJobs(3, "default"))
		assert.Zero(t, dispatched)
		assert.Equal(t, 3, released)
		assertAdmissionDrained(t, w, "default")
	})

	t.Run("a fully drained batch leaves nothing behind", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(10)))
		ch := make(chan dispatchedJob, 10)
		dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, newJobs(4, "default"))
		require.Equal(t, 4, dispatched)
		close(ch)
		for dj := range ch {
			w.untrackQueueJob(dj.token)
			w.releaseConcurrencySlots(context.Background(), dj.job.ID, dj.token)
		}
		assertAdmissionDrained(t, w, "default")
	})

	t.Run("processJob's own defers drain a directly-dispatched run", func(t *testing.T) {
		q := queue.New(&mockStorage{})
		q.Register("quick", func(context.Context, struct{}) error { return nil })
		w := NewWorker(q, WorkerQueue("default", Concurrency(4)))

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		w.processJob(ctx, &core.Job{ID: core.NewID(), Type: "quick", Queue: "default", Status: core.StatusRunning})
		assertAdmissionDrained(t, w, "default")
	})
}

// TestProcessJob_FreshTokenPathLeaksNothing covers the entry point that was kept
// for direct callers and tests.
//
// processJob allocates a fresh run token and registers NOTHING with dispatch, so
// its defers must find nothing and remove nothing — in particular they must not
// decrement a counter no one incremented, which would let the queue over-admit.
//
// FALSE-GREEN TRAP: with a queue that has no configured cap there is no counter to
// corrupt, so the queue has to be configured for the assertion to mean anything.
func TestProcessJob_FreshTokenPathLeaksNothing(t *testing.T) {
	q := queue.New(&mockStorage{})
	q.Register("quick", func(context.Context, struct{}) error { return nil })
	q.Register("boom", func(context.Context, struct{}) error { panic("exploded") })
	w := NewWorker(q, WorkerQueue("default", Concurrency(3)))

	counter := w.queueRunning["default"]
	require.Equal(t, int32(0), counter.Load())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, typ := range []string{"quick", "boom", "quick"} {
		w.processJob(ctx, &core.Job{
			ID: core.NewID(), Type: typ, Queue: "default",
			Status: core.StatusRunning, MaxRetries: 1,
		})
	}

	assert.Equal(t, int32(0), counter.Load(),
		"a run that never went through dispatch registered no admission state, so its cleanup "+
			"must not decrement a counter nobody incremented — that would let the queue admit "+
			"more than its cap")
	assertAdmissionDrained(t, w, "default")
}

// TestDispatch_TokensAreUniquePerRun guards the property every one of these maps
// now depends on. A repeated token would silently reintroduce the shared-slot bug
// that took three attempts to fix.
func TestDispatch_TokensAreUniquePerRun(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(100)))
	jobID := core.NewID() // deliberately the SAME job id every time
	jobs := make([]*core.Job, 50)
	for i := range jobs {
		jobs[i] = &core.Job{ID: jobID, Type: "t", Queue: "default", Status: core.StatusRunning}
	}

	ch := make(chan dispatchedJob, len(jobs))
	dispatched, _ := w.dispatchDequeuedJobs(context.Background(), ch, jobs)
	require.Equal(t, len(jobs), dispatched)
	close(ch)

	seen := map[uint64]bool{}
	for dj := range ch {
		require.False(t, seen[dj.token], "run token %d was issued twice", dj.token)
		seen[dj.token] = true
		w.untrackQueueJob(dj.token)
		w.releaseConcurrencySlots(context.Background(), dj.job.ID, dj.token)
	}
	assert.Len(t, seen, len(jobs), "every run must get its own token, even for one job id")
	assertAdmissionDrained(t, w, "default")
}
