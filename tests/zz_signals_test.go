package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resultBox captures a handler's observed value keyed by job ID.
type resultBox struct {
	mu sync.Mutex
	m  map[string]string
}

func newResultBox() *resultBox { return &resultBox{m: map[string]string{}} }
func (b *resultBox) set(id, v string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m[id] = v
}
func (b *resultBox) get(id string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.m[id]
	return v, ok
}

func startWorker(t *testing.T, q *jobs.Queue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	// Concurrency 2 matches the harness's capped connection pool on
	// Postgres/MySQL, avoiding connection starvation that slows the worker.
	w := q.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(2)))
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Start(ctx) }()
	// Wait for the worker to fully drain on cleanup. Without this, a cancelled
	// worker's background polls keep hitting the shared Postgres/MySQL server
	// while the next sequential test runs, starving its 2-connection pool and
	// making timing-sensitive resumes flake.
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// TestSignals_WaitThenSendResumes: the handler suspends waiting for a signal,
// the test sends one, and the job resumes and returns the payload.
func TestSignals_WaitThenSendResumes(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	box := newResultBox()
	q.Register("agent-wait", func(ctx context.Context, _ struct{}) error {
		v, err := jobs.WaitForSignal[string](ctx, "ctx")
		if err != nil {
			return err
		}
		box.set(jobs.JobIDFromContext(ctx), v)
		return nil
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-wait", struct{}{})
	require.NoError(t, err)

	// Wait until the handler has suspended on the signal.
	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond, "job should suspend waiting for a signal")

	require.NoError(t, jobs.Signal(ctx, q, id, "ctx", "hello-from-outside"))

	require.Eventually(t, func() bool {
		v, ok := box.get(id)
		return ok && v == "hello-from-outside"
	}, 10*time.Second, 50*time.Millisecond, "job should resume and observe the signal")

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusCompleted
	}, 10*time.Second, 50*time.Millisecond, "job should complete after observing the signal")
}

// TestSignals_BufferedBeforeWait: a signal sent before the handler waits is not
// lost — WaitForSignal returns it immediately without suspending.
func TestSignals_BufferedBeforeWait(t *testing.T) {
	q, store := openIntegrationQueue(t)
	box := newResultBox()
	q.Register("agent-buffered", func(ctx context.Context, _ struct{}) error {
		v, err := jobs.WaitForSignal[string](ctx, "ctx")
		if err != nil {
			return err
		}
		box.set(jobs.JobIDFromContext(ctx), v)
		return nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-buffered", struct{}{})
	require.NoError(t, err)

	// Send BEFORE starting the worker, so the signal is buffered.
	require.NoError(t, jobs.Signal(ctx, q, id, "ctx", "buffered"))

	startWorker(t, q)

	require.Eventually(t, func() bool {
		v, ok := box.get(id)
		return ok && v == "buffered"
	}, 10*time.Second, 50*time.Millisecond, "buffered signal must be delivered")

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusCompleted
	}, 10*time.Second, 50*time.Millisecond)
	_ = store
}

// TestSignals_PollBackstopResumes: deliver a signal straight to storage
// (bypassing the facade's fast-path resume), so ONLY the recovery poll can wake
// the waiting job. This is the deliver-vs-suspend race backstop.
func TestSignals_PollBackstopResumes(t *testing.T) {
	q, store := openIntegrationQueue(t)
	box := newResultBox()
	q.Register("agent-backstop", func(ctx context.Context, _ struct{}) error {
		v, err := jobs.WaitForSignal[string](ctx, "ctx")
		if err != nil {
			return err
		}
		box.set(jobs.JobIDFromContext(ctx), v)
		return nil
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-backstop", struct{}{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond)

	// Deliver via storage only — no ResumeJob — so the poll must recover it.
	sender, ok := store.(interface {
		SendSignal(context.Context, string, string, []byte) error
	})
	require.True(t, ok, "storage must support signals")
	require.NoError(t, sender.SendSignal(ctx, id, "ctx", []byte(`"via-poll"`)))

	require.Eventually(t, func() bool {
		v, ok := box.get(id)
		return ok && v == "via-poll"
	}, 25*time.Second, 100*time.Millisecond, "poll backstop must resume the waiting job")
}

// TestSignals_Timeout: WaitForSignalTimeout returns (false) when no signal
// arrives, resumed by the signal-resume poll once the deadline passes.
func TestSignals_Timeout(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	box := newResultBox()
	q.Register("agent-timeout", func(ctx context.Context, _ struct{}) error {
		v, ok, err := jobs.WaitForSignalTimeout[string](ctx, "ctx", 1*time.Second)
		if err != nil {
			return err
		}
		if ok {
			box.set(jobs.JobIDFromContext(ctx), "got:"+v)
		} else {
			box.set(jobs.JobIDFromContext(ctx), "timeout")
		}
		return nil
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-timeout", struct{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		v, ok := box.get(id)
		return ok && v == "timeout"
	}, 25*time.Second, 100*time.Millisecond, "no signal → timeout outcome")
}

// TestSignals_EmitsDeliveredEvent: a successful Signal publishes a
// SignalDelivered event on the queue's event stream.
func TestSignals_EmitsDeliveredEvent(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	q.Register("agent-evt", func(ctx context.Context, _ struct{}) error { return nil })

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-evt", struct{}{})
	require.NoError(t, err)

	events := q.Events()
	require.NoError(t, jobs.Signal(ctx, q, id, "ctx", "x"))

	timeout := time.After(3 * time.Second)
	for {
		select {
		case e := <-events:
			if sd, ok := e.(*jobs.SignalDelivered); ok {
				assert.Equal(t, id, sd.JobID)
				assert.Equal(t, "ctx", sd.Name)
				return
			}
		case <-timeout:
			t.Fatal("did not receive SignalDelivered event")
		}
	}
}

func TestSignals_EmitsResumedBySignalOnFastPath(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	q.Register("agent-resume-event-fast", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.WaitForSignal[string](ctx, "ctx")
		return err
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-resume-event-fast", struct{}{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond)

	events := q.Events()
	require.NoError(t, jobs.Signal(ctx, q, id, "ctx", "x"))

	timeout := time.After(3 * time.Second)
	for {
		select {
		case e := <-events:
			if resumed, ok := e.(*jobs.JobResumedBySignal); ok {
				assert.Equal(t, id, resumed.JobID)
				assert.Equal(t, "ctx", resumed.SignalName)
				return
			}
		case <-timeout:
			t.Fatal("did not receive JobResumedBySignal event")
		}
	}
}

func TestSignals_EmitsResumedBySignalOnPollBackstop(t *testing.T) {
	q, store := openIntegrationQueue(t)
	q.Register("agent-resume-event-poll", func(ctx context.Context, _ struct{}) error {
		_, err := jobs.WaitForSignal[string](ctx, "ctx")
		return err
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-resume-event-poll", struct{}{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond)

	events := q.Events()
	sender, ok := store.(interface {
		SendSignal(context.Context, string, string, []byte) error
	})
	require.True(t, ok, "storage must support signals")
	require.NoError(t, sender.SendSignal(ctx, id, "ctx", []byte(`"via-poll"`)))

	timeout := time.After(25 * time.Second)
	for {
		select {
		case e := <-events:
			if resumed, ok := e.(*jobs.JobResumedBySignal); ok {
				assert.Equal(t, id, resumed.JobID)
				assert.Equal(t, "ctx", resumed.SignalName)
				return
			}
		case <-timeout:
			t.Fatal("did not receive JobResumedBySignal event from poll backstop")
		}
	}
}

// TestSignals_PeekThenDrain: a handler peeks (non-consuming) then drains,
// observing the same buffered signals.
func TestSignals_PeekThenDrain(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	type counts struct{ peeked, drained int }
	var mu sync.Mutex
	seen := map[string]counts{}
	q.Register("agent-peek-drain", func(ctx context.Context, _ struct{}) error {
		id := jobs.JobIDFromContext(ctx)
		_, peeked, err := jobs.CheckSignal[string](ctx, "ctx")
		if err != nil {
			return err
		}
		all, err := jobs.DrainSignals[string](ctx, "ctx")
		if err != nil {
			return err
		}
		mu.Lock()
		c := seen[id]
		if peeked {
			c.peeked = 1
		}
		c.drained = len(all)
		seen[id] = c
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "agent-peek-drain", struct{}{})
	require.NoError(t, err)
	for _, v := range []string{"a", "b", "c"} {
		require.NoError(t, jobs.Signal(ctx, q, id, "ctx", v))
	}

	startWorker(t, q)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		c, ok := seen[id]
		return ok && c.drained == 3
	}, 10*time.Second, 50*time.Millisecond, "drain should return all three buffered signals")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, seen[id].peeked, "CheckSignal observed a pending signal without consuming it")
}
