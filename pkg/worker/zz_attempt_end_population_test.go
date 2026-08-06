package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// OnAttemptEnd is PUBLIC API on pkg/queue, and its whole reason to exist is that
// it fires for a NARROWER population than OnJobWaiting: attempts that ended
// without reporting any disposition. Nothing pinned that population.
//
// That gap is the exact mechanism of the defect this hook was introduced to fix.
// The original span-leak fix closed the leak by calling CallWaitingHooks, which
// silently widened OnJobWaiting to include cancelled jobs and every in-flight job
// on every graceful shutdown. Moving the call to a NEW hook does not retire the
// risk — it relocates it onto a surface users can register on, where the next
// person to route something through the fallback repeats the incident with no
// test to stop them.
//
// So both halves are asserted here, and the non-firing half is the load-bearing
// one: wiring OnAttemptEnd into the completion, failure or retry path must turn
// this file red.
//
// The firing condition is a single flag — dispositionReported, set only by the
// branches that actually persist a disposition — so the cases below are chosen to
// straddle it rather than to enumerate call sites.

// attemptEndProbe counts OnAttemptEnd firings and records which jobs caused them.
type attemptEndProbe struct {
	mu    sync.Mutex
	count int
	ids   []core.UUID
}

func (p *attemptEndProbe) register(q *queue.Queue) {
	q.OnAttemptEnd(func(_ context.Context, job *core.Job) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.count++
		if job != nil {
			p.ids = append(p.ids, job.ID)
		}
	})
}

func (p *attemptEndProbe) fired() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

func (p *attemptEndProbe) firedFor() []core.UUID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]core.UUID, len(p.ids))
	copy(out, p.ids)
	return out
}

// ---------------------------------------------------------------------------
// The population that MUST fire: no disposition was persisted.
// ---------------------------------------------------------------------------

func TestOnAttemptEnd_FiresWhenNoDispositionWasPersisted(t *testing.T) {
	notOwned := core.ErrJobNotOwned
	writeErr := errors.New("disposition write unavailable")

	cases := []struct {
		name string
		// why documents the real-world shape each mock stands in for, so a
		// future reader can tell whether a new exit belongs in this table.
		why     string
		store   *mockStorage
		job     *core.Job
		handler func(context.Context, struct{}) error
		// register is false for the no-handler exit, which has no handler by
		// definition.
		register bool
	}{
		{
			name: "cancelled mid-run: the completion write finds the row already terminal",
			why: "CancelJob writes the terminal status FIRST and then cancels the handler, " +
				"so the completion write comes back ErrJobNotOwned and nothing is reported",
			store: &mockStorage{
				completeFunc: func(context.Context, core.UUID, string) error { return notOwned },
			},
			job:      &core.Job{ID: "cancelled-mid-run", Type: "ok", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3},
			handler:  func(context.Context, struct{}) error { return nil },
			register: true,
		},
		{
			name: "lost ownership: the retry write is rejected",
			why:  "the lease expired and another worker reclaimed the row before the retry landed",
			store: &mockStorage{
				failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
					require.NotNil(t, retryAt, "attempts remain; this must be the retry disposition")
					return notOwned
				},
			},
			job:      &core.Job{ID: "lost-ownership-retry", Type: "boom", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3},
			handler:  func(context.Context, struct{}) error { return errors.New("handler failed") },
			register: true,
		},
		{
			name: "terminal failure write never landed",
			why:  "storage was unavailable for the terminal write; the attempt ends reporting nothing",
			store: &mockStorage{
				failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
					require.Nil(t, retryAt, "attempts are exhausted; this must be the terminal disposition")
					return writeErr
				},
			},
			job:      &core.Job{ID: "terminal-write-failed", Type: "boom", Queue: "default", Args: []byte(`{}`), Attempt: 3, MaxRetries: 3},
			handler:  func(context.Context, struct{}) error { return errors.New("handler failed") },
			register: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := queue.New(tc.store)
			if tc.register {
				q.Register(tc.job.Type, tc.handler)
			}
			var probe attemptEndProbe
			probe.register(q)

			w := NewWorker(q, DisableRetry())
			w.processJob(context.Background(), tc.job)

			assert.Equal(t, 1, probe.fired(),
				"OnAttemptEnd must fire exactly once for an attempt that reported no disposition (%s)", tc.why)
			assert.Equal(t, []core.UUID{tc.job.ID}, probe.firedFor(),
				"the hook must receive the job whose attempt ended")
		})
	}
}

// The boundary of the population, stated as a test because it is not obvious and
// reads like a bug when you first meet it.
//
// A job whose type this worker does not know ends its attempt reporting no
// disposition when the terminal write fails — and yet OnAttemptEnd must NOT fire
// for it. The handler lookup (worker.go, "no handler for job") happens BEFORE
// CallStartCtxHooks, so no span was ever opened and there is nothing for the
// fallback to close. The hook's population is "attempts that opened a span and
// ended without a disposition", not "every attempt that ended without one".
//
// This is asserted rather than left implicit because the alternative reading —
// that no-handler jobs are simply missing from the hook — would send the next
// reader to "fix" it, which would fire OnAttemptEnd for a job that has no span,
// and hand pkg/otel a context with no recording span in it.
func TestOnAttemptEnd_DoesNotFireForANoHandlerJobBecauseNoSpanWasOpened(t *testing.T) {
	store := &mockStorage{
		failFunc: func(context.Context, core.UUID, string, string, *time.Time) error {
			return errors.New("disposition write unavailable")
		},
	}

	q := queue.New(store)
	// Deliberately no Register for this type.
	var probe attemptEndProbe
	probe.register(q)

	w := NewWorker(q, DisableRetry())
	w.processJob(context.Background(), &core.Job{
		ID: "no-handler-write-failed", Type: "unregistered", Queue: "default",
		Args: []byte(`{}`), Attempt: 1, MaxRetries: 3,
	})

	assert.Equal(t, 0, probe.fired(),
		"the no-handler path precedes span creation, so there is no span to close and no attempt-end to report")
}

// A graceful shutdown releases the running job rather than failing it, so it too
// reports no disposition. This case runs against real SQLite storage and the real
// Start/drain path, because the release is driven by the worker's shutdown
// sequence and not by a storage return value — a mock cannot produce it.
func TestOnAttemptEnd_FiresForAShutdownReleasedJob(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	started := make(chan struct{})
	var once sync.Once
	q.Register("blocked", func(ctx context.Context, _ struct{}) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	})

	var probe attemptEndProbe
	probe.register(q)

	jobID, err := q.Enqueue(context.Background(), "blocked", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q,
		WithPollInterval(40*time.Millisecond),
		WithDrainTimeout(60*time.Millisecond),
		WithOwnershipAuditInterval(0),
		DisableRetry(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() { startReturned <- w.Start(ctx) }()

	// Liveness budgets only — each select returns the moment its channel fires,
	// so a healthy run pays nothing. WithDrainTimeout above is the load-bearing
	// one: it must expire promptly so the handler is really cancelled.
	const settle = 10 * time.Second

	select {
	case <-started:
	case <-time.After(settle):
		t.Fatal("job did not start")
	}

	cancel()
	select {
	case <-startReturned:
	case <-time.After(settle):
		t.Fatal("Start did not return")
	}

	assert.Equal(t, []core.UUID{jobID}, probe.firedFor(),
		"a job released by graceful shutdown reported no disposition, so OnAttemptEnd must fire for it")
}

// ---------------------------------------------------------------------------
// The population that MUST NOT fire: a disposition WAS persisted.
//
// This is the half that makes the hook worth having. If it ever goes green while
// OnAttemptEnd is wired into a disposition path, the hook has silently become
// OnJobWaiting again and every user counter built on it is wrong.
// ---------------------------------------------------------------------------

func TestOnAttemptEnd_DoesNotFireWhenADispositionWasPersisted(t *testing.T) {
	cases := []struct {
		name        string
		disposition string
		store       *mockStorage
		job         *core.Job
		handler     func(context.Context, struct{}) error
	}{
		{
			name:        "clean completion",
			disposition: "complete",
			store: &mockStorage{
				completeFunc: func(context.Context, core.UUID, string) error { return nil },
			},
			job:     &core.Job{ID: "completed", Type: "ok", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3},
			handler: func(context.Context, struct{}) error { return nil },
		},
		{
			name:        "persisted retry",
			disposition: "retry",
			store: &mockStorage{
				failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
					require.NotNil(t, retryAt, "attempts remain; this must be the retry disposition")
					return nil
				},
			},
			job:     &core.Job{ID: "retried", Type: "boom", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3},
			handler: func(context.Context, struct{}) error { return errors.New("handler failed") },
		},
		{
			name:        "persisted terminal failure",
			disposition: "fail",
			store: &mockStorage{
				failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
					require.Nil(t, retryAt, "attempts are exhausted; this must be the terminal disposition")
					return nil
				},
			},
			job:     &core.Job{ID: "failed", Type: "boom", Queue: "default", Args: []byte(`{}`), Attempt: 3, MaxRetries: 3},
			handler: func(context.Context, struct{}) error { return errors.New("handler failed") },
		},
		{
			name:        "persisted terminal failure via NoRetryError",
			disposition: "fail",
			store: &mockStorage{
				failFunc: func(_ context.Context, _ core.UUID, _ string, _ string, retryAt *time.Time) error {
					require.Nil(t, retryAt, "NoRetryError is terminal regardless of attempts remaining")
					return nil
				},
			},
			job: &core.Job{ID: "no-retry", Type: "boom", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3},
			handler: func(context.Context, struct{}) error {
				return &core.NoRetryError{Err: errors.New("permanently bad input")}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := queue.New(tc.store)
			q.Register(tc.job.Type, tc.handler)
			var probe attemptEndProbe
			probe.register(q)

			w := NewWorker(q, DisableRetry())
			w.processJob(context.Background(), tc.job)

			assert.Equal(t, 0, probe.fired(),
				"OnAttemptEnd must NOT fire for an attempt that persisted a %s disposition; "+
					"firing here would put %s jobs into every user counter built on this hook",
				tc.disposition, tc.disposition)
		})
	}
}

// The fallback exists to close the observability span, and the reason it is
// conditional is that ending an already-ended span is a no-op in OTel — so a span
// recorder cannot tell a correct unconditional call from a correct conditional
// one. Counting hook firings can. This pins the two together: the hook fires only
// for the abandoned attempt, and the completed attempt in the same worker does
// not leak into it.
func TestOnAttemptEnd_SeparatesAbandonedFromCompletedInTheSameWorker(t *testing.T) {
	var completions atomic.Int32
	store := &mockStorage{
		completeFunc: func(_ context.Context, jobID core.UUID, _ string) error {
			if jobID == "abandoned" {
				return core.ErrJobNotOwned
			}
			completions.Add(1)
			return nil
		},
	}

	q := queue.New(store)
	q.Register("ok", func(context.Context, struct{}) error { return nil })
	var probe attemptEndProbe
	probe.register(q)

	w := NewWorker(q, DisableRetry())
	w.processJob(context.Background(), &core.Job{
		ID: "completed-cleanly", Type: "ok", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3,
	})
	w.processJob(context.Background(), &core.Job{
		ID: "abandoned", Type: "ok", Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxRetries: 3,
	})

	assert.Equal(t, int32(1), completions.Load(), "exactly one of the two attempts persisted a completion")
	assert.Equal(t, []core.UUID{core.UUID("abandoned")}, probe.firedFor(),
		"only the attempt that reported no disposition may reach OnAttemptEnd")
}

// A panicking user hook must be contained, exactly as every other Call*Hooks
// dispatcher in pkg/queue contains one.
//
// This one is special because of WHERE it is called from: processJob's deferred
// fallback, inside a function that has its own panic recovery whose job is to
// conclude "the worker panicked, release the row". An unguarded panic in a user
// callback would therefore not merely skip that callback — it would unwind into
// that recovery and be logged as a library-side processJob panic, sending an
// operator to read a stack trace that blames the wrong code.
//
// Observability without depending on release semantics: safeUserCallback wraps
// each hook INDIVIDUALLY, so a later hook still runs after an earlier one panics.
// Unwrapped, the first panic unwinds out of the loop and the second hook never
// fires. That difference is what this asserts.
func TestOnAttemptEnd_ContainsAPanickingUserHook(t *testing.T) {
	store := &mockStorage{
		completeFunc: func(context.Context, core.UUID, string) error { return core.ErrJobNotOwned },
	}

	q := queue.New(store)
	q.Register("ok", func(context.Context, struct{}) error { return nil })

	var second atomic.Int32
	q.OnAttemptEnd(func(context.Context, *core.Job) { panic("user OnAttemptEnd hook panicked") })
	q.OnAttemptEnd(func(context.Context, *core.Job) { second.Add(1) })

	w := NewWorker(q, DisableRetry())
	require.NotPanics(t, func() {
		w.processJob(context.Background(), &core.Job{
			ID: "panicking-hook", Type: "ok", Queue: "default",
			Args: []byte(`{}`), Attempt: 1, MaxRetries: 3,
		})
	}, "a panicking user hook must not escape into processJob's own panic recovery")

	assert.Equal(t, int32(1), second.Load(),
		"each hook is wrapped individually, so a panic in one must not prevent the next from running")
}
