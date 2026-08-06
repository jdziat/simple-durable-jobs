package otel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/worker"
)

// startedSpanCounter counts job.process spans as they START, which the default
// recorder cannot report: a span that is never ended is never handed to
// OnEnd, so a leak is invisible in Ended() — it looks exactly like a job that
// was never dispatched.
type startedSpanCounter struct {
	sdktrace.SpanProcessor
	mu      sync.Mutex
	started map[string]int
	ended   map[string]int
}

func newStartedSpanCounter() *startedSpanCounter {
	return &startedSpanCounter{
		SpanProcessor: sdktrace.NewSimpleSpanProcessor(tracetest.NewNoopExporter()),
		started:       map[string]int{},
		ended:         map[string]int{},
	}
}

func (c *startedSpanCounter) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	c.mu.Lock()
	c.started[s.Name()]++
	c.mu.Unlock()
	c.SpanProcessor.OnStart(parent, s)
}

func (c *startedSpanCounter) OnEnd(s sdktrace.ReadOnlySpan) {
	c.mu.Lock()
	c.ended[s.Name()]++
	c.mu.Unlock()
	c.SpanProcessor.OnEnd(s)
}

func (c *startedSpanCounter) counts(name string) (started, ended int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started[name], c.ended[name]
}

// TestInstrumentedWorkerEndsProcessSpanOnCancel pins that CANCELLING a running
// job ends its per-attempt span.
//
// CancelJob writes the terminal status FIRST and then cancels the handler's
// context, so the handler returns context.Canceled to a worker that no longer
// owns the row. Every disposition write then comes back ErrJobNotOwned and
// handleError returns before any hook — so complete/fail/retry/waiting all fire
// zero times and nothing calls span.End(). An unended span is never exported AND
// is retained by the SDK, one per cancelled job.
//
// The assertion is started == ended, not "some span was ended": a leak is
// invisible in Ended() by construction, so counting only what ended cannot see it.
func TestInstrumentedWorkerEndsProcessSpanOnCancel(t *testing.T) {
	counter := newStartedSpanCounter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(counter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	db := newWALFileDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))

	q := queue.New(store)
	Instrument(q,
		WithTracerProvider(tp),
		WithPropagator(propagation.TraceContext{}),
	)

	running := make(chan struct{})
	var runningOnce sync.Once
	q.Register("otel-cancel-job", func(ctx context.Context, _ struct{}) error {
		runningOnce.Do(func() { close(running) })
		<-ctx.Done() // cancelled by CancelJob
		return ctx.Err()
	})

	jobID, err := q.Enqueue(context.Background(), "otel-cancel-job", struct{}{})
	require.NoError(t, err)

	w := worker.NewWorker(q, worker.WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = w.Start(ctx)
	}()
	defer func() {
		cancel()
		<-workerDone
	}()

	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("job never started")
	}

	require.NoError(t, q.CancelJob(context.Background(), jobID))

	require.Eventually(t, func() bool {
		status, statusErr := q.LoadStatus(context.Background(), jobID)
		return statusErr == nil && status == core.StatusCancelled
	}, 5*time.Second, 20*time.Millisecond, "job should reach cancelled")

	const spanName = "job.process otel-cancel-job"
	require.Eventually(t, func() bool {
		started, ended := counter.counts(spanName)
		return started > 0 && started == ended
	}, 5*time.Second, 20*time.Millisecond,
		"every started job.process span must be ended; a cancelled attempt fires no "+
			"disposition hook, so without an explicit end its span is never exported and "+
			"is retained by the SDK forever")

	started, ended := counter.counts(spanName)
	assert.Equal(t, started, ended, "started=%d ended=%d", started, ended)
	assert.Positive(t, started)
}

// TestInstrumentedWorkerFiresNoSpuriousWaitingHookOnComplete pins that a completed
// attempt does not reach OnJobWaiting.
//
// READ THE SCOPE BEFORE TRUSTING IT. This catches the original incident shape —
// the fallback routed through CallWaitingHooks *and* made unconditional — because
// then a plainly-completed job would call every user's OnJobWaiting. It does NOT
// catch the fallback merely becoming unconditional, because the fallback no longer
// calls CallWaitingHooks at all: it calls CallAttemptEndHooks, which this test does
// not observe. Deleting `if dispositionReported.reported { return }` leaves this
// test green.
//
// An earlier version of this comment claimed the broader property, which is how a
// reviewer would conclude the whole attemptDisposition mechanism was covered when
// it was not. The mutation that this test cannot see is caught by
// TestOnAttemptEnd_DoesNotFireWhenADispositionWasPersisted in pkg/worker, which
// counts the hook the fallback actually calls and was verified to redden under
// exactly that edit.
func TestInstrumentedWorkerFiresNoSpuriousWaitingHookOnComplete(t *testing.T) {
	counter := newStartedSpanCounter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(counter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	db := newWALFileDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))

	q := queue.New(store)
	Instrument(q,
		WithTracerProvider(tp),
		WithPropagator(propagation.TraceContext{}),
	)

	var waitingCalls int
	var mu sync.Mutex
	q.OnJobWaiting(func(context.Context, *core.Job) {
		mu.Lock()
		waitingCalls++
		mu.Unlock()
	})
	q.Register("otel-nowaiting-job", func(context.Context, struct{}) error { return nil })

	completed := make(chan struct{})
	var completedOnce sync.Once
	q.OnJobComplete(func(context.Context, *core.Job) {
		completedOnce.Do(func() { close(completed) })
	})

	_, err := q.Enqueue(context.Background(), "otel-nowaiting-job", struct{}{})
	require.NoError(t, err)

	w := worker.NewWorker(q, worker.WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = w.Start(ctx)
	}()
	defer func() {
		cancel()
		<-workerDone
	}()

	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not complete")
	}

	// Give any (wrong) deferred fallback a chance to run after the disposition.
	require.Eventually(t, func() bool {
		started, ended := counter.counts("job.process otel-nowaiting-job")
		return started > 0 && started == ended
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, waitingCalls,
		"a completed job must not fire OnJobWaiting; the abandoned-attempt fallback "+
			"has to be conditional on no disposition having been reported, not unconditional")
}
