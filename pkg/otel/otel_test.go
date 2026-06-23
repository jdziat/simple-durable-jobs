package otel

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/worker"
)

// setupTracer creates a test tracer with an in-memory span recorder.
func setupTracer() (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	return tp, exporter
}

func TestEnqueueMiddleware_InjectsTraceContext(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	prop := propagation.TraceContext{}

	mw := enqueueMiddleware(tracer, prop)

	// Create a parent span to simulate an active trace
	ctx, parentSpan := tracer.Start(context.Background(), "test.parent")
	defer parentSpan.End()

	job := &core.Job{
		Type:     "test-job",
		Queue:    "default",
		Priority: 5,
	}

	var persistedJob *core.Job
	persist := func(_ context.Context, j *core.Job) error {
		persistedJob = j
		j.ID = "job-123"
		return nil
	}

	err := mw(ctx, job, persist)
	require.NoError(t, err)

	// Verify trace context was injected
	assert.NotEmpty(t, persistedJob.TraceContext)

	// Verify it's valid JSON with traceparent
	var carrier map[string]string
	err = json.Unmarshal(persistedJob.TraceContext, &carrier)
	require.NoError(t, err)
	assert.Contains(t, carrier, "traceparent")

	// Verify the enqueue span was created
	parentSpan.End()
	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var enqueueSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.enqueue" {
			enqueueSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, enqueueSpan, "expected job.enqueue span")
	assert.Equal(t, trace.SpanKindProducer, enqueueSpan.SpanKind)
}

func TestEnqueueMiddleware_NoActiveSpan(t *testing.T) {
	tp, _ := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	prop := propagation.TraceContext{}

	mw := enqueueMiddleware(tracer, prop)

	job := &core.Job{
		Type:  "test-job",
		Queue: "default",
	}

	persist := func(_ context.Context, j *core.Job) error {
		j.ID = "job-456"
		return nil
	}

	err := mw(context.Background(), job, persist)
	require.NoError(t, err)

	// Even without a parent span, the middleware should still work
	// It will create a root span and inject its context
	assert.NotEmpty(t, job.TraceContext)
}

func TestEnqueueMiddleware_PropagatesPersistError(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	prop := propagation.TraceContext{}

	mw := enqueueMiddleware(tracer, prop)

	job := &core.Job{Type: "test-job", Queue: "default"}
	persistErr := errors.New("db connection failed")

	persist := func(_ context.Context, _ *core.Job) error {
		return persistErr
	}

	err := mw(context.Background(), job, persist)
	assert.ErrorIs(t, err, persistErr)

	// Verify the span recorded the error
	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)

	var enqueueSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.enqueue" {
			enqueueSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, enqueueSpan)
	assert.Equal(t, codes.Error, enqueueSpan.Status.Code)
}

func TestStartCtxHook_CreatesConsumerSpan(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	prop := propagation.TraceContext{}

	// Simulate enqueue: create a span and serialize its context
	enqueueCtx, enqueueSpan := tracer.Start(context.Background(), "test.enqueue")
	carrier := make(mapCarrier)
	prop.Inject(enqueueCtx, carrier)
	traceData, _ := json.Marshal(carrier)
	enqueueSpan.End()

	hook := startCtxHook(tracer, prop)

	job := &core.Job{
		ID:           "job-789",
		Type:         "process-order",
		Queue:        "orders",
		Priority:     10,
		Attempt:      2,
		TraceContext: traceData,
	}

	newCtx := hook(context.Background(), job)

	// Verify that a span is now in the context
	span := trace.SpanFromContext(newCtx)
	assert.True(t, span.SpanContext().IsValid())
	assert.True(t, span.IsRecording())

	// End the span so it gets exported
	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process process-order" {
			processSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, processSpan, "expected job.process span")
	assert.Equal(t, trace.SpanKindConsumer, processSpan.SpanKind)

	// Verify it's a child of the enqueue span (same trace ID)
	assert.Equal(t, enqueueSpan.SpanContext().TraceID(), processSpan.SpanContext.TraceID())

	// Verify attributes
	attrMap := spanAttrMap(processSpan.Attributes)
	assert.Equal(t, "job-789", attrMap["job.id"])
	assert.Equal(t, "process-order", attrMap["job.type"])
	assert.Equal(t, "orders", attrMap["job.queue"])
}

func TestStartCtxHook_NoTraceContext(t *testing.T) {
	tp, _ := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	prop := propagation.TraceContext{}

	hook := startCtxHook(tracer, prop)

	job := &core.Job{
		ID:    "job-no-trace",
		Type:  "simple-job",
		Queue: "default",
	}

	newCtx := hook(context.Background(), job)

	// Should still create a span (root span, no parent)
	span := trace.SpanFromContext(newCtx)
	assert.True(t, span.SpanContext().IsValid())
	span.End()
}

func TestCompleteHook_EndsSpanWithOK(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	ctx, span := tracer.Start(context.Background(), "job.process test-job")

	hook := completeHook()
	hook(ctx, &core.Job{ID: "job-1"})

	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process test-job" {
			processSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, processSpan)
	assert.Equal(t, codes.Ok, processSpan.Status.Code)
	_ = span
}

func TestFailHook_EndsSpanWithError(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	ctx, span := tracer.Start(context.Background(), "job.process failing-job")

	hook := failHook()
	jobErr := errors.New("handler panicked")
	hook(ctx, &core.Job{ID: "job-2"}, jobErr)

	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process failing-job" {
			processSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, processSpan)
	assert.Equal(t, codes.Error, processSpan.Status.Code)
	assert.Contains(t, processSpan.Status.Description, "handler panicked")
	_ = span
}

func TestRetryHook_AddsRetryEvent(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	ctx, span := tracer.Start(context.Background(), "job.process retry-job")

	hook := retryHook()
	retryErr := errors.New("temporary failure")
	// The hook itself ends the span — no manual span.End() here, which is what
	// proves the retrying attempt's span is not leaked.
	hook(ctx, &core.Job{ID: "job-3"}, 2, retryErr)

	_ = span // span ended by the hook
	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process retry-job" {
			processSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, processSpan, "retry hook must end the span so it is exported (no leak)")
	require.Len(t, processSpan.Events, 1)
	assert.Equal(t, "job.retry", processSpan.Events[0].Name)

	eventAttrs := spanAttrMap(processSpan.Events[0].Attributes)
	assert.Equal(t, "2", eventAttrs["job.attempt"])
	assert.Equal(t, "temporary failure", eventAttrs["job.error"])

	// The retrying attempt is recorded as an error disposition.
	assert.Equal(t, codes.Error, processSpan.Status.Code)
	assert.Contains(t, processSpan.Status.Description, "temporary failure")
}

func TestRetryHook_EndsRecordingSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	ctx, _ := tracer.Start(context.Background(), "job.process flapping-job")

	retryHook()(ctx, &core.Job{ID: "job-retry"}, 1, errors.New("flap"))

	ended := recorder.Ended()
	require.Len(t, ended, 1, "retry hook must end exactly one span")
	assert.Equal(t, "job.process flapping-job", ended[0].Name())
	assert.False(t, ended[0].EndTime().IsZero(), "span must be ended, not leaked")
}

func TestWaitingHook_EndsSpan(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	ctx, _ := tracer.Start(context.Background(), "job.process waiting-job")

	// The hook ends the span; no manual span.End().
	waitingHook()(ctx, &core.Job{ID: "job-wait"})

	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process waiting-job" {
			processSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, processSpan, "waiting hook must end the span so it is exported (no leak)")
	require.Len(t, processSpan.Events, 1)
	assert.Equal(t, "job.waiting", processSpan.Events[0].Name)

	// Waiting is neither success nor failure: status stays Unset.
	assert.Equal(t, codes.Unset, processSpan.Status.Code)
	attrMap := spanAttrMap(processSpan.Attributes)
	assert.Equal(t, "waiting", attrMap["job.disposition"])
}

func TestWaitingHook_EndsRecordingSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer(instrumentationName)
	ctx, _ := tracer.Start(context.Background(), "job.process parked-job")

	waitingHook()(ctx, &core.Job{ID: "job-parked"})

	ended := recorder.Ended()
	require.Len(t, ended, 1, "waiting hook must end exactly one span")
	assert.Equal(t, "job.process parked-job", ended[0].Name())
	assert.False(t, ended[0].EndTime().IsZero(), "span must be ended, not leaked")
}

func TestWaitingHook_NonRecordingSpanNoPanic(t *testing.T) {
	// A non-recording (no active span) context must be a safe no-op, mirroring
	// the guard in the complete/fail hooks.
	require.NotPanics(t, func() {
		waitingHook()(context.Background(), &core.Job{ID: "job-none"})
	})
	require.NotPanics(t, func() {
		retryHook()(context.Background(), &core.Job{ID: "job-none"}, 1, errors.New("x"))
	})
}

func TestJobAttributes(t *testing.T) {
	job := &core.Job{
		ID:       "abc-123",
		Type:     "send-email",
		Queue:    "emails",
		Priority: 5,
		Attempt:  1,
	}

	attrs := jobAttributes(job)
	m := make(map[string]string)
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.Emit()
	}

	assert.Equal(t, "abc-123", m["job.id"])
	assert.Equal(t, "send-email", m["job.type"])
	assert.Equal(t, "emails", m["job.queue"])
	assert.Equal(t, "5", m["job.priority"])
	assert.Equal(t, "1", m["job.attempt"])
}

func TestMapCarrier(t *testing.T) {
	c := make(mapCarrier)
	c.Set("traceparent", "00-abc-def-01")
	c.Set("tracestate", "vendor=value")

	assert.Equal(t, "00-abc-def-01", c.Get("traceparent"))
	assert.Equal(t, "", c.Get("nonexistent"))
	assert.ElementsMatch(t, []string{"traceparent", "tracestate"}, c.Keys())
}

// newWALFileDB returns a WAL file-backed SQLite DB for instrumented-worker tests
// that exercise a waiting/complete write handoff. The shared-cache in-memory DSN
// ("mode=memory&cache=shared") has a single global writer lock and surfaces
// contention as SQLITE_LOCKED — which busy_timeout does NOT retry (adding it
// measurably WORSENS contention) — so the worker's MarkWaiting/complete write can
// stall a test's wall-clock budget under -race (the ~1% TestInstrumentedWorkerEnds
// ProcessSpanOnWaiting flake). A WAL FILE DB allows concurrent readers + one writer
// with a busy_timeout, eliminating the stall.
func newWALFileDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/otel.db?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestInstrumentedWorkerEndsProcessSpanOnComplete(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	db := newWALFileDB(t)

	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))

	q := queue.New(store)
	Instrument(q,
		WithTracerProvider(tp),
		WithPropagator(propagation.TraceContext{}),
	)

	q.Register("otel-complete-job", func(context.Context, struct{}) error {
		return nil
	})
	completed := make(chan struct{})
	q.OnJobComplete(func(context.Context, *core.Job) {
		close(completed)
	})

	_, err := q.Enqueue(context.Background(), "otel-complete-job", struct{}{})
	require.NoError(t, err)

	w := worker.NewWorker(q, worker.WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	case <-time.After(2 * time.Second):
		t.Fatal("instrumented job did not complete")
	}

	require.Eventually(t, func() bool {
		for _, span := range recorder.Ended() {
			if span.Name() == "job.process otel-complete-job" && !span.EndTime().IsZero() {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "worker completion path must end the job.process span")

	var processSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "job.process otel-complete-job" {
			processSpan = span
			break
		}
	}
	require.NotNil(t, processSpan)
	assert.Equal(t, codes.Ok, processSpan.Status().Code)
	assert.False(t, processSpan.EndTime().IsZero())
}

func TestInstrumentedWorkerEndsProcessSpanOnWaiting(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	db := newWALFileDB(t)

	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))

	q := queue.New(store)
	Instrument(q,
		WithTracerProvider(tp),
		WithPropagator(propagation.TraceContext{}),
	)

	// The handler self-suspends by waiting on a signal that is never delivered,
	// which returns a *signal.WaitingError. The worker leaves the job in
	// StatusWaiting and must fire the waiting hook to end the per-attempt span.
	waited := make(chan struct{})
	var waitedOnce sync.Once
	q.Register("otel-waiting-job", func(ctx context.Context, _ struct{}) error {
		_, err := signal.WaitForSignal[string](ctx, "go")
		waitedOnce.Do(func() { close(waited) })
		return err
	})

	jobID, err := q.Enqueue(context.Background(), "otel-waiting-job", struct{}{})
	require.NoError(t, err)

	w := worker.NewWorker(q, worker.WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("instrumented job did not reach the signal wait")
	}

	// The job must be parked in StatusWaiting and its span ended by the hook.
	require.Eventually(t, func() bool {
		status, statusErr := q.LoadStatus(context.Background(), jobID)
		if statusErr != nil || status != core.StatusWaiting {
			return false
		}
		for _, span := range recorder.Ended() {
			if span.Name() == "job.process otel-waiting-job" && !span.EndTime().IsZero() {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "worker waiting path must end the job.process span")

	var processSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "job.process otel-waiting-job" {
			processSpan = span
			break
		}
	}
	require.NotNil(t, processSpan)
	assert.False(t, processSpan.EndTime().IsZero())
	// Waiting is neither OK nor Error.
	assert.Equal(t, codes.Unset, processSpan.Status().Code)
}

// spanAttrMap converts a slice of KeyValues into a string map for easy assertion.
func spanAttrMap(attrs []attribute.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.Emit()
	}
	return m
}
