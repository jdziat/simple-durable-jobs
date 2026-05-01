package otel

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
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
	hook(ctx, &core.Job{ID: "job-3"}, 2, retryErr)

	span.End()
	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var processSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "job.process retry-job" {
			processSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, processSpan)
	require.Len(t, processSpan.Events, 1)
	assert.Equal(t, "job.retry", processSpan.Events[0].Name)

	eventAttrs := spanAttrMap(processSpan.Events[0].Attributes)
	assert.Equal(t, "2", eventAttrs["job.attempt"])
	assert.Equal(t, "temporary failure", eventAttrs["job.error"])
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

// spanAttrMap converts a slice of KeyValues into a string map for easy assertion.
func spanAttrMap(attrs []attribute.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.Emit()
	}
	return m
}
