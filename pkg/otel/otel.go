// Package otel provides optional OpenTelemetry tracing for the jobs package.
//
// Usage:
//
//	import jobsotel "github.com/jdziat/simple-durable-jobs/pkg/otel"
//
//	queue := jobs.New(storage)
//	jobsotel.Instrument(queue)
//
// This registers enqueue middleware and lifecycle hooks that create spans
// for job enqueue, execution, completion, failure, and retry events.
// Trace context is propagated from the enqueue site to the worker via
// the Job.TraceContext field.
package otel

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

const instrumentationName = "github.com/jdziat/simple-durable-jobs/pkg/otel"

// instrumentConfig holds configuration for the OTel integration.
type instrumentConfig struct {
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
}

// InstrumentOption configures the OTel integration.
type InstrumentOption func(*instrumentConfig)

// WithTracerProvider sets the OTel TracerProvider. Defaults to the global provider.
func WithTracerProvider(tp trace.TracerProvider) InstrumentOption {
	return func(c *instrumentConfig) {
		c.tracerProvider = tp
	}
}

// WithPropagator sets the text map propagator. Defaults to the global propagator.
func WithPropagator(p propagation.TextMapPropagator) InstrumentOption {
	return func(c *instrumentConfig) {
		c.propagator = p
	}
}

// Instrument wires OpenTelemetry tracing into a Queue.
// It registers enqueue middleware for trace context propagation and
// lifecycle hooks for execution spans.
func Instrument(q *queue.Queue, opts ...InstrumentOption) {
	cfg := &instrumentConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.tracerProvider == nil {
		cfg.tracerProvider = otel.GetTracerProvider()
	}
	if cfg.propagator == nil {
		cfg.propagator = otel.GetTextMapPropagator()
	}

	tracer := cfg.tracerProvider.Tracer(instrumentationName)

	// Register enqueue middleware to inject trace context and create enqueue span
	q.UseEnqueueMiddleware(enqueueMiddleware(tracer, cfg.propagator))

	// Register context-modifying start hook to create execution span
	q.OnJobStartCtx(startCtxHook(tracer, cfg.propagator))

	// Register complete hook to end span with OK
	q.OnJobComplete(completeHook())

	// Register fail hook to end span with error
	q.OnJobFail(failHook())

	// Register retry hook to add retry event to span
	q.OnRetry(retryHook())
}

// enqueueMiddleware returns middleware that serializes the current span context
// into the Job.TraceContext field and creates a short enqueue span.
func enqueueMiddleware(tracer trace.Tracer, prop propagation.TextMapPropagator) queue.EnqueueMiddleware {
	return func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		ctx, span := tracer.Start(ctx, "job.enqueue",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(jobAttributes(job)...),
		)
		defer span.End()

		// Serialize span context into the job for worker-side propagation
		carrier := make(mapCarrier)
		prop.Inject(ctx, carrier)
		if len(carrier) > 0 {
			data, err := json.Marshal(carrier)
			if err == nil {
				job.TraceContext = data
			}
		}

		if err := next(ctx, job); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}

		span.SetAttributes(attribute.String("job.id", job.ID))
		return nil
	}
}

// startCtxHook returns a context-modifying hook that deserializes the trace
// context from the job and starts an execution span.
func startCtxHook(tracer trace.Tracer, prop propagation.TextMapPropagator) func(context.Context, *core.Job) context.Context {
	return func(ctx context.Context, job *core.Job) context.Context {
		// Deserialize trace context from the job
		if len(job.TraceContext) > 0 {
			var carrier mapCarrier
			if err := json.Unmarshal(job.TraceContext, &carrier); err == nil {
				ctx = prop.Extract(ctx, carrier)
			}
		}

		ctx, span := tracer.Start(ctx, fmt.Sprintf("job.process %s", job.Type),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(jobAttributes(job)...),
		)

		// Store the span in context so completeHook/failHook can retrieve it.
		// We use standard OTel trace.SpanFromContext which is already set by tracer.Start.
		_ = span
		return ctx
	}
}

// completeHook returns a hook that ends the execution span with OK status.
func completeHook() func(context.Context, *core.Job) {
	return func(ctx context.Context, job *core.Job) {
		span := trace.SpanFromContext(ctx)
		if !span.IsRecording() {
			return
		}
		span.SetStatus(codes.Ok, "")
		span.End()
	}
}

// failHook returns a hook that ends the execution span with error status.
func failHook() func(context.Context, *core.Job, error) {
	return func(ctx context.Context, job *core.Job, err error) {
		span := trace.SpanFromContext(ctx)
		if !span.IsRecording() {
			return
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
	}
}

// retryHook returns a hook that adds a retry event to the execution span.
func retryHook() func(context.Context, *core.Job, int, error) {
	return func(ctx context.Context, job *core.Job, attempt int, err error) {
		span := trace.SpanFromContext(ctx)
		if !span.IsRecording() {
			return
		}
		span.AddEvent("job.retry", trace.WithAttributes(
			attribute.Int("job.attempt", attempt),
			attribute.String("job.error", err.Error()),
		))
	}
}

// jobAttributes returns common span attributes for a job.
func jobAttributes(job *core.Job) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("job.type", job.Type),
		attribute.String("job.queue", job.Queue),
		attribute.Int("job.priority", job.Priority),
		attribute.Int("job.attempt", job.Attempt),
	}
	if job.ID != "" {
		attrs = append(attrs, attribute.String("job.id", job.ID))
	}
	return attrs
}

// mapCarrier is a propagation.TextMapCarrier backed by a map.
type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string       { return c[key] }
func (c mapCarrier) Set(key, value string)        { c[key] = value }
func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
