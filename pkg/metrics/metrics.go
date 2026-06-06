// Package metrics provides optional OpenTelemetry metrics for the jobs package.
//
// Usage:
//
//	import jobsmetrics "github.com/jdziat/simple-durable-jobs/pkg/metrics"
//
//	queue := jobs.New(storage)
//	jobsmetrics.Instrument(queue)
//
// This registers lifecycle hooks that emit job throughput, retry/failure,
// latency, and queue-depth metrics. It is opt-in and adds no overhead unless
// Instrument is called.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	prometheusclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

const instrumentationName = "github.com/jdziat/simple-durable-jobs/pkg/metrics"

const (
	metricJobsStarted     = "jobs.started"
	metricJobsCompleted   = "jobs.completed"
	metricJobsFailed      = "jobs.failed"
	metricJobsRetried     = "jobs.retried"
	metricJobWaitDuration = "jobs.wait.duration"
	metricJobRunDuration  = "jobs.run.duration"
	metricQueueDepth      = "jobs.queue.depth"

	attrQueue   = "queue"
	attrJobType = "job.type"
	attrOutcome = "outcome"

	outcomeStarted   = "started"
	outcomeCompleted = "completed"
	outcomeFailed    = "failed"
	outcomeRetried   = "retried"
	outcomePending   = "pending"
	outcomeRunning   = "running"
)

// instrumentConfig holds configuration for the OTel metrics integration.
type instrumentConfig struct {
	meterProvider metric.MeterProvider
}

// InstrumentOption configures the OTel metrics integration.
type InstrumentOption func(*instrumentConfig)

// WithMeterProvider sets the OTel MeterProvider. Defaults to the global provider.
func WithMeterProvider(mp metric.MeterProvider) InstrumentOption {
	return func(c *instrumentConfig) {
		c.meterProvider = mp
	}
}

// Instrument wires OpenTelemetry metrics into a Queue.
// It registers lifecycle hooks for job throughput, latency, failures, retries,
// and an optional observable queue-depth gauge when the storage supports it.
func Instrument(q *queue.Queue, opts ...InstrumentOption) {
	cfg := &instrumentConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.meterProvider == nil {
		cfg.meterProvider = otel.GetMeterProvider()
	}

	meter := cfg.meterProvider.Meter(instrumentationName)
	inst, err := newInstruments(meter)
	if err != nil {
		slog.Default().Warn("jobs metrics instrumentation disabled", "error", err)
		return
	}

	registerQueueDepthGauge(meter, q.Storage())

	q.OnJobStart(startHook(inst))
	q.OnJobComplete(completeHook(inst))
	q.OnJobFail(failHook(inst))
	q.OnRetry(retryHook(inst))
}

// NewPrometheusHandler creates a Prometheus scrape handler and a MeterProvider
// ready to pass to Instrument via WithMeterProvider.
func NewPrometheusHandler(opts ...otelprometheus.Option) (http.Handler, *sdkmetric.MeterProvider, error) {
	registry := prometheusclient.NewRegistry()
	exporter, err := otelprometheus.New(append(opts, otelprometheus.WithRegisterer(registry))...)
	if err != nil {
		return nil, nil, err
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	return handler, mp, nil
}

type instruments struct {
	started     metric.Int64Counter
	completed   metric.Int64Counter
	failed      metric.Int64Counter
	retried     metric.Int64Counter
	waitSeconds metric.Float64Histogram
	runSeconds  metric.Float64Histogram
}

func newInstruments(meter metric.Meter) (*instruments, error) {
	started, err := meter.Int64Counter(metricJobsStarted,
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs started by workers."))
	if err != nil {
		return nil, err
	}
	completed, err := meter.Int64Counter(metricJobsCompleted,
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs completed successfully."))
	if err != nil {
		return nil, err
	}
	failed, err := meter.Int64Counter(metricJobsFailed,
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs that reached terminal failure."))
	if err != nil {
		return nil, err
	}
	retried, err := meter.Int64Counter(metricJobsRetried,
		metric.WithUnit("{job}"),
		metric.WithDescription("Job attempts scheduled for retry."))
	if err != nil {
		return nil, err
	}
	waitSeconds, err := meter.Float64Histogram(metricJobWaitDuration,
		metric.WithUnit("s"),
		metric.WithDescription("Seconds from enqueue to worker start."))
	if err != nil {
		return nil, err
	}
	runSeconds, err := meter.Float64Histogram(metricJobRunDuration,
		metric.WithUnit("s"),
		metric.WithDescription("Seconds from worker start to terminal outcome."))
	if err != nil {
		return nil, err
	}
	return &instruments{
		started:     started,
		completed:   completed,
		failed:      failed,
		retried:     retried,
		waitSeconds: waitSeconds,
		runSeconds:  runSeconds,
	}, nil
}

func startHook(inst *instruments) func(context.Context, *core.Job) {
	return func(ctx context.Context, job *core.Job) {
		attrs := jobAttributes(job, outcomeStarted)
		inst.started.Add(ctx, 1, metric.WithAttributes(attrs...))
		if job.StartedAt != nil && !job.CreatedAt.IsZero() {
			recordDuration(ctx, inst.waitSeconds, job.StartedAt.Sub(job.CreatedAt), attrs)
		}
	}
}

func completeHook(inst *instruments) func(context.Context, *core.Job) {
	return func(ctx context.Context, job *core.Job) {
		attrs := jobAttributes(job, outcomeCompleted)
		inst.completed.Add(ctx, 1, metric.WithAttributes(attrs...))
		recordRunDuration(ctx, inst, job, attrs)
	}
}

func failHook(inst *instruments) func(context.Context, *core.Job, error) {
	return func(ctx context.Context, job *core.Job, _ error) {
		attrs := jobAttributes(job, outcomeFailed)
		inst.failed.Add(ctx, 1, metric.WithAttributes(attrs...))
		recordRunDuration(ctx, inst, job, attrs)
	}
}

func retryHook(inst *instruments) func(context.Context, *core.Job, int, error) {
	return func(ctx context.Context, job *core.Job, _ int, _ error) {
		attrs := jobAttributes(job, outcomeRetried)
		inst.retried.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

func recordRunDuration(ctx context.Context, inst *instruments, job *core.Job, attrs []attribute.KeyValue) {
	if job.StartedAt == nil {
		return
	}
	recordDuration(ctx, inst.runSeconds, time.Since(*job.StartedAt), attrs)
}

func recordDuration(ctx context.Context, hist metric.Float64Histogram, d time.Duration, attrs []attribute.KeyValue) {
	if d < 0 {
		return
	}
	hist.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
}

func jobAttributes(job *core.Job, outcome string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(attrQueue, job.Queue),
		attribute.String(attrJobType, job.Type),
		attribute.String(attrOutcome, outcome),
	}
}

type queuePendingCountSource interface {
	QueuePendingCounts(context.Context) (map[string]int, error)
}

type queueRunningCountSource interface {
	QueueRunningCounts(context.Context) (map[string]int, error)
}

func registerQueueDepthGauge(meter metric.Meter, s core.Storage) {
	pending, ok := s.(queuePendingCountSource)
	if !ok {
		slog.Default().Warn("storage backend does not support queue depth metrics; queue depth gauge disabled")
		return
	}
	running, _ := s.(queueRunningCountSource)

	_, err := meter.Int64ObservableGauge(metricQueueDepth,
		metric.WithUnit("{job}"),
		metric.WithDescription("Current queue depth by queue and status."),
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			if err := observePendingQueueCounts(ctx, observer, pending); err != nil {
				return err
			}
			if running != nil {
				return observeRunningQueueCounts(ctx, observer, running)
			}
			return nil
		}))
	if err != nil {
		slog.Default().Warn("queue depth gauge disabled", "error", err)
	}
}

func observePendingQueueCounts(ctx context.Context, observer metric.Int64Observer, source queuePendingCountSource) error {
	counts, err := source.QueuePendingCounts(ctx)
	if err != nil {
		return err
	}
	observeQueueCounts(observer, counts, outcomePending)
	return nil
}

func observeRunningQueueCounts(ctx context.Context, observer metric.Int64Observer, source queueRunningCountSource) error {
	counts, err := source.QueueRunningCounts(ctx)
	if err != nil {
		return err
	}
	observeQueueCounts(observer, counts, outcomeRunning)
	return nil
}

func observeQueueCounts(observer metric.Int64Observer, counts map[string]int, outcome string) {
	for queueName, count := range counts {
		observer.Observe(int64(count), metric.WithAttributes(
			attribute.String(attrQueue, queueName),
			attribute.String(attrOutcome, outcome),
		))
	}
}
