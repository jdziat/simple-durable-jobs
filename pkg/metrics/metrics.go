// Package metrics provides optional OpenTelemetry metrics for the jobs package.
//
// Usage:
//
//	import jobsmetrics "github.com/jdziat/simple-durable-jobs/v4/pkg/metrics"
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

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

const instrumentationName = "github.com/jdziat/simple-durable-jobs/v4/pkg/metrics"

const (
	metricJobsStarted      = "jobs.started"
	metricJobsCompleted    = "jobs.completed"
	metricJobsFailed       = "jobs.failed"
	metricJobsRetried      = "jobs.retried"
	metricJobWaitDuration  = "jobs.wait.duration"
	metricJobRunDuration   = "jobs.run.duration"
	metricQueueDepth       = "jobs.queue.depth"
	metricBacklogOldestAge = "jobs.queue.backlog.oldest_age"
	metricDeadLetterDepth  = "jobs.dead_letter.depth"
	metricQueueSaturation  = "jobs.queue.saturation"
	metricLeasesReclaimed  = "jobs.leases.reclaimed"
	metricClockSkewDropped = "jobs.clock_skew_dropped"

	metricDequeueReleased         = "jobs.dequeue.released"
	metricDequeueSuppressedTicks  = "jobs.dequeue.suppressed_ticks"
	metricRateSaturationCacheSize = "jobs.dequeue.rate_saturation_cache_size"

	attrQueue    = "queue"
	attrJobType  = "job.type"
	attrOutcome  = "outcome"
	attrReason   = "reason"
	attrWorkerID = "worker.id"
	attrMetric   = "metric"

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
	registerBacklogOldestAgeGauge(meter, q.Storage())
	registerDeadLetterDepthGauge(meter, q.Storage())

	q.OnJobStart(startHook(inst))
	q.OnJobComplete(completeHook(inst))
	q.OnJobFail(failHook(inst))
	q.OnRetry(retryHook(inst))
	q.OnJobReclaimed(reclaimHook(inst))
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
	started          metric.Int64Counter
	completed        metric.Int64Counter
	failed           metric.Int64Counter
	retried          metric.Int64Counter
	reclaimed        metric.Int64Counter
	waitSeconds      metric.Float64Histogram
	runSeconds       metric.Float64Histogram
	clockSkewDropped metric.Int64Counter
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
	reclaimed, err := meter.Int64Counter(metricLeasesReclaimed,
		metric.WithUnit("{job}"),
		metric.WithDescription("Job leases reclaimed from a presumed-dead owner (stale-lock reaper) or observed reclaimed by a peer (ownership audit)."))
	if err != nil {
		return nil, err
	}
	clockSkewDropped, err := meter.Int64Counter(metricClockSkewDropped,
		metric.WithUnit("{sample}"),
		metric.WithDescription("Latency samples dropped because the computed duration was negative — a symptom of DB-clock vs worker-wall-clock skew. Labeled by the affected histogram (wait/run)."))
	if err != nil {
		return nil, err
	}
	return &instruments{
		started:          started,
		completed:        completed,
		failed:           failed,
		retried:          retried,
		reclaimed:        reclaimed,
		waitSeconds:      waitSeconds,
		runSeconds:       runSeconds,
		clockSkewDropped: clockSkewDropped,
	}, nil
}

func startHook(inst *instruments) func(context.Context, *core.Job) {
	return func(ctx context.Context, job *core.Job) {
		attrs := jobAttributes(job, outcomeStarted)
		inst.started.Add(ctx, 1, metric.WithAttributes(attrs...))
		if job.StartedAt != nil && !job.CreatedAt.IsZero() {
			recordDuration(ctx, inst, inst.waitSeconds, metricJobWaitDuration, job.StartedAt.Sub(job.CreatedAt), attrs)
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

func reclaimHook(inst *instruments) func(context.Context, core.UUID, string) {
	return func(ctx context.Context, _ core.UUID, reason string) {
		inst.reclaimed.Add(ctx, 1, metric.WithAttributes(attribute.String(attrReason, reason)))
	}
}

func recordRunDuration(ctx context.Context, inst *instruments, job *core.Job, attrs []attribute.KeyValue) {
	if job.StartedAt == nil {
		return
	}
	// NOTE: StartedAt is a DB-clock timestamp while time.Since reads the worker
	// wall clock, so this run-duration endpoint pair can straddle two clocks. The
	// wait histogram (StartedAt-CreatedAt) is DB-clock on both ends. recordDuration
	// surfaces any resulting skew via the clockSkewDropped counter rather than
	// dropping negative samples silently.
	recordDuration(ctx, inst, inst.runSeconds, metricJobRunDuration, time.Since(*job.StartedAt), attrs)
}

// recordDuration records a latency sample, discarding physically-impossible
// negative durations. A negative value means the two endpoints came from clocks
// that disagree (DB clock vs worker wall clock under NTP skew); instead of
// dropping it silently it is counted on clockSkewDropped, labeled by histogram,
// so operators can observe skew directly instead of inferring it from a gap.
func recordDuration(ctx context.Context, inst *instruments, hist metric.Float64Histogram, metricName string, d time.Duration, attrs []attribute.KeyValue) {
	if d < 0 {
		inst.clockSkewDropped.Add(ctx, 1, metric.WithAttributes(attribute.String(attrMetric, metricName)))
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

type queueOldestPendingAtSource interface {
	QueueOldestPendingAt(context.Context) (map[string]time.Time, error)
}

type queueDeadLetterCountSource interface {
	QueueDeadLetterCounts(context.Context) (map[string]int, error)
}

// QueueRunningSnapshotFunc returns a point-in-time running job count by queue.
type QueueRunningSnapshotFunc func(context.Context) (map[string]int, error)

// InstrumentQueueSaturation registers jobs.queue.saturation for a worker.
// The worker supplies its configured per-queue capacity and a running-count
// snapshot source because storage cannot know worker-local capacity.
func InstrumentQueueSaturation(workerID string, capacities map[string]int, running QueueRunningSnapshotFunc, opts ...InstrumentOption) {
	if running == nil {
		slog.Default().Warn("queue saturation gauge disabled", "error", "missing running count source")
		return
	}

	cfg := &instrumentConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.meterProvider == nil {
		cfg.meterProvider = otel.GetMeterProvider()
	}

	capacitySnapshot := make(map[string]int, len(capacities))
	for queueName, capacity := range capacities {
		capacitySnapshot[queueName] = capacity
	}

	meter := cfg.meterProvider.Meter(instrumentationName)
	_, err := meter.Float64ObservableGauge(metricQueueSaturation,
		metric.WithUnit("1"),
		metric.WithDescription("Worker-local running jobs divided by configured capacity by queue."),
		metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
			runningCounts, err := running(ctx)
			if err != nil {
				return err
			}
			for queueName, capacity := range capacitySnapshot {
				if capacity <= 0 {
					continue
				}
				observer.Observe(float64(runningCounts[queueName])/float64(capacity), metric.WithAttributes(
					attribute.String(attrWorkerID, workerID),
					attribute.String(attrQueue, queueName),
				))
			}
			return nil
		}))
	if err != nil {
		slog.Default().Warn("queue saturation gauge disabled", "error", err)
	}
}

// InstrumentWorkerDequeue registers the cto-F2 dispatch-churn observability for
// a worker: jobs.dequeue.released{worker.id,reason} (jobs claimed then released
// back to pending, by reason) and jobs.dequeue.suppressed_ticks{worker.id}
// (poll ticks the rate-saturation throttle skipped claiming). Both are
// monotonic observable counters read from the worker's own atomic snapshots, so
// pkg/metrics stays decoupled from pkg/worker (pass worker.DequeueReleasedByReason
// and worker.DequeueSuppressedTicks as the func values).
//
// A healthy throttled worker shows suppressed_ticks rising while
// released{reason="fleet_rate"} stays low (one probe batch per rate window) —
// the throttle collapsing the claim->release churn, not a stuck worker.
func InstrumentWorkerDequeue(workerID string, releasedByReason func() map[string]int64, suppressedTicks func() int64, opts ...InstrumentOption) {
	if releasedByReason == nil && suppressedTicks == nil {
		slog.Default().Warn("worker dequeue metrics disabled", "error", "no snapshot sources")
		return
	}

	cfg := &instrumentConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.meterProvider == nil {
		cfg.meterProvider = otel.GetMeterProvider()
	}
	meter := cfg.meterProvider.Meter(instrumentationName)

	if releasedByReason != nil {
		_, err := meter.Int64ObservableCounter(metricDequeueReleased,
			metric.WithUnit("{job}"),
			metric.WithDescription("Dequeued jobs released back to pending without running, by reason."),
			metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
				for reason, count := range releasedByReason() {
					observer.Observe(count, metric.WithAttributes(
						attribute.String(attrWorkerID, workerID),
						attribute.String(attrReason, reason),
					))
				}
				return nil
			}))
		if err != nil {
			slog.Default().Warn("dequeue released counter disabled", "error", err)
		}
	}

	if suppressedTicks != nil {
		_, err := meter.Int64ObservableCounter(metricDequeueSuppressedTicks,
			metric.WithUnit("{tick}"),
			metric.WithDescription("Poll ticks the rate-saturation throttle skipped claiming jobs."),
			metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
				observer.Observe(suppressedTicks(), metric.WithAttributes(
					attribute.String(attrWorkerID, workerID),
					attribute.String(attrReason, "fleet_rate_saturated"),
				))
				return nil
			}))
		if err != nil {
			slog.Default().Warn("dequeue suppressed-ticks counter disabled", "error", err)
		}
	}
}

// InstrumentWorkerRateSaturation registers jobs.dequeue.rate_saturation_cache_size
// {worker.id} — the current number of saturated rate-limit buckets a worker is
// caching for the cto-F2 per-key cooldown. When this sits at the configured cap
// (WithRateSaturationCacheSize) the cooldown is shedding new buckets to the DB
// (a high-cardinality-RateLimitKey signal). Pass worker.DequeueRateSaturationCacheSize
// directly (it returns int64). Decoupled from pkg/worker via a plain func value.
func InstrumentWorkerRateSaturation(workerID string, cacheSize func() int64, opts ...InstrumentOption) {
	if cacheSize == nil {
		slog.Default().Warn("worker rate-saturation gauge disabled", "error", "no cache-size source")
		return
	}
	cfg := &instrumentConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.meterProvider == nil {
		cfg.meterProvider = otel.GetMeterProvider()
	}
	meter := cfg.meterProvider.Meter(instrumentationName)
	_, err := meter.Int64ObservableGauge(metricRateSaturationCacheSize,
		metric.WithUnit("{bucket}"),
		metric.WithDescription("Saturated rate-limit buckets cached by the per-key cooldown; at the cap means new buckets fall back to the DB rate transaction."),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(cacheSize(), metric.WithAttributes(attribute.String(attrWorkerID, workerID)))
			return nil
		}))
	if err != nil {
		slog.Default().Warn("worker rate-saturation gauge disabled", "error", err)
	}
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

func registerBacklogOldestAgeGauge(meter metric.Meter, s core.Storage) {
	source, ok := s.(queueOldestPendingAtSource)
	if !ok {
		slog.Default().Warn("storage backend does not support backlog oldest age metrics; backlog oldest age gauge disabled")
		return
	}

	_, err := meter.Float64ObservableGauge(metricBacklogOldestAge,
		metric.WithUnit("s"),
		metric.WithDescription("Age in seconds of the oldest pending job by queue."),
		metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
			oldestByQueue, err := source.QueueOldestPendingAt(ctx)
			if err != nil {
				return err
			}
			now := time.Now()
			for queueName, oldest := range oldestByQueue {
				age := now.Sub(oldest)
				if age < 0 {
					continue
				}
				observer.Observe(age.Seconds(), metric.WithAttributes(attribute.String(attrQueue, queueName)))
			}
			return nil
		}))
	if err != nil {
		slog.Default().Warn("backlog oldest age gauge disabled", "error", err)
	}
}

func registerDeadLetterDepthGauge(meter metric.Meter, s core.Storage) {
	source, ok := s.(queueDeadLetterCountSource)
	if !ok {
		slog.Default().Warn("storage backend does not support dead letter depth metrics; dead letter depth gauge disabled")
		return
	}

	_, err := meter.Int64ObservableGauge(metricDeadLetterDepth,
		metric.WithUnit("{job}"),
		metric.WithDescription("Current dead-letter job depth by queue."),
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			counts, err := source.QueueDeadLetterCounts(ctx)
			if err != nil {
				return err
			}
			for queueName, count := range counts {
				observer.Observe(int64(count), metric.WithAttributes(attribute.String(attrQueue, queueName)))
			}
			return nil
		}))
	if err != nil {
		slog.Default().Warn("dead letter depth gauge disabled", "error", err)
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
