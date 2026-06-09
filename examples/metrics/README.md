# Metrics

This example demonstrates the optional Prometheus metrics exporter.

## What It Does

- Sets up a SQLite-backed queue and instruments it with `pkg/metrics`
- Creates a Prometheus `/metrics` handler with `jobsmetrics.NewPrometheusHandler()`
- Enqueues three jobs and scrapes queue depth before the worker starts
- Starts a worker and scrapes completed, started, and queue depth metrics after drain
- Notes the wait and run duration histograms exposed by the same instrumentation

## Prerequisites

- Go 1.25+
- No external services needed (uses SQLite)

## Running

```bash
go run ./examples/metrics
```

The example uses an in-process HTTP test server for the scrape and then shuts down.

## Expected Output

```
Enqueued metric job 6d389568
Enqueued metric job 5d1583c5
Enqueued metric job 9a4dcb97
Queue depth before worker starts:
jobs_queue_depth{otel_scope_name="github.com/jdziat/simple-durable-jobs/v2/pkg/metrics",otel_scope_schema_url="",otel_scope_version="",outcome="pending",queue="default"} 3
Starting worker...
[worker] Processing metric job 1
[worker] Processing metric job 2
[worker] Processing metric job 3
Processed job metrics after worker completes:
jobs_completed_total{job_type="metric-job",otel_scope_name="github.com/jdziat/simple-durable-jobs/v2/pkg/metrics",otel_scope_schema_url="",otel_scope_version="",outcome="completed",queue="default"} 3
jobs_started_total{job_type="metric-job",otel_scope_name="github.com/jdziat/simple-durable-jobs/v2/pkg/metrics",otel_scope_schema_url="",otel_scope_version="",outcome="started",queue="default"} 3
jobs_queue_depth: no pending series (queue drained)
Histogram metrics also exist for jobs.wait.duration and jobs.run.duration.
Metrics example complete: scraped real queue metrics
```

After the worker drains the queue, there is no pending queue-depth series, so the example prints that state explicitly.

## Key Concepts

- **Optional instrumentation** - Metrics are opt-in via `jobsmetrics.Instrument(queue)`.
- **Prometheus export** - `jobsmetrics.NewPrometheusHandler()` returns a scrape handler and OpenTelemetry meter provider.
- **Queue depth gauge** reports pending work by queue and outcome while pending rows exist.
- **Lifecycle counters** record started and completed jobs by job type, queue, and outcome.
- **Duration histograms** such as `jobs.wait.duration` and `jobs.run.duration` are available for latency analysis.

## Tips

- Mount the Prometheus handler in your application when you want external scrapers to collect queue metrics.
- Reuse your existing OpenTelemetry `MeterProvider` with `jobsmetrics.WithMeterProvider()` if your service already has one.
- Scrape queue depth before and after load tests to confirm workers are draining jobs as expected.
- Metrics complement lifecycle hooks and tracing; use all three for production observability.

## Related Documentation

- [Advanced - Metrics](https://jdziat.github.io/simple-durable-jobs/docs/advanced/metrics/)
- [API Reference - Observability](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/observability/)
- [Embedded UI](https://jdziat.github.io/simple-durable-jobs/docs/embedded-ui/)
- [pkg.go.dev - metrics package](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs/pkg/metrics)
