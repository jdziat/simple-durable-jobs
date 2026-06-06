---
title: "Metrics"
weight: 16
---

`pkg/metrics` adds optional OpenTelemetry metrics for queues and workers. It is
not enabled by default and is complementary to `pkg/otel` tracing: tracing shows
individual job executions, while metrics provide aggregate time series for
scraping and alerting.

## Prometheus Handler

Use `NewPrometheusHandler` when you want a ready-to-mount `/metrics` endpoint.
Pass the returned meter provider into `Instrument`.

```go
package main

import (
	"context"
	"net/http"

	jobs "github.com/jdziat/simple-durable-jobs"
	jobsmetrics "github.com/jdziat/simple-durable-jobs/pkg/metrics"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	handler, meterProvider, err := jobsmetrics.NewPrometheusHandler()
	if err != nil {
		panic(err)
	}
	defer meterProvider.Shutdown(ctx)

	jobsmetrics.Instrument(q, jobsmetrics.WithMeterProvider(meterProvider))
	http.Handle("/metrics", handler)
	http.ListenAndServe(":8080", nil)
}
```

## Bring Your Own MeterProvider

If your service already owns OpenTelemetry SDK setup, pass that provider instead.

```go
jobsmetrics.Instrument(queue, jobsmetrics.WithMeterProvider(meterProvider))
```

Without `WithMeterProvider`, instrumentation uses `otel.GetMeterProvider()`.

## Metric Catalog

| Name | Type | Unit | Attributes | Description |
|---|---|---|---|---|
| `jobs.started` | `Int64Counter` | `{job}` | `queue`, `job.type`, `outcome=started` | Worker attempts started. |
| `jobs.completed` | `Int64Counter` | `{job}` | `queue`, `job.type`, `outcome=completed` | Jobs completed successfully. |
| `jobs.failed` | `Int64Counter` | `{job}` | `queue`, `job.type`, `outcome=failed` | Jobs that reached terminal failure. |
| `jobs.retried` | `Int64Counter` | `{job}` | `queue`, `job.type`, `outcome=retried` | Job attempts scheduled for retry. |
| `jobs.wait.duration` | `Float64Histogram` | `s` | `queue`, `job.type`, `outcome=started` | Time from enqueue to worker start. |
| `jobs.run.duration` | `Float64Histogram` | `s` | `queue`, `job.type`, `outcome=completed\|failed` | Time from worker start to terminal outcome. |
| `jobs.queue.depth` | `Int64ObservableGauge` | `{job}` | `queue`, `outcome=pending\|running` | Current pending and running depth by queue. |

Queue depth is collected through optional storage capabilities returning plain Go
maps, not UI protobufs. `GormStorage` supports these capabilities. Custom
storage backends that do not implement them still get throughput, latency,
failure, and retry metrics; only the queue-depth gauge is skipped.
