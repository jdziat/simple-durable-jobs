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

	jobs "github.com/jdziat/simple-durable-jobs/v3"
	jobsmetrics "github.com/jdziat/simple-durable-jobs/v3/pkg/metrics"
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
| `jobs.queue.backlog.oldest_age` | `Float64ObservableGauge` | `s` | `queue` | Age in seconds of the oldest pending job by queue. |
| `jobs.dead_letter.depth` | `Int64ObservableGauge` | `{job}` | `queue` | Current dead-lettered job depth by queue. |
| `jobs.queue.saturation` | `Float64ObservableGauge` | `1` | `queue`, `worker.id` | Worker-local running jobs divided by configured capacity by queue. |
| `jobs.leases.reclaimed` | `Int64Counter` | `{job}` | `reason=stale_lock\|ownership_audit` | Job leases reclaimed from a presumed-dead owner or observed reclaimed by a peer. |
| `jobs.dequeue.released` | `Int64ObservableCounter` | `{job}` | `worker.id`, `reason=queue_cap\|queue_rate\|concurrency\|fleet_rate\|shutdown` | Dequeued jobs released back to pending without running, by reason. |
| `jobs.dequeue.suppressed_ticks` | `Int64ObservableCounter` | `{tick}` | `worker.id`, `reason=fleet_rate_saturated` | Poll ticks the rate-saturation throttle skipped claiming jobs. |

The throughput, latency, depth, backlog-age, dead-letter-depth, and reclaimed
metrics are wired automatically by `Instrument`; `jobs.leases.reclaimed`
is registered through the same call (it hooks `OnJobReclaimed`) and needs no extra
setup. Unlike the throughput, latency, and depth series, it carries no `queue` or
`job.type` attribute — `reason` is its only label. `reason=stale_lock` is the
actor side (this worker's reaper recovered a job from a presumed-dead peer, the
crash leading-indicator), while `reason=ownership_audit` is the victim side (this
worker observed a peer reclaim a job it was still running). In a multi-process
fleet the same logical reclaim can surface once per side on different workers, so
alert on each `reason` separately and do not sum across reason values.

Queue depth, backlog age, and dead-letter depth are collected through optional
storage capabilities returning plain Go maps, not UI protobufs. `GormStorage`
supports these capabilities. Custom storage backends that do not implement them
still get throughput, latency, failure, retry, and reclaimed metrics; only the
unsupported storage-side gauges are skipped.

`jobs.queue.saturation` is worker-side because storage does not know a worker's
configured per-queue capacity. Register it per worker with
`InstrumentQueueSaturation(workerID, capacities, running, ...)`; the gauge
carries `worker.id`. Alert with `avg by (queue)`, not `sum`, so two workers at
50% saturation do not appear as a fake 100% fleet value:

```promql
avg by (queue) (jobs_queue_saturation) > 0.9
```

### Dequeue churn (rate-limit saturation throttle)

`jobs.dequeue.released` and `jobs.dequeue.suppressed_ticks` are worker-side
counters that surface dispatch churn. They are registered per worker with
`InstrumentWorkerDequeue(workerID, worker.DequeueReleasedByReason, worker.DequeueSuppressedTicks, ...)`
and both carry `worker.id`.

A worker dequeues (claims) a job before the final admission gates run; if a gate
denies, the job is released back to pending — a "bounce" counted under
`jobs.dequeue.released` by `reason`. When **every** configured fleet `RateLimit`
is unkeyed, a saturated limit engages a claim-rate throttle: the worker stops
claiming until the limit's rate window rolls over, rather than bouncing the full
concurrency budget every poll tick. Each suppressed tick increments
`jobs.dequeue.suppressed_ticks`.

The healthy steady state under a saturated fleet limit is `suppressed_ticks`
rising while `released{reason="fleet_rate"}` stays low (one probe batch per rate
window) — the throttle collapsing the claim/release write amplification, not a
stuck worker. A `released{reason="fleet_rate"}` rate that climbs every tick means
the throttle is **not** engaging — most often because a *keyed* `RateLimit` is
configured (the throttle only applies to all-unkeyed configs, since a keyed
limit's window can't be checked before a job is claimed). Trade-off: a
just-recovered limit can wait up to one rate window before this worker probes it
again.

```promql
rate(jobs_dequeue_released_total{reason="fleet_rate"}[5m])
```

See [Production Operations]({{< relref "/docs/production-ops" >}}) for the full
alerting guidance and CLI runbooks.
