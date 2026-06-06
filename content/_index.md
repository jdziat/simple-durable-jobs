---
title: Simple Durable Jobs
layout: hextra-home
description: "A Go library for durable job queues with checkpointed workflows, fan-out/fan-in, crash recovery, and an embedded web dashboard."
---

<div class="hx:mt-6 hx:mb-2">
{{< hextra/hero-badge link="https://github.com/jdziat/simple-durable-jobs" >}}
  <div class="hx:w-2 hx:h-2 hx:rounded-full hx:bg-primary-400"></div>
  <span>Open source · MIT licensed · Go</span>
{{< /hextra/hero-badge >}}
</div>

<div class="hx:mt-4 hx:mb-4">
{{< hextra/hero-headline >}}
  Durable job queues for Go
{{< /hextra/hero-headline >}}
</div>

<div class="hx:mb-6">
{{< hextra/hero-subtitle >}}
  Background jobs, checkpointed multi-step workflows, fan-out/fan-in,&nbsp;<br class="hx:hidden sm:hx:block" />crash recovery, and an embedded real-time dashboard — in a single import.
{{< /hextra/hero-subtitle >}}
</div>

<div class="sdj-hero-buttons">
{{< hextra/hero-button text="Get Started" link="docs/getting-started/" >}}
{{< hextra/hero-button text="Live Demo  →" link="docs/live-demo/" style="background:transparent;border:1px solid var(--sdj-accent);color:var(--sdj-accent)" >}}
</div>

<div class="sdj-badges">
  <a href="https://pkg.go.dev/github.com/jdziat/simple-durable-jobs" target="_blank" rel="noopener"><img src="https://pkg.go.dev/badge/github.com/jdziat/simple-durable-jobs.svg" alt="Go Reference" loading="lazy"></a>
  <a href="https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml" target="_blank" rel="noopener"><img src="https://github.com/jdziat/simple-durable-jobs/actions/workflows/ci.yml/badge.svg" alt="CI" loading="lazy"></a>
  <a href="https://goreportcard.com/report/github.com/jdziat/simple-durable-jobs" target="_blank" rel="noopener"><img src="https://goreportcard.com/badge/github.com/jdziat/simple-durable-jobs" alt="Go Report Card" loading="lazy"></a>
  <a href="https://codecov.io/gh/jdziat/simple-durable-jobs" target="_blank" rel="noopener"><img src="https://codecov.io/gh/jdziat/simple-durable-jobs/branch/main/graph/badge.svg" alt="Coverage" loading="lazy"></a>
  <a href="https://github.com/jdziat/simple-durable-jobs" target="_blank" rel="noopener"><img src="https://img.shields.io/github/stars/jdziat/simple-durable-jobs?style=flat&label=Star&color=4f46e5" alt="GitHub stars" loading="lazy"></a>
</div>

<div class="sdj-backends">
  <b>Works with</b>
  <span>SQLite</span>
  <span>PostgreSQL</span>
  <span>MySQL</span>
  <span>Go 1.25+</span>
</div>

<p class="sdj-section-label">Up and running in a few lines</p>

```go
package main

import (
	"context"
	"fmt"

	jobs "github.com/jdziat/simple-durable-jobs"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	db, _ := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
	storage := jobs.NewGormStorage(db)
	_ = storage.Migrate(context.Background())
	queue := jobs.New(storage)

	// Register a typed handler.
	queue.Register("greet", func(ctx context.Context, name string) error {
		fmt.Println("Hello,", name)
		return nil
	})

	// Enqueue work and start processing.
	queue.Enqueue(context.Background(), "greet", "world")
	queue.NewWorker().Start(context.Background())
}
```

<p class="sdj-section-label">Everything you need to run jobs reliably</p>

{{< hextra/feature-grid >}}
  {{< hextra/feature-card
    icon="refresh"
    title="Durable Workflows"
    subtitle="Multi-step workflows with automatic checkpointing. If a workflow fails, it resumes from the last successful step instead of starting over."
  >}}
  {{< hextra/feature-card
    icon="share"
    title="Fan-Out / Fan-In"
    subtitle="Spawn parallel sub-jobs, wait for results, and aggregate. Supports fail-fast, collect-all, and threshold strategies."
  >}}
  {{< hextra/feature-card
    icon="shield-check"
    title="Crash Recovery"
    subtitle="Jobs persist to a database and survive crashes. A stale-lock reaper automatically reclaims jobs stuck on dead workers."
  >}}
  {{< hextra/feature-card
    icon="chart-bar"
    title="Embedded Web UI"
    subtitle="A real-time monitoring dashboard with stats, historical charts, live event streaming, and job management — mounted into your own HTTP server."
  >}}
  {{< hextra/feature-card
    icon="clock"
    title="Scheduled Jobs"
    subtitle="Cron expressions plus daily, weekly, and interval-based scheduling with a built-in scheduler."
  >}}
  {{< hextra/feature-card
    icon="code"
    title="Simple API"
    subtitle="Minimal boilerplate, type-safe handlers, and a clean facade. Import one package and start processing jobs."
  >}}
  {{< hextra/feature-card
    icon="database"
    title="Transactional Enqueue"
    subtitle="Enqueue inside your own database transaction so business rows and the jobs that act on them commit — or roll back — together. No dual-write race."
  >}}
  {{< hextra/feature-card
    icon="bell"
    title="Durable Signals"
    subtitle="Send, wait, check, and drain durable signals to coordinate jobs with each other and with the outside world — buffered, FIFO, and exactly-once."
  >}}
  {{< hextra/feature-card
    icon="exclamation-circle"
    title="Dead-Letter Queue"
    subtitle="Jobs that exhaust retries (or hit a non-retryable error) carry explicit dead-letter metadata you can list, count, inspect, and requeue."
  >}}
  {{< hextra/feature-card
    icon="adjustments"
    title="Rate Limiting"
    subtitle="Throttle execution with per-queue token buckets and an optional fleet-wide limiter, and cap concurrency fleet-wide or per key — without burning retry attempts."
  >}}
  {{< hextra/feature-card
    icon="chart-bar"
    title="Prometheus / OTel Metrics"
    subtitle="Opt into an exporter for queue depth, throughput, wait and run latency, attempts, failures, and retries — scrapeable for alerting and capacity planning."
  >}}
  {{< hextra/feature-card
    icon="lock-closed"
    title="Payload Encryption"
    subtitle="Plug in a codec to transparently encrypt job arguments, results, and checkpoints at rest, with key rotation, at the storage boundary."
  >}}
{{< /hextra/feature-grid >}}

<p class="sdj-section-label">A real-time dashboard, embedded in your app</p>

<div class="sdj-shot-wrap">
  <div class="sdj-window">
    <div class="sdj-window__bar"><span></span><span></span><span></span><span class="sdj-window__title">localhost:8080/jobs/</span></div>
    <img class="hx:block hx:dark:hidden" src="/simple-durable-jobs/images/dashboard/dashboard-light.png" alt="Simple Durable Jobs embedded dashboard — queue stats, throughput chart, and per-queue table" loading="lazy">
    <img class="hx:hidden hx:dark:block" src="/simple-durable-jobs/images/dashboard/dashboard-dark.png" alt="Simple Durable Jobs embedded dashboard (dark) — queue stats, throughput chart, and per-queue table" loading="lazy">
  </div>
  <a class="sdj-quietlink" href="/simple-durable-jobs/docs/live-demo/">Try the interactive demo, running on simulated data →</a>
</div>

<div class="sdj-hero-buttons" style="margin-top:3.5rem">
{{< hextra/hero-button text="Read the docs" link="docs/getting-started/" >}}
{{< hextra/hero-button text="View on GitHub" link="https://github.com/jdziat/simple-durable-jobs" style="background:transparent;border:1px solid var(--sdj-accent);color:var(--sdj-accent)" >}}
</div>
