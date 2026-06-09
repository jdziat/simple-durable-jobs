---
title: Documentation
weight: 1
toc: false
---

Welcome to the **Simple Durable Jobs** documentation — a Go library for durable job queues with checkpointed workflows, fan-out/fan-in, crash recovery, and an embedded web dashboard.

{{< callout type="info" >}}
**New here?** Start with [Getting Started]({{< relref "getting-started" >}}) — you'll have a worker processing jobs in about five minutes.
{{< /callout >}}

## Guides

{{< cards >}}
  {{< card link="getting-started/" title="Getting Started" icon="play" subtitle="Install the library, wire up storage, register handlers, enqueue jobs, and run a worker." >}}
  {{< card link="examples/" title="Examples" icon="code" subtitle="Copy-paste-runnable examples: workflows, fan-out/fan-in, scheduling, priorities, error handling, and more." >}}
  {{< card link="embedded-ui/" title="Embedded Web UI" icon="chart-bar" subtitle="Mount the real-time monitoring dashboard into your own HTTP server." >}}
  {{< card link="production-ops/" title="Production Operations" icon="shield-check" subtitle="Run workers in production: migrations, graceful drain, probes, CLI, throughput tuning, metrics, DLQ, and retention." >}}
  {{< card link="live-demo/" title="Live Demo" icon="play" subtitle="Try the dashboard in your browser on simulated data — no backend required." >}}
  {{< card link="benchmarks/" title="Benchmarks" icon="chart-bar" subtitle="Measured enqueue, end-to-end, workflow, and dispatch-latency results with reproduction commands." >}}
  {{< card link="comparison/" title="Choosing a Job System" icon="scale" subtitle="Honest comparison with River, Temporal, and Asynq, including where each is stronger." >}}
  {{< card link="migrating-from-river/" title="Migrating from River" icon="database" subtitle="Map River clients, workers, transactional inserts, unique jobs, and schedules to this library." >}}
  {{< card link="migrating-from-asynq/" title="Migrating from Asynq" icon="switch-horizontal" subtitle="Move Redis-backed task concepts to SQL-backed jobs, workers, delays, and schedules." >}}
{{< /cards >}}

## Reference

{{< cards >}}
  {{< card link="api-reference/" title="API Reference" icon="book-open" subtitle="Complete type and method reference: Queue, Worker, durable Calls, fan-out, options, events, and storage." >}}
  {{< card link="advanced/" title="Advanced Topics" icon="cog" subtitle="Production features: transactional enqueue, dead-letter queue, retention/GC, rate limiting, concurrency caps, metrics, workflow versioning, payload codec, dashboard authorization, testing utilities, and tuning knobs." >}}
  {{< card link="advanced/multi-tenancy/" title="Multi-tenancy" icon="users" subtitle="Tenant-scoped queues, per-key caps, rate limits, metrics labels, and dashboard filtering without schema isolation." >}}
  {{< card link="advanced/sql-interop/" title="SQL Interop" icon="terminal" subtitle="Minimal INSERT-only contract for enqueueing pending jobs from non-Go producers." >}}
{{< /cards >}}
