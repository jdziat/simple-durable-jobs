---
title: "API Reference"
weight: 2
toc: false
---

Complete API documentation for Simple Durable Jobs, split by area. Use the **search box** (top of the page) or pick a section below.

{{< cards cols="3" >}}
  {{< card link="queue/" title="Queue" icon="collection" subtitle="New, Register, Enqueue, EnqueueRemote, EnqueueTx, EnqueueBatch, EnqueueBatchTx, CancelJob, Schedule, NewWorker." >}}
  {{< card link="worker/" title="Worker" icon="cog" subtitle="Start, pause/resume, concurrency, scheduler, stale-lock, retry." >}}
  {{< card link="workflows/" title="Durable Workflows & Fan-Out" icon="refresh" subtitle="Call, checkpoints, FanOut, SubJob, result helpers, strategies." >}}
  {{< card link="typed-api/" title="Typed API" icon="code" subtitle="Define, DeclareUnchecked, DefineVoid, and typed Enqueue, Call, Load handles." >}}
  {{< card link="signals/" title="Workflow Signals" icon="bell" subtitle="Signal, WaitForSignal, WaitForSignalTimeout, CheckSignal, DrainSignals." >}}
  {{< card link="job-options/" title="Job Options" icon="adjustments" subtitle="Priority, Retries, Delay, At, Queue, Unique, Timeout, Determinism." >}}
  {{< card link="types/" title="Types" icon="cube" subtitle="Job, JobStatus, Result, RetryConfig, SubJobFailure, error variables." >}}
  {{< card link="scheduling/" title="Scheduling" icon="clock" subtitle="Every, Daily, Weekly, Cron." >}}
  {{< card link="events/" title="Events & Hooks" icon="bell" subtitle="Event stream, event types, lifecycle hooks." >}}
  {{< card link="error-handling/" title="Error Handling" icon="exclamation-circle" subtitle="NoRetry, RetryAfter." >}}
  {{< card link="observability/" title="Observability" icon="chart-bar" subtitle="OpenTelemetry tracing instrumentation plus the Prometheus / OpenTelemetry metrics exporter." >}}
  {{< card link="storage/" title="Storage & Embedded UI" icon="database" subtitle="GormStorage, pool presets/options, the UI handler." >}}
{{< /cards >}}
