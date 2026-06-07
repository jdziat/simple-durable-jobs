---
title: "Advanced Topics"
weight: 6
toc: false
---

Production tuning for Simple Durable Jobs. These pages cover the knobs you reach for once jobs are flowing and you're hardening for scale and failure.

{{< cards >}}
  {{< card link="guarantees/" title="Guarantees & Production Readiness" icon="shield-check" subtitle="The honest contract: at-least-once execution, the idempotency requirement, backend support tiers, crash-recovery tuning, and how the guarantees are verified." >}}
  {{< card link="stale-lock-reaper/" title="Stale Lock Reaper" icon="refresh" subtitle="How the worker reclaims jobs whose owners crashed or disappeared, and the two options (WithStaleLockInterval, WithStaleLockAge) that tune it." >}}
  {{< card link="pool-configuration/" title="Pool Configuration" icon="adjustments" subtitle="The four connection-pool presets, what each optimizes for, and how to hand-roll a custom PoolConfig with the option functions." >}}
  {{< card link="storage-retry/" title="Storage Retry" icon="shield-check" subtitle="Why the worker has two retry knobs (WithStorageRetry, WithDequeueRetry), what each protects against, and tuning for slow storage." >}}
  {{< card link="retry-backoff/" title="Job Retry Backoff" icon="clock" subtitle="Configure worker-default and per-handler retry delays with BackoffPolicy, ExponentialBackoff, and jitter." >}}
  {{< card link="dead-letter-queue/" title="Dead-Letter Queue" icon="exclamation-circle" subtitle="List, count, inspect, and requeue jobs that exhausted retries, with explicit DLQ metadata and retention caveats." >}}
  {{< card link="batch-enqueue/" title="Batch Enqueue" icon="collection" subtitle="Use Batch entries and Queue.EnqueueBatch to insert many producer jobs with one storage batch operation." >}}
  {{< card link="workflow-versioning/" title="Workflow Versioning" icon="code" subtitle="Use GetVersion markers to evolve Call, fan-out, and signal-wait sequences while in-flight runs keep their recorded branch." >}}
  {{< card link="durable-timers/" title="Durable Timers" icon="clock" subtitle="Pause workflows with Sleep and SleepUntil without holding a worker slot, with checkpointed deadlines that survive restart." >}}
  {{< card link="batch-dequeue/" title="Batch Dequeue" icon="collection" subtitle="Opt in to bounded worker-side batch dequeue to claim multiple due jobs per poll when storage supports it." >}}
  {{< card link="transactional-checkpoints/" title="Transactional Checkpoints" icon="database" subtitle="Commit a phase checkpoint in the same GORM transaction as the business effect it protects." >}}
  {{< card link="transactional-enqueue/" title="Transactional Enqueue" icon="database" subtitle="Persist jobs inside your own GORM transaction so business rows and jobs commit or roll back together." >}}
  {{< card link="payload-codec/" title="Payload Codec" icon="lock-closed" subtitle="Encrypt or transform job arguments, results, checkpoints, and signal payloads at the GORM storage boundary." >}}
  {{< card link="authorization/" title="Dashboard Authorization" icon="shield-check" subtitle="Add optional per-action authorization for mutating dashboard RPCs while keeping identity extraction in your own middleware." >}}
  {{< card link="execution-middleware/" title="Execution Middleware" icon="adjustments" subtitle="Wrap handler execution with interceptors for logging, auth context, panic classification, and error mapping." >}}
  {{< card link="concurrency-caps/" title="Concurrency Caps" icon="adjustments" subtitle="Cap parallelism fleet-wide and per-key (e.g. at most N jobs per customer) with DB-coordinated concurrency slots." >}}
  {{< card link="rate-limiting/" title="Rate Limiting" icon="clock" subtitle="Throttle execution to a requests-per-window ceiling with per-queue token buckets and an optional fleet-wide limiter — without burning retry attempts." >}}
  {{< card link="retention-gc/" title="Retention & GC" icon="trash" subtitle="Automatically prune terminal jobs by per-status age window with a background retention worker, so the jobs table stays bounded." >}}
  {{< card link="metrics/" title="Metrics" icon="chart-bar" subtitle="Export Prometheus / OpenTelemetry metrics — queue depth, throughput, wait/run latency, attempts, failures, retries — via an opt-in instrument." >}}
  {{< card link="cancel-job/" title="Cancel & Terminate" icon="x-circle" subtitle="Cancel a running job fleet-wide via a first-class verb across the facade, Connect-RPC, and the dashboard (cooperative cancellation)." >}}
  {{< card link="testing/" title="Testing Utilities" icon="beaker" subtitle="The jobstest package: isolated SQLite fixtures and rivertest-style enqueue assertions for testing job producers and consumers." >}}
{{< /cards >}}
