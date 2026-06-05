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
  {{< card link="batch-enqueue/" title="Batch Enqueue" icon="collection" subtitle="Use Batch entries and Queue.EnqueueBatch to insert many producer jobs with one storage batch operation." >}}
  {{< card link="batch-dequeue/" title="Batch Dequeue" icon="collection" subtitle="Opt in to bounded worker-side batch dequeue to claim multiple due jobs per poll when storage supports it." >}}
  {{< card link="transactional-enqueue/" title="Transactional Enqueue" icon="database" subtitle="Persist jobs inside your own GORM transaction so business rows and jobs commit or roll back together." >}}
  {{< card link="payload-codec/" title="Payload Codec" icon="lock-closed" subtitle="Encrypt or transform job arguments, results, checkpoints, and signal payloads at the GORM storage boundary." >}}
{{< /cards >}}
