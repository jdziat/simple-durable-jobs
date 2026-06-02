---
title: "Advanced Topics"
weight: 6
toc: false
---

Production tuning for Simple Durable Jobs. These pages cover the knobs you reach for once jobs are flowing and you're hardening for scale and failure.

{{< cards >}}
  {{< card link="stale-lock-reaper/" title="Stale Lock Reaper" icon="refresh" subtitle="How the worker reclaims jobs whose owners crashed or disappeared, and the two options (WithStaleLockInterval, WithStaleLockAge) that tune it." >}}
  {{< card link="pool-configuration/" title="Pool Configuration" icon="adjustments" subtitle="The four connection-pool presets, what each optimizes for, and how to hand-roll a custom PoolConfig with the option functions." >}}
  {{< card link="storage-retry/" title="Storage Retry" icon="shield-check" subtitle="Why the worker has two retry knobs (WithStorageRetry, WithDequeueRetry), what each protects against, and tuning for slow storage." >}}
{{< /cards >}}
