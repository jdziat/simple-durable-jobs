---
title: "Advanced Topics"
weight: 6
---

Detailed documentation for advanced features and production tuning.

- **[Stale Lock Reaper]({{< relref "stale-lock-reaper" >}})** — how the worker reclaims jobs whose owners crashed or disappeared, and the two options (`WithStaleLockInterval`, `WithStaleLockAge`) that tune it.
- **[Pool Configuration]({{< relref "pool-configuration" >}})** — the four pool presets (`DefaultPoolConfig`, `HighConcurrencyPoolConfig`, `LowLatencyPoolConfig`, `ResourceConstrainedPoolConfig`), what they optimize for, and how to hand-roll a custom `PoolConfig` with the option functions.
- **[Storage Retry]({{< relref "storage-retry" >}})** — why the worker has two separate retry knobs (`WithStorageRetry`, `WithDequeueRetry`), what each one protects against, and tuning guidance for slow storage.
