---
title: "Choosing a Job System"
weight: 8
toc: true
---

## How to read this

We built Simple Durable Jobs, so this page naturally evaluates trade-offs through the lens of embedded, SQL-native Go applications. The criteria favor a small operational footprint, ordinary database deployment, and checkpointed workflows that do not require a separate service.

River, Temporal, and Asynq are all good systems when their design center matches your problem. Use this page to pick the system whose constraints you want to live with, not to crown a universal winner.

For performance, see this project's [benchmarks](../benchmarks/). We do not publish cross-vendor performance numbers because those require identical workloads, tuning, hardware, and operating assumptions.

> Competitor descriptions below reflect our understanding of each project's
> public documentation **as of 2026-06-13** and describe features, not quality.
> Third-party products change; verify any comparison against each project's
> current documentation before relying on it. River, Temporal, and Asynq are
> trademarks of their respective owners and are referenced here for
> identification only; this project is not affiliated with or endorsed by them.

## Feature Comparison

| Area | Simple Durable Jobs | River | Temporal | Asynq |
| --- | --- | --- | --- | --- |
| Backends | SQLite, PostgreSQL, MySQL through GORM | PostgreSQL | Temporal Server backed by its supported persistence layer | Redis |
| Embedded vs separate service | Embedded Go library; workers run in your process | Embedded Go library; workers run in your process | Separate server or cluster plus worker processes | Embedded Go library plus Redis |
| Durable workflows/checkpoints | Checkpointed `Call[T]`, fan-out/fan-in, signals, timers | Job queue in OSS; paid Pro tier adds workflow-oriented features such as workflows and sequences (as of 2026-06-13) | Full deterministic workflow replay with durable history | Task processing; no first-party workflow checkpointing model as of 2026-06-13 |
| Signals | Durable job signals for Go workflows | No first-party OSS queue signal primitive as of 2026-06-13 | First-class workflow signals | No first-party workflow signal primitive as of 2026-06-13 |
| Timers | Durable `Sleep`, `SleepUntil`, delay, and schedules | Scheduled and delayed jobs | First-class durable timers in workflow history | Delayed and scheduled tasks |
| Transactional enqueue | `EnqueueTx` and `EnqueueBatchTx` with caller-owned GORM transactions | Transactional insert into PostgreSQL is a core fit | Client API call to Temporal service; not a row inserted in your app DB transaction as of 2026-06-13 | Redis enqueue; not in your SQL transaction |
| Batch ops | Batch enqueue and optional batch dequeue | Batch insertion support | Workflow and activity APIs; not a queue batch API in the same shape | Task groups and task enqueue patterns; no SQL batch enqueue |
| Rate limiting/concurrency caps | Per-worker queue throttles plus storage-backed fleet and per-key caps | Queue and worker tuning focused on PostgreSQL-backed throughput | Worker concurrency, task queue controls, and server-side scaling model | Queue-level concurrency and rate limiting patterns backed by Redis |
| DLQ | Failed/cancelled terminal jobs with dead-letter metadata and requeue | Failed job handling and retry controls | Failure history, retries, and workflow recovery through Temporal APIs | Retry/archive inspection through Redis-backed task states and UI |
| Retention | Worker-driven terminal-job retention | Job retention controls | Namespace/history retention configured in Temporal | Completed/archived task retention controls |
| Metrics/tracing | Hooks, event stream, Prometheus/OpenTelemetry metrics, optional OpenTelemetry tracing package | Operational observability around the PostgreSQL queue | Mature visibility, metrics, tracing, and workflow history | Web UI and operational task inspection |
| Dashboard | Embedded dashboard served from your Go app | River UI | Temporal Web UI | Asynqmon web UI |
| Testing utils | `jobstest` helpers plus SQLite fixtures | Go testing with PostgreSQL-backed queue APIs | Temporal SDK test environments | Go testing around Redis/task handlers |
| Language support | Go | Go | Multi-language SDKs | Go |
| License/pricing | MIT | OSS core with paid Pro tier | OSS server plus Temporal Cloud commercial option | OSS |

## Choose Simple Durable Jobs if

Choose this library if you are a Go team already running PostgreSQL or MySQL and you want durable background jobs, light workflows, durable signals, scheduling, and an embedded dashboard without operating a separate workflow service. It is strongest when your workflows can be expressed as at-least-once Go handlers with explicit checkpoints and idempotent side effects.

You trade away Temporal's replay model, River's Postgres-specialized queue focus, and Asynq's Redis simplicity for one import, your existing database, and a smaller surface area.

## Choose River if

Choose River if you want a PostgreSQL-only Go queue that is deeply optimized around Postgres behavior. River's design gives it a strong Postgres-native performance and operations story, including low-latency wakeups through PostgreSQL mechanisms such as LISTEN/NOTIFY. Its paid Pro tier also adds features such as workflows, sequences, and encryption.

River is a better fit when Postgres is the only backend you need and you want a focused queue maintained around that database's strengths.

## Choose Temporal if

Choose Temporal if workflows are the center of the system, especially when you need deterministic replay, workflow queries, workflow versioning at scale, long histories, multi-language SDKs, and a dedicated workflow control plane. Temporal is the strongest option here for complex orchestration across services and teams.

The trade-off is operational shape: you run or buy a Temporal service, and your application architecture follows Temporal's workflow/activity model instead of a small embedded queue.

## Choose Asynq if

Choose Asynq if your team wants Redis-backed Go task processing with a simple mental model and a web UI. It is a good fit for fast task queues when Redis is already part of your stack and you do not need workflow checkpointing.

Asynq is stronger than this library when Redis speed and operational familiarity matter more than SQL-native transactional enqueue or checkpointed workflows.

## What We Do Not Have

Simple Durable Jobs deliberately does not have:

- Multi-language clients. The public API is Go.
- Push wakeups such as PostgreSQL LISTEN/NOTIFY. Workers poll; tune `WithPollInterval` and `WithDequeueBatchSize` for your load.
- Schema-level multi-tenancy. All tenants share the same jobs tables unless you run separate databases.
- Deterministic replay queries like Temporal workflows provide. `Call[T]` checkpoints results, but the workflow model is not Temporal's event-history replay engine.
- A separate hosted control plane. The dashboard is embedded in your application.

River, Temporal, Asynq, PostgreSQL, MySQL, Redis, and other names are trademarks of their respective owners; used here for identification only. This project is not affiliated with or endorsed by them.
