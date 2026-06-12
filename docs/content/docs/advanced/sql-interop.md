---
title: "Enqueue from Any Language via SQL"
weight: 67
toc: true
---

The supported raw SQL interop contract is intentionally narrow: an external producer may insert a new pending row into the `jobs` table after the library migrations have run. After that insert, the row belongs to Simple Durable Jobs.

Prefer the Go API when you can. Raw SQL enqueue bypasses validation, enqueue middleware, payload codecs, unique-job helpers, and future facade-level safeguards.

## Stability Caveat

The database schema changes only through the versioned migration runner. Within a major version, migrations are forward-only and safe to apply to an existing database; always run `Migrate()` on deploy. See the repository [versioning policy](https://github.com/jdziat/simple-durable-jobs/blob/main/VERSIONING.md).

This page documents the current INSERT-only enqueue contract for pending jobs. Minor releases may add nullable or defaulted columns. The contract is that a major version will not remove or rename the existing required columns, but raw producers should name columns explicitly and tolerate additional columns.

## Minimal Insert Contract

Insert into the `jobs` table with these columns:

| Column | Required value |
| --- | --- |
| `id` | UUID string, 36 characters |
| `type` | Registered job name, matching the Go worker's `queue.Register(...)` name |
| `args` | JSON bytes matching the handler argument type |
| `queue` | Queue name, usually `default` or an application queue |
| `priority` | Integer; higher runs first |
| `status` | Literal `pending` |
| `attempt` | `0` |
| `max_retries` | Retry count, commonly `3` |
| `timeout` | Duration stored as nanoseconds; use `0` for no timeout |
| `determinism` | `0` for the default `ExplicitCheckpoints` mode |
| `run_at` | `NULL` for immediate execution or a timestamp for delayed execution |
| `created_at` | Current timestamp |
| `updated_at` | Current timestamp |

Leave library-owned execution columns empty or `NULL`: `locked_by`, `locked_until`, `last_heartbeat_at`, `started_at`, `completed_at`, `last_error`, `dead_lettered_at`, `dead_letter_reason`, `previous_status`, `parent_job_id`, `root_job_id`, `fan_out_id`, `fan_out_index`, `result`, and `trace_context`. Also leave `unique_key` `NULL` — uniqueness/dedup is not part of this contract (use the Go API's `Unique` option instead).

Do not insert `active_unique_key` on MySQL. It is a generated column added by migration for active-job uniqueness.

## PostgreSQL Example

```sql
INSERT INTO jobs (
  id,
  type,
  args,
  queue,
  priority,
  status,
  attempt,
  max_retries,
  timeout,
  determinism,
  run_at,
  created_at,
  updated_at
) VALUES (
  $1,
  $2,
  $3::bytea,
  $4,
  $5,
  'pending',
  0,
  $6,
  0,
  0,
  $7,
  NOW(),
  NOW()
);
```

Example parameters:

| Parameter | Example |
| --- | --- |
| `$1` | `018f6f20-91d4-7b5c-9f8d-f55e7a0a0c44` |
| `$2` | `email.deliver` |
| `$3` | UTF-8 bytes for `{"user_id":"user-123","email":"user@example.com"}` |
| `$4` | `mailers` |
| `$5` | `0` |
| `$6` | `3` |
| `$7` | `NULL` or a `timestamptz` |

## MySQL Example

```sql
INSERT INTO jobs (
  id,
  type,
  args,
  queue,
  priority,
  status,
  attempt,
  max_retries,
  timeout,
  determinism,
  run_at,
  created_at,
  updated_at
) VALUES (
  ?,
  ?,
  ?,
  ?,
  ?,
  'pending',
  0,
  ?,
  0,
  0,
  ?,
  NOW(6),
  NOW(6)
);
```

Example parameters:

| Parameter | Example |
| --- | --- |
| `1` | `018f6f20-91d4-7b5c-9f8d-f55e7a0a0c44` |
| `2` | `email.deliver` |
| `3` | UTF-8 bytes for `{"user_id":"user-123","email":"user@example.com"}` |
| `4` | `mailers` |
| `5` | `0` |
| `6` | `3` |
| `7` | `NULL` (run immediately), or a future timestamp to delay the job (e.g. `2026-01-01 12:00:00`) |

## Rules

The JSON in `args` must unmarshal into the Go handler's argument type. If the worker registered `queue.Register("email.deliver", func(ctx context.Context, args EmailArgs) error { ... })`, the JSON object must match `EmailArgs`.

The `id` must be a UUID string. Generate it in the producer and treat it as the durable job ID.

Do not update or delete rows after insertion unless you are using documented Simple Durable Jobs APIs. Workers own status transitions, locks, attempts, results, checkpointing, and dead-letter metadata.

Do not use raw SQL INSERT for unique or deduplicated jobs. `jobs.Unique(key)` uses library and database behavior that raw producers should not duplicate. Use `queue.Enqueue(...)` or `queue.EnqueueTx(...)` from Go for unique enqueue.

Do not use this contract when `GormStorage` is configured with a payload codec such as encryption. Raw JSON will not be encoded with the codec and may fail to decode in workers. Use the Go API in that deployment.

Run `Migrate()` before external producers start inserting. The migration ledger is the source of truth for the table shape.
