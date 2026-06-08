---
title: "Dead-Letter Queue"
weight: 11
---

## Failed jobs as the DLQ

Simple Durable Jobs has historically treated exhausted failed jobs as the
dead-letter queue. Today the DLQ is every automatic terminal failure: jobs that
exhaust retries and jobs whose handler returns `NoRetry`; operator cancellation
is excluded. There is still no separate archive table: poison jobs remain in the
`jobs` table with `status = failed`, and `Requeue` is the replay path.

Newer schemas add explicit DLQ metadata to those rows:

- `dead_lettered_at`: when the job reached an automatic terminal failure
- `dead_letter_reason`: a concise summary, such as `max retries exhausted: ...`

Rows that existed before this metadata was added are not backfilled. They remain
queryable by failed status, but they do not appear in explicit DLQ queries unless
they fail again and reach an automatic terminal failure.

## List and count

Use the named triage helpers to inspect poison jobs without scanning all failed
rows:

```go
dead, err := jobs.ListDeadLettered(ctx, q,
	jobs.DeadLetterQueue("emails"),
	jobs.DeadLetterType("send-email"),
	jobs.DeadLetterLimit(50),
)
if err != nil {
	return err
}

count, err := jobs.CountDeadLettered(ctx, q, jobs.DeadLetterQueue("emails"))
if err != nil {
	return err
}
_ = count
```

Results are ordered by `dead_lettered_at DESC`. Use `DeadLetterOffset` with
`DeadLetterLimit` for pagination.

## Triage and replay

Dead-lettered jobs keep their original job row, arguments, last error, and DLQ
metadata for inspection. After fixing code or an external dependency, replay a
job with `Requeue`:

With a payload codec configured, `last_error` and the error suffix of
`dead_letter_reason` are encrypted at rest, just like job arguments and results.
The fixed `dead_letter_reason` label (such as `max retries exhausted: `) stays
plaintext so the SQL retries-exhausted classification keeps working; only the
appended error text is encrypted. Both values are decoded transparently on
readback through storage and in the dashboard, so triage helpers and the embedded
UI show readable text. Direct SQL against the `last_error` and `dead_letter_reason`
columns sees the ciphertext form (base64 behind an `sdjenc:` tag). Under the
default identity codec the error text is stored verbatim. See
[Payload Codec]({{< relref "/docs/advanced/payload-codec" >}}) for details.

```go
ok, err := jobs.Requeue(ctx, q, jobID)
if err != nil {
	return err
}
if !ok {
	// job was missing or no longer failed/cancelled
}
```

Requeue clears `dead_lettered_at` and `dead_letter_reason`, resets execution
state, and deletes checkpoints so the workflow starts from the beginning.
Handlers must still be idempotent because execution is at least once.

## Retention interaction

Retention GC still prunes terminal failed and cancelled rows by `completed_at`.
DLQ metadata does not protect a failed row from retention.

Configure `RetentionFailedAfter` with enough time for operators to inspect,
diagnose, export, or requeue dead-lettered jobs:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionFailedAfter(30*24*time.Hour),
	),
)
```

Deletes are permanent. When retention removes a dead-lettered job, it also
removes the row operators would use for DLQ triage and replay.
