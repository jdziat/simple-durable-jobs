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
dead, err := q.ListDeadLettered(ctx,
	jobs.DeadLetterQueue("emails"),
	jobs.DeadLetterType("send-email"),
	jobs.DeadLetterLimit(50),
)
if err != nil {
	return err
}

count, err := q.CountDeadLettered(ctx, jobs.DeadLetterQueue("emails"))
if err != nil {
	return err
}
_ = count
```

Results are ordered by `dead_lettered_at DESC`. Use `DeadLetterOffset` with
`DeadLetterLimit` for pagination.

On SQLite that column is stored as text carrying a UTC offset, so the sort is a
newest-first ordering of instants only when every row shares one clock face. Rows
dead-lettered by this version do; rows written by releases before this one carry
the offset of whichever process wrote them, so a mixed-zone fleet — or one worker
across a daylight-saving fall-back — can leave those legacy rows out of order
relative to each other. They are still returned, and the `DeadLetteredSince` /
`DeadLetteredUntil` window selects them by instant regardless. They age out with
retention. Postgres and MySQL store a real instant and are unaffected.

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
ok, err := q.Requeue(ctx, jobID)
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

Retention GC is on by default, and dead-letter metadata does not protect a
failed row from it: terminal failed and cancelled rows are pruned by
`completed_at`, 90 days after they reached their terminal status on a worker
started with no retention options. Triage that has not happened by then has no
row left to triage.

Widen `RetentionFailedAfter` to however long operators actually need to inspect,
diagnose, export, or requeue dead-lettered jobs. `WithRetention` **replaces** the
stock windows rather than merging with them, so restate the windows you still
want — a window omitted from the call is `0`, which keeps that status forever:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(30*24*time.Hour),
		jobs.RetentionFailedAfter(180*24*time.Hour),
		jobs.RetentionConsumedSignalsAfter(7*24*time.Hour),
	),
)
```

See [Retention GC]({{< relref "/docs/advanced/retention-gc" >}}) for the stock
windows and for how to turn retention off entirely.

Deletes are permanent. When retention removes a dead-lettered job, it also
removes the row operators would use for DLQ triage and replay.
