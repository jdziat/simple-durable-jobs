# Backlog — below-gate findings

Findings that are REAL and reproduced but did not clear the 4-of-4 blocking gate
(wrong outcome · ordinary path · silent · unbounded). They are recorded here rather
than fixed immediately, so a later round does not re-derive them and so the reason
each was deferred is on file.

Gate scores are the verifying skeptic's, not the finder's.

## Chaos harness (`cmd/chaostest`) — checks that cannot fail

The release-gating harness has 1,125 LOC, 55 invariant sites and no test files. A
mutation audit seeded each invariant's violation and checked whether the predicate
fired. None was individually 4/4, but several cannot fail:

- **`INV-EXACTLY-ONCE` / `duplicate_effect_groups` is structurally dead.**
  `chaos_effects` carries `UNIQUE(job_id, marker)` (main.go:126-155), so the
  duplicate row the check looks for cannot exist. Seeding it is rejected with
  SQLSTATE 23505 and the check still returns PASS. Its two sibling sub-checks
  (`phase_reexec_markers`, `checkpointed_reexec_markers`) DO fire, so the
  invariant as a whole still gates — which is why this is not 4/4.
- **`INV-AT-LEAST-ONCE-WINDOW` hardcodes `pass: true`** (main.go:786-798). INFO
  level, never increments `hardFailed`, and the comment says reporting-only by
  design. Harmless, but it reads as an invariant in the output.
- **`INV-NO-WEDGE` and `INV-READY-NO-STUCK` evaluate over sets `waitForDrain`
  has already guaranteed are empty.** A real wedge takes the DID-NOT-DRAIN branch
  and exits 1 first — loud, so not silent, but the invariants themselves add
  nothing at the point they run.
- **`INV-SCHED` is one-sided** (`got <= maxExpected`): a scheduler that fires ZERO
  times passes. Verified — PASS with `ticks_in_12s_window=0`. Covered elsewhere by
  84 scheduler test funcs, hence not silent.
- **Four invariants lack the population guard two others carry.**
  `INV-FANOUT-COUNTS`, `INV-EXACTLY-ONCE`, `INV-SLOT-NO-LEAK` and
  `INV-RATE-WELLFORMED` have no `expected > 0` check, so a regression that EMPTIES
  their population reads as PASS. `INV-SIGNAL-EXACTLY-ONCE` and
  `INV-TIMER-EXACTLY-ONCE` show the right shape.

Cheapest high-value work on this list: add the `expected > 0` guard to the four,
and either delete `duplicate_effect_groups` or drop the UNIQUE constraint it needs.

## CLI (`cmd/sdj`)

- **The `--queue` scope predicate is correct but untested.** Mutating
  `if filter.Queue != ""` to `if false` in `deadletter.go:119-121` — so
  `dlq requeue --queue emails` would drain EVERY queue — leaves
  `go test ./cmd/sdj/` GREEN. The identical mutation on `Tenant` fails two tests.
  Cause: the fixture's only cross-queue row is also cross-tenant. Fix: add a
  bystander row in a DIFFERENT queue, SAME tenant.
- `dlq list` prints no total and no "more rows" indicator; the default `--limit 50`
  truncates silently while the runbook's next step touches every matching row.
  `CountDeadLettered` exists and is never called. 3/4 — the rows shown are real.
- `dlq list --limit N` above 1000 is silently clamped. 3/4 — documented cap.
- Flag asymmetry: `dlq list` accepts `--type`/`--metadata`, `dlq requeue` does not,
  so the runbook inspects a narrower slice than it then requeues. 3/4 — visible.
- Bulk requeue exits 0 both when nothing matched and when rows were skipped.

## Observability (`pkg/metrics`, `pkg/otel`)

Verified CLEAN against ground truth on 8 workloads: retries counted as retried not
failed; timeout and panic as failed; fan-out children exactly once; spans closed on
fan-out and signal paths; `DequeueReleasedByReason` returns all 7 documented reasons.

- **A no-handler job reaches terminal failure without calling fail hooks**
  (worker.go:2272-2303), so `jobs.failed` misses that class entirely. 3/4 — an
  ERROR is logged and `dead_letter.depth` still rises. Not single-process
  reproducible, since `Enqueue` rejects unregistered types.
- **`CancelJob` on a running job leaks its `job.process` span** — `handleError`
  returns before any hook when `dispositionWriteLanded` is false. Reproduced
  (started=2, ended=1). 3/4 — a WARN fires on the same path.

## SQL plans (`ui`)

- **`GetQueueDepthQueueOnly` flips to a 300k-row scan after `ANALYZE`** — 1043ms vs
  8.0ms for the same query written `GROUP BY status, queue` (130x).
  3/4 **on the SILENCE prong**: 1043ms crosses GORM's 200ms Warn threshold, so the
  default logger names the statement.

  **DO NOT refute a plan finding with "the library never runs ANALYZE".** That was
  this entry's original reasoning and it is wrong. It holds only on SQLite. Postgres
  autovacuum issues `ANALYZE` automatically once roughly `50 + 0.1 x reltuples` rows
  change, and `jobs` is the highest-churn table in the system — every enqueue an
  insert, every transition an update. The analyzed state is the STEADY state there,
  not an edge case. A future plan finding that lands at ~150ms after autoanalyze
  would be silent, ordinary, >=10x and unbounded — 4/4 — and that phrase would
  wrongly kill it.
- `SearchJobs` with no filter (the dashboard default) scans + temp-B-tree sorts,
  549ms at 300k. Already measured and published in `jobSortOrder`'s godoc; closing
  it needs a new index, i.e. a schema change.
- `getQueueDepthStats` 1181ms at 300k, behind a 2s single-flight cache and
  documented as unbounded in table size.
- `GetStatsHistory` 30d materializes every raw row to emit 60 buckets — ~4x slower
  than a SQL-side aggregate, below the bar, plan already correct.

## Retention index (round 46's only gate claim, refuted 2/4)

`retention.go:150`'s candidate SELECT emits `status = ?` while
`idx_jobs_retention_terminal`'s partial WHERE is `status IN (...)`, so SQLite
cannot use the index built for that query (`INDEXED BY` returns "no query
solution"). Adding a redundant `AND status IN ('completed','failed','cancelled')`
restores it and roughly halves the productive pass.

Refuted as 2/4: GORM's default logger (Warn, 200ms threshold — which every
quick-start in this repo installs via a bare `&gorm.Config{}`) prints a WARN naming
`retention.go:161` with the full statement on EVERY pass, so it is not silent; and
end-to-end the productive pass is 1.97x, not the >=10x bar. The 345x figure is
statement-level and the 4880x is an idle pass that deletes nothing.

Worth doing — it is one redundant clause — but it does not warrant a remediation
cycle ahead of anything above.

## A caution on the "not silent, GORM warns" refutation

Two findings above are now refuted on the grounds that GORM's default slow-query
logger (Warn, 200ms) names the statement. That is literally true and operationally
weak: production users routinely install `logger.Silent`, and this repo's own
dequeue path uses a silenced handle. It holds for these two only because they run
on the un-silenced `s.db`.

If that signal is going to carry refutations, it has to be watchable — expose a
slow-query counter, or document that operators must keep GORM's slow log enabled
and routed. Otherwise "not silent" is true on paper and false in practice.

## Needs live Postgres/MySQL (agents are SQLite-only)

- MySQL case-insensitive over-match on bulk requeue scope. `jobs.queue`/`jobs.tenant`
  are `utf8mb4_0900_as_cs` after migration v40, so `--queue Emails` should NOT match
  a stored `emails`. Unverified.
- Whether Postgres shares the retention-index finding — PG gets the same partial
  index; the question is whether `predicate_implied_by` proves
  `status = 'completed'` implies the IN-list. Believed yes, unmeasured.
- Backlog-gauge blanking under positive DB clock skew.
- The chaos mutation matrix on MySQL (dialect-specific branches of
  `checkExactlyOnce`, `checkRateWellFormed`, `checkSlotNoLeak`).

## A HARD invariant sub-check that has never seen LIVE data

**Status: narrowed, not closed. No defect found.**

`checkExactlyOnce`'s third term, `windowCheckpointedRows` (`cmd/chaostest/main.go`),
gates a HARD invariant and therefore gates every release. Both chaos runs on the
v4.9.0 branch reported `window_reexec_markers=0`, so in a *live* run it has only
ever evaluated an empty set.

An earlier version of this entry said the two-sided unit probe was a thing still to
write. That was wrong — `TestWindowCheckpointJoinActuallyMatches` already seeds a
re-exec marker with no checkpoint (asserts PASS), then commits the checkpoint
(asserts FAIL). The entry has been corrected rather than deleted, because writing
down a gap that does not exist is its own defect: it sends the next round to build
something twice.

What WAS missing, and is now fixed: **every one of the harness's 20 invariant tests
ran `dialectSQLite`.** The window-checkpoint join is written three different ways —
`cp.job_id::text` (PG uuid), `BIN_TO_UUID(cp.job_id)` (MySQL binary(16)),
hand-assembled hex (SQLite blob) — and only the SQLite branch was proven. That is
the same defect one layer out: the repair for a join that had been dead on PG and
MySQL since the v3 binary-UUID migration was itself verified only on SQLite, while
`ci.yml`'s release-gating chaos smoke runs on **Postgres**.

`TestWindowCheckpointJoinMatchesOnEveryDialect` now runs the two-sided proof against
live PG and MySQL. **Both pass, and both mutations kill** — replacing the join with a
silently-never-matching variant (`... || 'x'` / `CONCAT(...,'x')`, the historical
defect shape) reddens each dialect. So the code was right; only the evidence was
missing.

Incidental finding from writing it: `checkpoints.job_id` carries a real FK to `jobs`
on PG and MySQL, so the SQLite-only probe's fixture — a checkpoint for a job that was
never enqueued — is impossible on either real backend. A fixture that works on SQLite
can be unbuildable on the backend that gates releases.

**Residual, and it is now small:** the term has still never matched or not-matched
against data produced by a real crash rather than a seeded row. Close it with a
torture run (raise `CHAOS_DURATION` until `window_reexec_markers > 0`); the SIGKILL
has to land inside a ~150ms window in `chaos.pipeline_window`, which is why 25s CI
runs never populate it.

## OPEN: three more unfenced checkpoint writers (round 47b hole #7)

`GormStorage.SaveCheckpoint` is an unfenced upsert — `OnConflict(job_id, call_index,
call_type) DoUpdates(...)` — so any writer can overwrite an existing checkpoint's
`result`, with no ownership predicate. Four call sites reach it through
`jc.SaveCheckpoint`:

| site | audited |
| --- | --- |
| `pkg/signal/signal.go:135` (the timeout verdict) | **yes — was the round-47b HIGH, fixed** |
| `pkg/call/call.go:321` (error checkpoint) | **no** |
| `pkg/call/call.go:335` (result checkpoint) | **no** |
| `pkg/fanout/fanout.go:161` (fan-out checkpoint) | **no** |

The signal one was confirmed harmful because the value written is a **terminal
verdict**: a run that had lost its lease could decide "timed out" for a signal that
was pending and in time, and replay treats that as authoritative.

Whether the other three are harmful is **genuinely open, and should not be assumed
either way**. The argument for benign is that under a double-run both executions
compute the same value, so an overwrite is a no-op. The argument for harmful is that
`Call` exists precisely to make a NONDETERMINISTIC operation replay-safe — that is
its whole contract — so two runs need not agree, and a later replay reading the
loser's result is the v4.6.0 nested-`Call` corruption class, which completed jobs
carrying another call's result.

Note the fix for the signal site does NOT cover these: it fenced the ownership gate
in `consumeSignalTx`/`drainSignalsTx`, not `SaveCheckpoint` itself. A fence on
`SaveCheckpoint` would cover all four, but it is a wider change — the fan-out site
in particular writes before the parent is marked waiting, so the ownership predicate
has to be checked against the right state.

This is the largest unexamined surface in the codebase right now. It wants its own
round with an executed reproduction attempt per site, not a reasoned verdict.

## Deferred from round 47b, with the reasoning already done

- **Rate-limit window GC across a DST fall-back.** `deleteExpiredRateLimitWindows`
  uses a `windowStart - 2*window` cutoff; across a fall-back the current window's own
  row can sort below it when `window < 30min`, deleting the live counter and
  resetting the cap. Blast radius is a `2*window` sliver twice a year. Hypothesis
  only — **not reproduced**, and explicitly not filed as a finding.
- **`batchCompleteChunkSize = 400` sits at 80% of a measured SQLite compound-SELECT
  ceiling of 500**, and no comment says so. Fails loudly rather than silently if
  raised, so this is a comment, not a fix.
- **Does the `signals.created_at` face bug exist on MySQL?** `DATETIME(3)` is
  zone-less and go-sql-driver converts via the DSN `loc` param (default UTC).
  Reasoned unaffected; not verified.
- **PG/MySQL execution of `batch_complete.go`'s dialect branches.** Rounds 47 and 47b
  were SQLite-only by rule, so `batchCompleteFlipPostgres` and `batchCompleteFlipMySQL`
  were read but never run.
