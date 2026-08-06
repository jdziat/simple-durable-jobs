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
  8.0ms for the same query written `GROUP BY status, queue` (130x). 3/4: the
  library never runs `ANALYZE`, so the shipped state plans correctly at 7.4ms.
  One-word fix (swap the GROUP BY column order) removes the trap for free.
  **Postgres autovacuum ANALYZEs automatically, so that escape hatch may not exist
  there — unverified.**
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
