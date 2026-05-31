# Bulletproof Campaign — long-running goal

**Created:** 2026-05-31
**Branch:** `fix/bulletproof-audit`
**Goal:** Make simple-durable-jobs bulletproof. Fix all real correctness/durability/fan-in-fan-out
defects from the 2026-05-31 exhaustive 10x audit (a second pass after PR #16 closed ~52 earlier findings).

## Audit method (reproducible)

`Workflow` "bulletproof-audit" (run `wf_924448e5-db5`): 8 review dimensions → adversarial
per-finding verification → synthesis → completeness critic. **59 raw → 52 confirmed, 7 rejected,
0 uncertain.** Cross-checked by 10 independent verification agents; several proved the defect is worse
than first framed.

## Gate per packet (same discipline as PR #16)

1. **Unit/integration** — builds, `go vet`, and a **new regression test that fails before / passes
   after** (RED→GREEN), run with `-race`.
2. **Multi-backend** (`make test-backends` = sqlite+PG+MySQL) for any storage/race/fan-in change —
   **sqlite masks PG/MySQL races** (the P2 lesson from #16).
3. **Chaos harness** (`make chaos-test`) for distributed invariants.
4. **10x-reviewer** pass on the diff.
5. Commit `fix(<area>): … (audit <id>)`.

Delivery: well-bounded packets via the **codex-dispatch plugin** (`codex-dispatch:codex-dispatch`
agent, codex at ~/.local/bin/codex) with explicit Allowed files + Acceptance + Verification; Claude
gates + reviews + commits. Packets sharing `worker.go`/`gorm.go`/`queue.go`/`fanout.go` run **serial**;
disjoint packets (P6 security, P8 ui) may run parallel via worktree isolation.

## Execution order

Serial spine (shared hot files): **P1 → P2 → P4 → P3 → P5 → P9 → P7**.
Parallelizable (disjoint): **P6** (security.go), **P8** (ui/*). P1 introduces
`core.(*FanOut).TerminalStatus()` reused by P2/P3.

## Packets

### P1 — Fan-out terminal-status unification  (serial; HIGHEST fan-in/out value)
Files: `pkg/core/fanout.go`, `pkg/worker/worker.go`, `pkg/queue/queue.go`, `pkg/fanout/options.go` (+tests)
Findings: queue-cancelsubjob-ignores-strategy-and-threshold (rank1), threshold-int-truncation-looser-early-gate (rank8), threshold-dual-denominator-with-cancellations, collectall-all-cancelled-marked-failed, failfast-default-leaves-siblings-running, queue-cancelsubjob-no-handler-context-cancel.
Fix: one pure `core.(*FanOut).TerminalStatus()` / `Decide()` (strategy + threshold + cancelled-aware,
single denominator, ceil-based required successes) used by BOTH `worker.checkFanOutCompletion` and
`queue.CancelSubJob`; replace early threshold gate `int(Total*(1-Threshold))` with doomed-under-best-case
inequality; `queue.CancelSubJob` cancels local running ctx via runningJobs registry (mirror PauseJob).
Accept: `TestFanOut_TerminalStatusParity` (worker vs queue agree for every (Strategy,Threshold,C,F,X,Total));
CollectAll all-cancelled ⇒ Completed; threshold early gate never fails a still-winnable batch.
Gate: unit + multi-backend + chaos (INV-FANOUT-COUNTS).

### P2 — Worker shutdown, partial-failure & no-handler recovery  (serial)
Files: `pkg/worker/worker.go`, `pkg/core/storage.go`, `pkg/storage/gorm.go` (+tests)
Findings: shutdown-dequeue-dispatch-drop-orphans-job, per-queue-counter-leak-on-shutdown-drop,
no-handler-path-skips-fanout-accounting (rank10), [critic] SaveJobResult-then-Complete partial failure
wedges parent, [critic] reaper reclaims job mid-Complete → duplicate run.
Fix: drop-branch best-effort `Release(jobID,workerID)`→pending (detached ctx) + untrack; no-handler
branch calls `handleSubJobCompletion(...,false)`; on Complete→ErrJobNotOwned still run fan-in accounting;
shrink reaper-vs-Complete window. Gate: unit + integration + chaos (INV-NO-WEDGE).

### P3 — Scheduler catch-up & Every semantics  (serial)
Files: `pkg/worker/worker.go`, `pkg/storage/gorm.go`, `pkg/core/scheduled.go`, `pkg/schedule/schedule.go` (+tests)
Findings: scheduler-no-catchup-missed-fire-after-fleet-gap (rank4), everyschedule-next-from-now-skips,
every-truncate-epoch-anchored-drift, hardcoded-100ms-ticker.
Fix: seed lastRun from persisted LastFireAt (+`GetScheduledFireTime`); fire one catch-up via
ClaimScheduledFire when a boundary elapsed during downtime; document Every epoch alignment.
Gate: unit + chaos (INV-SCHED).

### P4 — Storage fan-in counters, locking, migration  (serial)
Files: `pkg/storage/gorm.go`, `pkg/core/job.go` (+tests)
Findings: fanout-counter-no-total-guard-overcount (rank3), getstalledfanoutparents-no-limit,
pausejob-cancels-running-clears-lock-without-owner-check (rank2), dequeue-no-status-guard-pg-mysql,
cancel-paths-completedat-inconsistency, enqueuebatch-mysql-no-partial-index-dedup-gap,
increment-fanout-returns-nonnil-zero-on-error, savecheckpoint-onconflict-updates-created_at,
[critic] Job.Timeout no gorm tag, [critic] AutoMigrate schema drift, [critic] recovery INNER JOIN
wedges parent whose fan_out row was purged.
Fix: guard cancelled_count increment on RowsAffected==1; LIMIT recovery queries; ownership-cooperative
PauseJob; Increment* return nil,err; SaveCheckpoint stop bumping created_at; uniform completed_at;
explicit gorm type/default for Timeout/Determinism; composite dequeue index; document migration limits.
Gate: unit + multi-backend + chaos (INV-FANOUT-COUNTS, INV-UNIQUE).

### P5 — Determinism per-job override + race  (serial)
Files: `pkg/queue/queue.go`, `pkg/queue/options.go` (+tests)
Findings: explicitcheckpoints-cannot-override-besteffort-queue-default (rank6), setdeterminism-data-race.
Fix: `determinismSet bool`; guard q.determinism with mutex. Gate: unit + `-race`.

### P6 — Security redaction  (PARALLEL — disjoint)
Files: `pkg/security/security.go` (+tests)
Findings: redaction-misses-jwt-and-base64url, redos-input-bounded-after-not-before, retryafter-no-future-clamp.
Fix: truncate-with-margin BEFORE redact; provider-prefix patterns + `-_` in run charclass; FP-safe.
Gate: unit.

### P7 — Durable Call: result size, error fidelity, determinism docs  (serial, after P6)
Files: `pkg/call/call.go`, `pkg/security/security.go`, `pkg/core/errors.go`, `pkg/internal/context/context.go`, `pkg/jobctx/jobctx.go`, `pkg/internal/handler/handler.go`, `jobs.go` (+tests)
Findings: checkpoint-result-size-unbounded (rank9), rehydrate-sentinel-substitution, rehydrate-double-wrap-loss,
concurrent-call-index-race (rank5), phase-checkpoint-name-collision, besteffort-stale-checkpoint-masks-mismatch,
empty-args-noretry-valid-empty-types, [critic] JSON float64 precision loss, [critic] error-only handler via Call[T] returns zero,nil.
Fix: MaxResultSize + NoRetry on oversize; rehydrate by recorded kind only (no message-based sentinel swap),
persist inner cause; document Call/FanOut single-goroutine determinism + at-least-once; accept empty args for
nilable kinds; error if Call[T] targets error-only handler. Gate: unit.

### P8 — UI purge safety, redaction-on-read, stats/list correctness  (PARALLEL — disjoint, ui/*)
Files: `ui/service.go`, `pkg/storage/gorm_ui.go`, `ui/stats_gorm.go` (+tests)
Findings: purgequeue-empty-status-deletes-table (rank7), purgequeue-orphans-checkpoints,
listworkflows-running-jobs-ignores-cancelled, getworkflow-parent-walk-no-cycle-detection,
getstatshistory-unbounded, snapshot-queue-depth-non-atomic-upsert-race, searchjobs-negative-or-huge-offset,
raw Args/Result/LastError on open read path, dead `q.events` channel cleanup.
Gate: unit.

### P9 — Fan-out resume ergonomics & API footguns  (serial; low sev)
Files: `pkg/fanout/fanout.go`, `pkg/fanout/sub.go`, `pkg/fanout/options.go`, `jobs.go`, `pause.go`, `pkg/queue/options.go`
Findings: sub-zero-priority-overwrites-fanout-default, loadresult-loadstatus-not-found-not-sentinel,
resume-job-facade-divergence, fanout-completion-backoff-blocks-worker-pool, fanout-withtimeout-silently-unenforced (doc),
register-schedule-post-start-contract-gap (doc), timezone-option-dead.
Gate: unit.

## Rejected by verification (do NOT fix)
double-completion-via-CAS (CAS sufficient); fan_out_id-unindexed (it IS indexed);
resume-JOIN-missing-index (fan_outs composite exists); ReDoS (RE2 linear);
phase-name distinct-collision (triple-keyed).

## Progress

| Packet | Status | Commit | Notes |
|---|---|---|---|
| P1 | ✅ committed | 6c7479b | core.FanOut.TerminalStatus() unifies worker+queue; threshold doomed-gate; CancelSubJob local-cancel. Gate: race units + 10x APPROVE + chaos INV-FANOUT-COUNTS green (20 fan-outs, 0 mismatch) |
| P2 | ✅ committed | 6ce8187 | Release()+shutdown-release + no-handler fan-in accounting. Gate: full race suite green, go vet ./... clean, chaos all-HARD-PASS, multi-backend PG+MySQL green, 4 new tests RED→GREEN, 10x APPROVE. (Interface change broke 3 test mocks fanout/queue/ui — parent added no-op Release; queue mock fix amended in.) |
| P2b | pending | — | DEFERRED to pair with P4: SaveJobResult/Complete partial-failure wedge + reaper-vs-Complete window (need PauseJob cancel-accounting from P4 first) |
| P3 | dispatching | — | scheduler catch-up: seed lastRun from persisted LastFireAt via optional interface (no core.Storage churn); fire one missed boundary after fleet gap |
| P4a | ✅ committed | 02e77a5 | storage fan-in correctness (gorm.go): cancelled_count over-count guard, Increment* nil-on-error, completed_at parity, LIMIT recovery queries. Gate: race units + 5 RED→GREEN tests + multi-backend PG(ok 8.6s)+MySQL(ok 6.2s) + 10x APPROVE. (Optional follow-up nit: ORDER BY on recovery queries for FIFO/starvation-proof.) |
| P4b | ✅ committed | 4d6712f | schema: Job.Timeout/Determinism gorm `not null;default:0`; composite idx_jobs_dequeue (backend-aware, idempotent); SaveCheckpoint stops bumping created_at (adds error_kind/delay). **Bundles the P1 regression fix** (see REG below). Gate: race + 3 RED→GREEN + multi-backend PG/MySQL isolated (-p=1) + chaos all-HARD-PASS + 10x APPROVE-WITH-NITS. |
| REG | ✅ committed | 4d6712f (+ lock-in test, this commit) | **P1 regression fix**: `isSerializationFailure` now retries SQLite BUSY/LOCKED. P1's CancelSubJob local-cancel woke sibling handlers into concurrent writes → SQLITE_BUSY that withSerializationRetry didn't retry → sub-job unaccounted → CollectAll/Threshold parent wedged in `waiting`. Caught by `tests/` integration pkg (NOT in my pkg-only P1/P4a gate — gap now closed: `tests/` added to gate). `TestFanOut_CancelledSubJobsCountTowardCompletion` 12/12 green after fix (was ~2/3 failing). Lock-in: `TestIsSerializationFailure_RetryableErrors` (RED→GREEN verified). |
| P5 | ✅ committed | ea9e7e2 | determinismSet flag (per-job mode incl ExplicitCheckpoints overrides queue default) + q.determinism mutex guard. Gate: race + 2 RED→GREEN (ExplicitOverridesQueueDefault, SetDeterminism_NoRace). |
| P8 | ✅ committed | 3f40afa + 8f254e5 | UI: PurgeQueue rejects empty name/status (+isValidPurgeStatus), PurgeJobs cascades checkpoints in tx, ListWorkflows excludes cancelled from running, SearchJobs/GetWorkflowRoots clamp offset/limit. Gate: full suite 14 ok/0 fail incl ui race + tests/ integration + PG/MySQL. **INCIDENT (resolved): P5/P8 ran as concurrent codex dispatches; P5's iter-3 cleanup reverted P8's ui/service.go to HEAD in the shared tree. First P8 commit 3f40afa shipped gorm_ui+tests but NOT service.go impl → HEAD red; two intervening docs commits compounded it; recovered by hand-reimplementing F1/F3 in 8f254e5 (verified ui green + tests/ integration green). Lessons (in memory): (1) NEVER run concurrent codex in shared tree — serialize or isolation:worktree; (2) re-run named acceptance tests immediately before EVERY commit, abort if red; (3) Edit anchors must come from a fresh Read of current HEAD, not assumptions.** Read-path redaction + dead q.events deferred to P8b. |
| P6 | ✅ committed | d132a01 | provider-key redaction (Stripe/GitHub/Slack/Google/AWS/JWT) + base64url charclass + bounded sanitize (cap before redact). Gate: race + 3 RED→GREEN (ProviderKeys/PreservesDiagnostics/BoundedForHugeInput) + 10x APPROVE-WITH-NITS. |
| P7 | pending | — | after P6 |
| P8 | pending | — | parallel-safe |
| P9 | pending | — | |
