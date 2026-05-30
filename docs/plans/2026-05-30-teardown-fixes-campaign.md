# Teardown-Fixes Campaign — long-running goal

**Created:** 2026-05-30
**Branch:** `fix/teardown-findings` (off merged `e6adf8b`)
**Source of findings:** adversarial 10x teardown review (72 verified findings → ~52 distinct defects), Tier-0/1 headliners independently re-confirmed against source on 2026-05-30.

## Goal

Fix **all** teardown findings. Every fix packet is delegated to the **OpenAI Codex CLI via codex-dispatch**, then gated on three things before it counts as done:

1. **Unit/integration tests** — package builds, `go vet`, and a *new regression test* that fails before the fix and passes after.
2. **Distributed chaos/load harness** (docker-compose) — the relevant cross-worker invariant flips from RED → GREEN.
3. **10x review** — a `10x-reviewer` pass on the diff (scope, correctness, no regressions).

Progress survives context compaction via: this doc's **Progress** table, the TaskList, and the `teardown-fixes-campaign` memory.

## Execution model

- **Serial by default** on this branch (many packets touch `gorm.go` / `worker.go` / `fanout.go`; parallel codex writers on one tree clobber). Disjoint-file packets MAY run in parallel via `isolation: worktree`.
- Each packet: author acceptance criteria here → dispatch to `codex-dispatch:codex-dispatch` (it returns structured JSON, does not commit) → run unit tests + harness → `10x-reviewer` → I commit with a `fix(<area>): … (teardown <id>)` message.
- Codex must not invent scope: every dispatch carries explicit **Allowed files**, **Acceptance criteria**, **Verification commands**.

## Verification gate: distributed chaos/load harness (Packet H0)

A docker-compose harness that runs **N worker replicas against a shared Postgres**, drives a representative workload, **SIGKILLs workers mid-flight**, and asserts cross-worker invariants via a side-effect ledger:

- **INV-EXACTLY-ONCE** — every executed unit/sub-job/workflow-phase appears exactly once in `chaos_effects(job_id, marker)` (UNIQUE). Re-execution on replay ⇒ violation. *(detects 0.1, double-processing)*
- **INV-NO-WEDGE** — after drain, zero jobs in `waiting`/`running`. *(detects 0.3 wedge)*
- **INV-FANOUT-COUNTS** — per fan_out: `completed+failed+cancelled == total_count`. *(detects 2.2)*
- **INV-UNIQUE** — exactly one row for a hammered `EnqueueUnique` key. *(detects 0.4)*
- **INV-SCHED** — scheduled job fired within expected bounds (not immediate-on-boot, not N×replicas/period). *(detects 0.5)*

Harness MUST be RED on current code (reproduce ≥2 defect classes) and GREEN-able by the fixes. Non-distributed findings (OTel leak, API footguns, UI N+1) are gated by unit tests only.

## Finding → Packet map

| Packet | Findings | Files (allowed) | Acceptance (regression must flip) |
|---|---|---|---|
| **H0** harness | — | `cmd/chaostest/**`, `docker-compose.chaos.yml`, `scripts/chaos-test.sh`, `Makefile` | builds; `make chaos-test` runs; RED on current code reproducing ≥2 of {0.1,0.3,0.4,0.5} |
| **P1** replay keying | 0.1, 0.2, 3.3, 5.3 | `pkg/internal/context/context.go`, `pkg/call/call.go`, `pkg/fanout/fanout.go`, `pkg/core/job.go`, `pkg/core/errors.go` | phase checkpoints keyed by `(CallIndex,CallType)`; `Call` asserts `CallType`; replayed error keeps `errors.Is`/`NoRetry` identity; `Call` args size-checked. INV-EXACTLY-ONCE green for pipeline phases. |
| **P2** dedup constraint | 0.4 | `pkg/core/job.go`, `pkg/storage/gorm.go`, `pkg/storage/pool.go` | partial-unique index on `unique_key`; `ON CONFLICT`/duplicate→`ErrDuplicateJob`; sqlite `_txlock=immediate`+`busy_timeout`. INV-UNIQUE green. |
| **P3** fan-out atomicity | 0.3, 5.1, 5.2 | `pkg/fanout/fanout.go`, `pkg/core/storage.go`, `pkg/storage/gorm.go` | checkpoint persisted after `EnqueueBatch`; re-enqueue on empty-resume; `DeleteCheckpoint`; sub-job args size + `ValidateJobTypeName`. INV-NO-WEDGE green under chaos. |
| **P4** scheduler | 0.5, 1.1, 5.5, 5.6 | `pkg/worker/worker.go`, `pkg/queue/queue.go`, `pkg/schedule/schedule.go`, `jobs.go` | seed `lastRun=now`; persistent atomic claim `(name,bucket)`; forward all options + args; UTC consistency; `Cron` returns error + `MustCron`. INV-SCHED green. |
| **P5** otel ctx | 0.6 | `pkg/worker/worker.go`, `pkg/otel/otel.go` | terminal hooks get `jobCtx`; test asserts span has end time. |
| **P6** backoff/poison | 1.2, 5.4 | `pkg/worker/worker.go`, `pkg/internal/handler/handler.go` | `time.Second << min(attempt,30)` floored/capped; non-future `run_at` rejected; empty-args-typed-handler → `NoRetryError`. |
| **P7** worker lifecycle | 1.7, 1.8 | `pkg/worker/worker.go`, `pkg/worker/options.go` | aux goroutines joined to `wg`; `Start` return ⇒ full teardown; configurable graceful drain on ctx cancel. |
| **P8a** storage guards | 2.1, 2.2, 2.3 | `pkg/storage/gorm.go` | status guards on `Heartbeat`/`SaveJobResult`/`CancelSubJobs`; unify cleared-owner sentinel; `Dequeue` targeted guarded `Updates` on PG/MySQL. INV-FANOUT-COUNTS green. |
| **P8b** storage cost | 2.4, 2.5 | `pkg/storage/gorm.go`, `pkg/core/job.go` | `GetWaitingJobsToResume` `LIMIT`+composite index; shared serialization-retry wrapper on all deadlock-prone txns. |
| **P9** fanout fidelity | 3.1, 3.2 | `pkg/fanout/fanout.go` | `CollectResults` sets `Index`, sentinels cancelled/missing; terminal fan-out failure wrapped `NoRetryError`. |
| **P10** ui security | 1.3, 1.5, 1.6 | `pkg/storage/gorm_ui.go`, `ui/service.go`, `ui/handler.go`, `pkg/security/security.go` | LIKE escaped+capped+`ESCAPE`; mutating RPCs fail-closed w/o auth; bulk caps; real error redaction. |
| **P11** ui scalability | 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7 | `ui/service.go`, `pkg/storage/gorm_ui.go`, `ui/stats_collector.go`, `pkg/queue/queue.go` | batch BFS/list queries; depth via aggregate; honest totals; resume routes through queue+emit; events drop-counter+doc; collector logs errors. |
| **P12** stats atomicity | 1.4 | `ui/stats_gorm.go`, `ui/stats.go`, `ui/handler.go` | unique index + atomic upsert; one collector per DB. |
| **P13** api footguns | FanOutResult, Sub() zero-value, WithLockDuration, RegisterE, LoadResult | `jobs.go`, `pkg/fanout/*`, `pkg/worker/*`, `pkg/queue/*` | dead types removed or wired; documented; `RegisterE` added. |
| **P14** determinism/timeout | Determinism, SubJob.Timeout, jobs.Timeout | `jobs.go`, `pkg/queue/*`, `pkg/worker/*`, `pkg/core/*` | implement minimally or remove + document; `Strict` actually guards replay (pairs with 0.2). |

## Progress

| Packet | Status | Commit | Notes |
|---|---|---|---|
| H0 | ✅ committed | `469c6d0` | harness verified RED: INV-EXACTLY-ONCE phase_reexec=50, INV-SCHED immediate_burst |
| P1 | ✅ committed | `b827971` | 0.1/0.2/3.3/5.3 — INV-EXACTLY-ONCE flipped to PASS (phase_reexec=0) |
| P2 | ✅ committed | `73e6d09` | 0.4 + partial 2.5 — partial active-unique index (PG+sqlite) + MySQL deadlock-retry; verified on all 3 engines |
| P3 | ✅ committed | `28a0291` | 0.3 mode-b + 5.1/5.2 — resume-path idempotent re-enqueue + sub-job validation |
| P3b | ✅ committed | `b8769ca` | 0.3 mode-a — GetStalledFanOutParents recovery; chaos harness ALL HARD invariants green (PG+MySQL verified) |
| P4 | ✅ committed | `a086779` | 0.5 boot-storm/1.1/5.5/5.6 — seed lastRun, options+args, UTC, Cron error+MustCron |
| P4b | ✅ committed | `e6cc15a` | 0.5 exactly-once — atomic ClaimScheduledFire CAS; chaos INV-SCHED 6→2; PG+MySQL verified |
| P5 | 🔄 in progress | — | OTel terminal-hook ctx (0.6) |
| P6–P14 | pending | — | |

**Milestone (after P4b):** chaos harness FULLY GREEN — all 4 HARD invariants + INV-SCHED PASS under SIGKILL chaos on Postgres. All harness-reproducible distributed findings (0.1/0.3/0.4/0.5 + 2.2-so-far) closed. Remaining packets (P5–P14) are unit-test-gated (OTel, backoff, lifecycle, storage guards, UI, API) — not distributed-correctness invariants.
Findings closed: 0.1 0.2 0.3 0.4 0.5 1.1 3.3 5.1 5.2 5.3 5.5 5.6 + partial 2.5.

**Milestone (after P3b):** chaos harness reports NO HARD failures — INV-EXACTLY-ONCE / NO-WEDGE / FANOUT-COUNTS / UNIQUE all PASS under SIGKILL chaos. Only INV-SCHED (INFO, finding 0.5) still RED → P4. Distributed-correctness Tier-0 (0.1/0.3/0.4) closed.

**P2 lesson (multi-backend gate):** codex (sqlite-only) reported pass twice; the real fix needed a Postgres-only partial index AND a MySQL-only `withSerializationRetry` wrap (the 1213 deadlock = finding 2.5). The race is PG-only, the deadlock MySQL-only, sqlite shows neither — only the docker-compose multi-backend run caught both. First P2 attempt (permanent unique index) was correctly rejected by the dispatch loop for breaking the frees-after-completion contract.

Status legend: pending → dispatched → review → verified → committed.

**Gating note (important for resumption):** the chaos harness exits RED until ALL
distributed packets land, so the per-packet gate is the *specific* invariant, not the
script exit code. Map: P1→INV-EXACTLY-ONCE ✅ · P2→INV-UNIQUE · P3→INV-NO-WEDGE ·
P8a→INV-FANOUT-COUNTS · P4→INV-SCHED. P3 (0.3) and P8a (2.2) already observed RED under
chaos (INV-NO-WEDGE waiting=1, INV-FANOUT-COUNTS mismatched=1) — expected until those land.

**Carry-over for the testing packet:** `TestIntegration_ConcurrentWorkers` is flaky
(shutdown race; 5/5 pass isolated, fails under full-suite contention) — the review's
"test theater". Fix it when the testing-quality work happens.
