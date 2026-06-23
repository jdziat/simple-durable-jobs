# Migrating from v3 to v4.0.0

v4.0.0 is a **small major release**. It contains exactly two breaking changes:

1. The module import path gains the required `/v4` suffix (was `/v3`).
2. `CancelJob` is now **terminal cancellation** instead of an alias for
   aggressive pause. A cancelled job is no longer resumable, and a job that is
   `waiting` (parked on a fan-out, signal, durable sleep, or `Call`) is now
   **cancelled**, not paused.

A third, additive change is breaking **only if you hand-roll `core.Storage`**:
the interface gains a `CancelJobTerminal` method (see Step 3).

Everything else — every other type, function, option, and metric — is unchanged.

## The on-disk schema: one forward-only migration (v35)

Unlike v2/v3, v4.0.0 includes **one schema migration, v35**: it widens the
`chk_fan_outs_status` CHECK constraint on Postgres and MySQL to admit a new
terminal `cancelled` fan-out status. It rewrites no rows and removes nothing.

Rolling deploys are safe. The migration only *permits an additional value*; a v3
worker never writes the `cancelled` fan-out status and never depends on its
absence, so a mixed v3/v4 fleet during a rolling deploy is fine. Deploy v4
workers (they run the migration at startup), drain v3, decommission.

> One behavioral caveat during the rollover: a `CancelJob` request is handled
> with **whichever version's semantics** the worker/RPC that serves it has — v3
> workers still pause-alias, v4 workers cancel terminally. If that matters for an
> in-flight operation, finish the fleet cutover before relying on the new
> semantics.

## Coexistence: there is no forced cutover

v3 and v4 are **distinct module paths** (`…/simple-durable-jobs/v3` vs
`…/simple-durable-jobs/v4`). You can depend on both at once and migrate package
by package; the v3 line stays installable forever (it just stops getting new
features).

## Step 1 — update the import path

```sh
go get github.com/jdziat/simple-durable-jobs/v4@v4.0.0
```

Rewrite imports across your code (the `/v3` → `/v4` suffix is the only change):

```sh
# preview, then apply
grep -rl '"github.com/jdziat/simple-durable-jobs/v3' --include='*.go' . \
  | xargs sed -i 's|simple-durable-jobs/v3|simple-durable-jobs/v4|g'
goimports -w .   # or: gofmt -w .
```

Then `go mod tidy`.

## Step 2 — adjust to terminal `CancelJob` semantics

In v3, `CancelJob` was a thin alias for aggressive pause: the job was durably
recorded but **recoverable**, and a job that had self-suspended to `waiting`
ended up *paused*, not cancelled.

In v4, `CancelJob`:

- moves a `pending` / `waiting` / `running` job to a **terminal `cancelled`**
  state that **`ResumeJob` will not revive** (`UnpauseJob` returns
  `ErrJobNotPaused`);
- when the target is a fan-out parent, terminally cancels the **entire
  descendant fan-out subtree** in one transaction;
- emits a new `JobCancelled` event;
- returns `ErrJobNotCancellable` for a job already in a non-cancellable terminal
  state, `ErrJobNotFound` for an unknown job, and `nil` (idempotent) for an
  already-cancelled job.

What to change in your code:

| If you previously… | Do this instead |
|---|---|
| called `CancelJob` and later `ResumeJob`/`UnpauseJob` to recover it | use `PauseJob(ctx, id, jobs.WithPauseMode(jobs.PauseModeAggressive))` for a **recoverable** stop; reserve `CancelJob` for when you mean *terminal* |
| relied on `CancelJob` leaving a `waiting` job recoverable | a `waiting` job is now terminally cancelled — use `Requeue` to replay it from scratch if needed |
| listened only for `JobPaused` to detect cancels | also handle the new `JobCancelled` event |
| treated cancel as non-terminal in your own status checks | `cancelled` is terminal; a cancelled job is not redelivered |

If you do not call `CancelJob`, no change is required.

## Step 3 — implement `CancelJobTerminal` (only if you hand-roll `core.Storage`)

If you embed the built-in `*storage.GormStorage` (the overwhelming majority of
users), **do nothing** — the new method ships with it.

If you have a hand-written `core.Storage`, add the method. It must move a
`pending` / `waiting` / `running` job to `cancelled`, release any held
concurrency slot, and terminally cancel the job's descendant fan-out subtree:

```go
func (s *MyStore) CancelJobTerminal(ctx context.Context, jobID core.UUID) error {
    // ... see storage.GormStorage.CancelJobTerminal for the reference behavior:
    // pending/waiting/running -> cancelled; release slots; cancel the fan-out
    // subtree and reconcile counts to sum == total; preserve locked_by so the
    // owning worker's ownership audit stops the live handler.
}
```

Add `var _ core.Storage = (*MyStore)(nil)` next to your implementation to catch a
missing or mis-typed `CancelJobTerminal` at compile time.

## Verify

```sh
go build ./... && go test ./...
```

If `go build` is green, you're on v4.
