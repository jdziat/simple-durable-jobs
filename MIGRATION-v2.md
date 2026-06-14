# Migrating from v1 to v2.0.0

v2.0.0 is a **deliberately small, mechanical major release**. It contains no new
features and no database/schema changes — only the three breaking changes that
Go's compatibility rules and `VERSIONING.md` reserved for the next major:

1. The module import path gains the required `/v2` suffix.
2. `core.Storage.SuspendJob` is renamed to `MarkWaiting` (and
   `SuspendJobWithDeadline` to `MarkWaitingWithDeadline`) — the status these set
   is `waiting`, so the name now matches.
3. The deprecated aliases `IsSuspendError` and `SuspendError` are removed.

Everything else — every other type, function, option, metric, and the on-disk
schema — is **unchanged**. Migration is three find/replace steps.

## Coexistence: there is no forced cutover

v1 and v2 are **distinct module paths** (`…/simple-durable-jobs` vs
`…/simple-durable-jobs/v3`). You can depend on both at once and migrate package
by package; the v1 line stays installable forever (it just stops getting new
features). Nothing is deleted from v1.

## The on-disk schema is identical — rolling deploys are safe

v2.0.0 changes only Go API surface; it adds **no migration** and changes no
column or wire format. A v1.x worker and a v2.0.0 worker run against the **same
database** with identical behavior, so a mixed fleet during a rolling deploy is
safe: deploy v2 workers alongside v1, drain v1, decommission. No data migration.

## Step 1 — update the import path

```sh
go get github.com/jdziat/simple-durable-jobs/v3@v2.0.0
```

Rewrite imports across your code (the `/v2` suffix is the only change):

```sh
# preview, then apply
grep -rl '"github.com/jdziat/simple-durable-jobs' --include='*.go' . \
  | xargs sed -i 's|"github.com/jdziat/simple-durable-jobs|"github.com/jdziat/simple-durable-jobs/v3|g'
goimports -w .   # or: gofmt -w .
```

Then `go mod tidy`. Most users import the root `jobs` package and a few
subpackages; all move under `/v2`.

## Step 2 — rename `SuspendJob` → `MarkWaiting` (only if you hand-roll `core.Storage`)

If you embed the built-in `*storage.GormStorage` (the overwhelming majority of
users), **do nothing** — the rename is internal to the library.

If you have a hand-written `core.Storage` implementation, rename the method
(body unchanged):

```go
// before
func (s *MyStore) SuspendJob(ctx context.Context, jobID, workerID string) error { ... }
func (s *MyStore) SuspendJobWithDeadline(ctx context.Context, jobID, workerID string, d time.Duration) error { ... }
// after
func (s *MyStore) MarkWaiting(ctx context.Context, jobID, workerID string) error { ... }
func (s *MyStore) MarkWaitingWithDeadline(ctx context.Context, jobID, workerID string, d time.Duration) error { ... }
```

Add `var _ core.Storage = (*MyStore)(nil)` next to your implementation to catch
drift in `core.Storage` methods at compile time, including a missed `MarkWaiting`
rename. That assertion does **not** guard `MarkWaitingWithDeadline`: it is an
optional signal/durable-timer capability discovered at runtime via type
assertion against an unexported `signalStorage` interface in `pkg/signal`. A
wrong or missing `MarkWaitingWithDeadline` signature produces no compile error;
signal timeout waits and durable timers instead degrade to `core.ErrStorageNoSignals`
at runtime. Double-check the `d time.Duration` signature by hand, or cover your
signal/timeout-wait path with a test.

## Step 3 — `IsSuspendError` → `IsWaitingError`, `SuspendError` → `WaitingError`

Pure find/replace (these were no-op aliases):

```sh
grep -rl 'IsSuspendError\|SuspendError' --include='*.go' . \
  | xargs sed -i -e 's/IsSuspendError/IsWaitingError/g' -e 's/\bSuspendError\b/WaitingError/g'
```

## Verify

```sh
go build ./... && go test ./...
```

That's it. If `go build` is green, you're on v2.
