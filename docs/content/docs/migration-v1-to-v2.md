---
title: "Migrating from v1 to v2"
weight: 9
---

v2.0.0 is a deliberately small, mechanical major release. It contains no new
features and no database/schema changes, only the three breaking changes that
Go's compatibility rules and `VERSIONING.md` reserved for the next major:

1. The module import path gains the required `/v2` suffix.
2. `core.Storage.SuspendJob` is renamed to `MarkWaiting`, and
   `SuspendJobWithDeadline` is renamed to `MarkWaitingWithDeadline`. The status
   these set is `waiting`, so the name now matches.
3. The deprecated aliases `IsSuspendError` and `SuspendError` are removed.

Everything else, including every other type, function, option, metric, and the
on-disk schema, is unchanged. Migration is three find/replace steps.

## Coexistence

v1 and v2 are distinct module paths:
`github.com/jdziat/simple-durable-jobs` and
`github.com/jdziat/simple-durable-jobs/v2`. You can depend on both at once and
migrate package by package. The v1 line remains installable.

## Database Schema

v2.0.0 changes only Go API surface. It adds no migration and changes no column
or wire format. A v1.x worker and a v2.0.0 worker can run against the same
database with identical behavior, so a mixed fleet during a rolling deploy is
safe: deploy v2 workers alongside v1, drain v1, then decommission v1.

## Step 1: Update the Import Path

```sh
go get github.com/jdziat/simple-durable-jobs/v2@v2.0.0
```

Rewrite imports across your code. The `/v2` suffix is the only module-path
change:

```sh
# preview, then apply
grep -rl 'github.com/jdziat/simple-durable-jobs' --include='*.go' . \
  | xargs perl -pi -e 's|github\.com/jdziat/simple-durable-jobs(?!/v2)|github.com/jdziat/simple-durable-jobs/v2|g'
goimports -w .   # or: gofmt -w .
```

Then run `go mod tidy`. Most users import the root `jobs` package and a few
subpackages; all move under `/v2`.

## Step 2: Rename Storage Waiting Methods

This matters only if you hand-roll `core.Storage`.

If you embed the built-in `*storage.GormStorage`, no action is required. The
rename is internal to the library.

If you have a hand-written `core.Storage` implementation, rename the methods
with the body unchanged:

```go
// before
func (s *MyStore) SuspendJob(ctx context.Context, jobID, workerID string) error { ... }
func (s *MyStore) SuspendJobWithDeadline(ctx context.Context, jobID, workerID string, d time.Duration) error { ... }

// after
func (s *MyStore) MarkWaiting(ctx context.Context, jobID, workerID string) error { ... }
func (s *MyStore) MarkWaitingWithDeadline(ctx context.Context, jobID, workerID string, d time.Duration) error { ... }
```

Add `var _ core.Storage = (*MyStore)(nil)` next to your implementation to catch
drift in `core.Storage` methods at compile time, including a missed
`MarkWaiting` rename.

That assertion does not guard `MarkWaitingWithDeadline`: it is an optional
signal/durable-timer capability discovered at runtime. A wrong or missing
`MarkWaitingWithDeadline` signature produces no compile error; signal timeout
waits and durable timers instead degrade to `core.ErrStorageNoSignals` at
runtime. Double-check the `d time.Duration` signature by hand, or cover your
signal/timeout-wait path with a test.

## Step 3: Rename Removed Aliases

These were no-op aliases, so this is a pure find/replace:

```sh
grep -rl 'IsSuspendError\|SuspendError' --include='*.go' . \
  | xargs sed -i -e 's/IsSuspendError/IsWaitingError/g' -e 's/\bSuspendError\b/WaitingError/g'
```

## Verify

```sh
go build ./... && go test ./...
```

If `go build` is green, you are on v2.

The root repository copy remains available at
[MIGRATION-v2.md](https://github.com/jdziat/simple-durable-jobs/blob/main/MIGRATION-v2.md).
