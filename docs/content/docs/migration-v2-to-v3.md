---
title: "Migrating from v2 to v3"
weight: 9
---

v3.0.0 is a breaking release for the Go module path, a small set of Go APIs,
and the SQL schema representation used by PostgreSQL and MySQL. Plan the schema
step deliberately before rolling v3 workers onto an existing v2 database.

## Module Path

v2 and v3 are distinct module paths:
`github.com/jdziat/simple-durable-jobs/v2` and
`github.com/jdziat/simple-durable-jobs/v3`. You can depend on both at once while
you migrate package by package.

```sh
go get github.com/jdziat/simple-durable-jobs/v3@v3.0.0
```

Rewrite imports across your code:

```sh
grep -rl 'github.com/jdziat/simple-durable-jobs/v2' --include='*.go' . \
  | xargs perl -pi -e 's|github\.com/jdziat/simple-durable-jobs/v2|github.com/jdziat/simple-durable-jobs/v3|g'
goimports -w .   # or: gofmt -w .
go mod tidy
```

## Go API Changes

Four operational helpers moved onto `*Queue`:

| v2 shape | v3 shape |
| --- | --- |
| `Signal(ctx, q, id, name, payload)` | `q.Signal(ctx, id, name, payload)` |
| `Requeue(ctx, q, id)` | `q.Requeue(ctx, id)` |
| `ListDeadLettered(ctx, q, opts...)` | `q.ListDeadLettered(ctx, opts...)` |
| `CountDeadLettered(ctx, q, opts...)` | `q.CountDeadLettered(ctx, opts...)` |

`jobs.LoadResult[T](ctx, q, id)` is unchanged. It remains a package-level
function because Go does not allow generic methods.

The typed producer-only declaration helper is now `DeclareUnchecked`:

```go
sendEmail := typed.DeclareUnchecked[SendEmailArgs, SendEmailResult](q, "send-email")
```

Use it only when no local handler is registered and the producer must enqueue
for workers running elsewhere. `typed.Define` and `typed.DefineE` now validate
that result type `R` matches the handler's return type.

`q.Schedule` now returns an error. It rejects names without a registered handler
and duplicate scheduled names:

```go
q.Register("cleanup", func(ctx context.Context, _ struct{}) error {
	return cleanup(ctx)
})

if err := q.Schedule("cleanup", nil, jobs.Every(5*time.Minute)); err != nil {
	return err
}
```

`q.EnqueueRemote` now rejects malformed job names instead of accepting them for
remote producers.

## Schema Conversion

{{< callout type="warning" >}}
Running v3 `Migrate()` against an existing v2 database performs a one-way,
in-place schema conversion. Take a backup first and run the upgrade during a
maintenance window.
{{< /callout >}}

On PostgreSQL and MySQL, v3 converts all UUID columns from `varchar(36)` to the
database-native representation: native `uuid` on PostgreSQL and `binary(16)` on
MySQL. It also converts nine integer columns from `bigint` to `integer`.

The conversion is idempotent, crash-resumable, and preserves all data. On a
large table, it is still a hot-table rewrite and holds the fleet migration lock
for its duration. PostgreSQL uses `AccessExclusive` locking for the table
rewrite. MySQL may use copy or in-place DDL depending on the engine and version.

JSON payloads, wire/API formats, and dashboard rendering are unchanged: IDs
still appear as canonical UUID strings. SQLite, used as the dev/test engine,
rewrites the schema in place.

After the migration completes, run your normal application build and test suite
against the v3 module path before deploying workers broadly.
