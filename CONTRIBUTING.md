# Contributing to Simple Durable Jobs

Thanks for your interest in improving Simple Durable Jobs. This project is a
durable job/workflow library, so correctness under concurrency and crashes is
the bar everything is held to.

## Getting set up

```bash
git clone https://github.com/jdziat/simple-durable-jobs
cd simple-durable-jobs
make test            # SQLite suite (fast, no containers)
```

The library targets the Go version pinned in `go.mod`.

## Running the full test matrix

Postgres and MySQL run in Docker via `docker compose`:

```bash
make compose-up          # start Postgres + MySQL
make test-backends       # run the suite against SQLite + Postgres + MySQL
make compose-down
```

Concurrency- and durability-sensitive changes must also pass the chaos harness,
which kills workers at random and asserts the durability invariants:

```bash
make chaos-test          # Postgres
make chaos-test-mysql     # MySQL
```

Benchmarks for the hot storage paths:

```bash
make bench               # SQLite baseline
make bench-postgres      # representative Postgres numbers
```

## Expectations for changes

- **Match the surrounding code.** Comment density and style here lean toward
  explaining *why* a concurrency decision is made — keep that up.
- **Multi-backend.** Anything touching `pkg/storage` must work on SQLite,
  Postgres, and MySQL. SQLite masks races; always run Postgres/MySQL before
  claiming a storage change is done.
- **Schema changes** go through the versioned migration runner
  (`pkg/storage/migrations.go`) with an idempotent body, not ad-hoc DDL.
- **Tests.** New behavior needs tests; concurrency/durability behavior should be
  covered by the chaos harness invariants where applicable.
- **`go vet ./...` and `golangci-lint run` must pass.**

## Pull requests

Use Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, …) — releases and
the changelog are derived from commit messages (see [VERSIONING.md](VERSIONING.md)).
Keep PRs focused and describe how you verified the change (which backends, chaos,
benchmarks).

## Reporting bugs

Open an issue with a minimal reproduction, the backend you hit it on, and the
versions involved. Security issues: see [SECURITY.md](SECURITY.md) — please do
not open a public issue.
