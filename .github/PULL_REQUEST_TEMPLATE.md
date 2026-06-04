<!--
Title should follow Conventional Commits, e.g. "fix(storage): ..." or
"feat(worker): ...". The release and changelog are derived from commit/PR titles.
-->

## What & why

<!-- What does this change and why? Link any related issue. -->

## How verified

- [ ] `go vet ./...` and `golangci-lint run` pass
- [ ] SQLite suite (`make test`)
- [ ] Postgres + MySQL (`make test-backends`) — required for any `pkg/storage` change
- [ ] Chaos harness (`make chaos-test` / `make chaos-test-mysql`) — for concurrency/durability changes
- [ ] Benchmarks, if performance-relevant (`make bench-postgres`)

## Compatibility

- [ ] No breaking changes to the exported `jobs` API or `core.Storage` (or this is a `feat!:`/`BREAKING CHANGE:` for a major release)
- [ ] Schema changes go through the versioned migration runner and are safe on an existing database
