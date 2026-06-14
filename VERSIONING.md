# Versioning & Deprecation Policy

Simple Durable Jobs follows [Semantic Versioning](https://semver.org/). Releases
are cut automatically from [Conventional Commit](https://www.conventionalcommits.org/)
messages on `main` by `go-semantic-release`:

- `fix:` → patch release
- `feat:` → minor release
- `feat!:` / `fix!:` or a `BREAKING CHANGE:` footer → major release

## Compatibility promise

Within a major version (currently `v3`):

- The exported API of the root `jobs` package does not break.
- The `core.Storage` interface does not gain or change required methods.
- The database schema only changes through the versioned migration runner, and
  migrations are forward-only and safe to apply to an existing database. Always
  run `Migrate()` on deploy.

New optional storage capabilities are added as **optional interfaces**
(discovered via type assertion), never as new required methods on
`core.Storage`, so custom backends keep compiling.

CI runs an API-compatibility gate against the latest released `v3` module.

## Deprecations

Deprecated API stays and keeps working for the remainder of the current major
version; it carries a `// Deprecated:` doc comment pointing at the replacement.
Removal happens only in the next major version.

Shipped in `v2.0.0` (the deliberate major cut — see `MIGRATION-v2.md`):

- Module import path moved to `github.com/jdziat/simple-durable-jobs/v2`.
- `core.Storage.SuspendJob` → `MarkWaiting` and `SuspendJobWithDeadline` →
  `MarkWaitingWithDeadline` (the status they set is `waiting`).
- Removed the deprecated `IsSuspendError` and `SuspendError` aliases
  (use `IsWaitingError` / `WaitingError`).

No database/schema change and no other API change: a v1.x worker and a v2.0.0
worker interoperate on the same database, so rolling deploys are safe.

## Changelog

Per-release notes are generated from commit messages and published as
[GitHub Releases](https://github.com/jdziat/simple-durable-jobs/releases), which
are the authoritative changelog. `CHANGELOG.md` carries an `Unreleased` summary
of notable in-flight work.
