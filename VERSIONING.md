# Versioning & Deprecation Policy

Simple Durable Jobs follows [Semantic Versioning](https://semver.org/). Releases
are cut automatically from [Conventional Commit](https://www.conventionalcommits.org/)
messages on `main` by `go-semantic-release`:

- `fix:` → patch release
- `feat:` → minor release
- `feat!:` / `fix!:` or a `BREAKING CHANGE:` footer → major release

## Compatibility promise

Within a major version (`v1`):

- The exported API of the root `jobs` package does not break.
- The `core.Storage` interface does not gain or change required methods.
- The database schema only changes through the versioned migration runner, and
  migrations are forward-only and safe to apply to an existing database. Always
  run `Migrate()` on deploy.

New optional storage capabilities are added as **optional interfaces**
(discovered via type assertion), never as new required methods on
`core.Storage`, so custom backends keep compiling.

## Deprecations

Deprecated API stays and keeps working for the remainder of the current major
version; it carries a `// Deprecated:` doc comment pointing at the replacement.
Removal happens only in the next major version.

Currently deprecated / planned changes for the next major (`v2`):

- `IsSuspendError` → use `IsWaitingError`.
- The `core.Storage.SuspendJob` method will be renamed to `MarkWaiting`
  (the status it sets is `waiting`). It remains `SuspendJob` through all of
  `v1` to avoid breaking external `Storage` implementations.

When `v2` happens it will: perform these renames, drop the deprecated aliases,
move to the `/v2` module import path, and ship with a migration guide.

## Changelog

Per-release notes are generated from commit messages and published as
[GitHub Releases](https://github.com/jdziat/simple-durable-jobs/releases), which
are the authoritative changelog. `CHANGELOG.md` carries an `Unreleased` summary
of notable in-flight work.
