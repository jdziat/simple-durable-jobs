---
title: "Workflow Versioning"
weight: 13
---

Durable workflows replay from checkpoints. That means handler code changes can
break in-flight jobs when the new code changes the order or names of durable
steps:

- adding, removing, or renaming `jobs.Call(...)`
- changing fan-out/fan-in shape
- adding or removing signal waits

Use `jobs.GetVersion` to record a small version marker before a code change and
branch on the recorded value. The marker is stored in the same checkpoint table
as `Call` and `SavePhaseCheckpoint`, so it survives retry, suspend/resume, and
worker restarts. `q.Requeue` clears checkpoints for a full replay, so a
requeued run records versions again from the current code.

## API

```go
version, err := jobs.GetVersion(ctx, "shipping-v2", jobs.DefaultVersion, 1)
```

- `changeID` identifies one code change. Keep it stable forever for that
  change.
- `minSupported` is the oldest version this deployed code can still run.
- `maxSupported` is the newest version this deployed code should record for new
  runs.
- `jobs.DefaultVersion` is `-1` and represents the code path that existed before
  a version marker was introduced.
- `jobs.ErrUnsupportedWorkflowVersion` is returned when an existing run recorded
  a version outside the supported range of the current code.

On first execution for a `changeID`, `GetVersion` records `maxSupported` and
returns it. On replay, it returns the recorded version even if a later deploy
increases `maxSupported`.

## Runs That Were Already In Flight

A run that was mid-flight when you deployed the marker has no marker to replay —
the code that produced its checkpoints never called `GetVersion`. Those are
exactly the runs the branch exists to protect, so they are **pinned to
`DefaultVersion`** rather than handed `maxSupported`.

The pin is decided on durable evidence, not a guess. When `GetVersion` finds no
marker it looks for an indexed checkpoint — a `Call`, fan-out, signal wait, or
timer — that an **earlier** execution recorded **at or beyond the call position
the handler is standing on**. Only a run that already executed past this point
can carry one. `DefaultVersion` is then recorded like any other version, so every
later replay of that run reads it straight back from the marker.

Two consequences worth knowing:

- **Put the marker before the durable operations it guards.** That is what makes
  the evidence visible. A marker placed *after* the changed `Call` sees nothing
  and records `maxSupported`.
- **A run that passed the marker's position without recording any durable step at
  or after it is indistinguishable from a first execution** and gets
  `maxSupported`. This is harmless: there is no recorded step at those positions
  for the new branch to collide with. It does mean the branch is a guard for
  *durable shape*, not a way to freeze non-durable behaviour for old runs.

`q.Requeue` clears checkpoints, so a requeued run is a first execution again and
records the current `maxSupported`.

Each distinct `changeID` records exactly one checkpoint row, which persists for
the life of the run (cleared only by `q.Requeue` or job deletion). The
`jobs.version:` checkpoint-type prefix is reserved: do not pass a
`SavePhaseCheckpoint` phase name of the form `jobs.version:<changeID>` that would
collide with a `GetVersion` marker in the same job.

## Branch Pattern

Place the marker before the workflow shape changes, then branch all affected
durable operations on the returned version.

```go
func ProcessOrder(ctx context.Context, order Order) error {
	version, err := jobs.GetVersion(ctx, "shipping-v2", jobs.DefaultVersion, 1)
	if err != nil {
		if errors.Is(err, jobs.ErrUnsupportedWorkflowVersion) {
			return jobs.NoRetry(err)
		}
		return err
	}

	switch version {
	case jobs.DefaultVersion:
		_, err = jobs.Call[LegacyQuote](ctx, "quote-shipping", order.ID)
	case 1:
		_, err = jobs.Call[Quote](ctx, "quote-shipping-v2", order.ID)
	}
	if err != nil {
		return err
	}

	_, err = jobs.Call[Receipt](ctx, "send-receipt", order.ID)
	return err
}
```

An in-flight run reaching this handler takes the `jobs.DefaultVersion` arm and
replays its recorded `quote-shipping` checkpoint; a job enqueued after the deploy
takes the `case 1:` arm. Keep the `DefaultVersion` arm as long as any run that
predates the deploy can still be retried, resumed, or is sitting in the
dead-letter queue awaiting requeue.

After every in-flight run that could have recorded `DefaultVersion` has
completed or been requeued, a later deploy can raise `minSupported` and remove
the old branch:

```go
version, err := jobs.GetVersion(ctx, "shipping-v2", 1, 1)
```

If an old run still has `DefaultVersion` recorded — or is pinned to it by the
checkpoints it carries — this returns `jobs.ErrUnsupportedWorkflowVersion`
instead of silently taking the wrong branch. Removing the old arm while such runs
exist therefore fails them loudly on the sentinel; wrap it in `jobs.NoRetry` (as
the pattern above does) if you would rather they dead-letter for triage than
retry.

## Determinism Modes

`GetVersion` is safe in all `jobs.Determinism` modes. Version markers use named
checkpoints with `CallIndex == -1`, not indexed `Call` checkpoints, so they are
excluded from Strict mode's unconsumed-`Call` guard.

This means adding a new `GetVersion` call, removing one after the migration is
complete, or skipping one through normal branching does not by itself cause a
Strict determinism failure. Strict still validates the actual indexed
`jobs.Call(...)` sequence, which is why the version branch must wrap the changed
`Call`, fan-out, or signal-wait sequence.
