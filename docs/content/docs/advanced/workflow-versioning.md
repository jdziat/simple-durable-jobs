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

After every in-flight run that could have recorded `DefaultVersion` has
completed or been requeued, a later deploy can raise `minSupported` and remove
the old branch:

```go
version, err := jobs.GetVersion(ctx, "shipping-v2", 1, 1)
```

If an old run still has `DefaultVersion` recorded, this returns
`jobs.ErrUnsupportedWorkflowVersion` instead of silently taking the wrong branch.

## Determinism Modes

`GetVersion` is safe in all `jobs.Determinism` modes. Version markers use named
checkpoints with `CallIndex == -1`, not indexed `Call` checkpoints, so they are
excluded from Strict mode's unconsumed-`Call` guard.

This means adding a new `GetVersion` call, removing one after the migration is
complete, or skipping one through normal branching does not by itself cause a
Strict determinism failure. Strict still validates the actual indexed
`jobs.Call(...)` sequence, which is why the version branch must wrap the changed
`Call`, fan-out, or signal-wait sequence.
