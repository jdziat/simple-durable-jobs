---
title: "Durable Timers"
weight: 14
---

Durable timers pause a workflow without keeping a worker slot busy. Use them
when a handler needs to wait before continuing, but the wait itself must survive
worker restarts and deploys:

- pacing retries against a third-party API
- waiting before a human follow-up reminder
- backing off an agent loop after a rate limit or empty poll

`jobs.Sleep` waits for a duration. `jobs.SleepUntil` waits until a specific
time. Both functions may only be called from inside a registered job handler.

```go
queue.Register("agent.poll", func(ctx context.Context, args PollArgs) error {
	for attempt := 0; attempt < 12; attempt++ {
		result, err := jobs.Call[PollResult](ctx, "poll-provider", PollAttempt{
			AccountID: args.AccountID,
			Attempt:   attempt,
		})
		if err != nil {
			return err
		}
		if !result.Empty {
			_, err = jobs.Call[any](ctx, "handle-result", result)
			return err
		}
		if err := jobs.Sleep(ctx, 30*time.Second); err != nil {
			return err
		}
	}
	return nil
})
```

Each `Sleep` is checkpointed by call index. In a bounded loop, every iteration
reaches a distinct call index, so each empty poll creates its own timer while
already-resolved timers replay instantly.

```go
queue.Register("trial.followup", func(ctx context.Context, args FollowupArgs) error {
	if _, err := jobs.Call[any](ctx, "send-welcome", args.UserID); err != nil {
		return err
	}

	followupAt := args.StartedAt.Add(72 * time.Hour)
	if err := jobs.SleepUntil(ctx, followupAt); err != nil {
		return err
	}

	_, err := jobs.Call[any](ctx, "send-followup", args.UserID)
	return err
})
```

## What Happens While Sleeping

On the first execution, the timer records a checkpoint with the original
deadline, moves the job to `StatusWaiting`, stores the wake deadline in
`run_at`, clears the worker lock, and returns control to the worker. The worker
slot is free for another job.

After the deadline, any worker in the fleet can resume the job. The handler is
replayed from the beginning, reaches the same `Sleep` or `SleepUntil` call, sees
the recorded deadline, and continues without sleeping again.

{{< callout type="warning" >}}
Anything before a sleep that is not checkpointed can run again on wake, because
the handler replays from the start. Put side effects before a timer behind
`jobs.Call` or an explicit phase checkpoint. See
[Execution semantics: at-least-once]({{< relref "/docs/advanced/guarantees#execution-semantics-at-least-once" >}}).
{{< /callout >}}

If a worker crashes while a job is sleeping, no in-process state is needed. The
checkpoint and `run_at` row are enough for another worker to resume the job
after the original deadline. A `jobs.Sleep(ctx, 10*time.Minute)` does not become
another 10-minute wait after restart.

## Wake Granularity

Timers are durable scheduling primitives, not precise clocks. Elapsed sleeps are
detected by the worker's waiting-job polling backstop, currently about every 5
seconds, plus the normal dispatch poll. A sleeping job can therefore resume
several seconds after its deadline.

Use timers for backoff, scheduling gaps, reminders, and workflow pacing. Do not
use them for sub-second timing or latency-sensitive coordination.

## Fast Paths

Non-positive durations return immediately:

```go
if err := jobs.Sleep(ctx, delay); err != nil {
	return err
}
```

Times in the past also return immediately:

```go
if err := jobs.SleepUntil(ctx, deadline); err != nil {
	return err
}
```

Both fast paths still record a resolved checkpoint. That matters for replay: if
a later deploy changes `delay` or `deadline`, an already-executed timer keeps
returning the result it recorded originally.

## Signals Are Separate

User signals cannot complete or consume a durable timer. Signal names beginning
with `_` are reserved for library internals and `Queue.Signal` rejects them with
`jobs.ErrSignalNameReserved`.

## Versioning Timer Changes

Timer calls are part of workflow shape. Changing a sleep duration for new jobs is
usually fine because the original deadline is checkpointed for in-flight runs.
Changing whether a timer exists, moving it relative to `jobs.Call`, fan-out, or
signal waits, or changing the branch that reaches it should use
`jobs.GetVersion`.

```go
version, err := jobs.GetVersion(ctx, "agent-backoff-v2", jobs.DefaultVersion, 1)
if err != nil {
	return err
}

switch version {
case jobs.DefaultVersion:
	return jobs.Sleep(ctx, 10*time.Second)
default:
	return jobs.Sleep(ctx, time.Minute)
}
```

See [Workflow Versioning]({{< relref "workflow-versioning" >}}) for the branch
pattern used to evolve in-flight workflows safely.
