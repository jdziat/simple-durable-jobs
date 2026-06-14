# Durable Agent

This example demonstrates a durable AI-agent loop with mocked LLM calls, durable sleeps, and human approval before action.

## What It Does

- Sets up a SQLite-backed queue with no external API calls or API keys
- Registers a mocked `mock-llm` handler that returns deterministic research results
- Runs an `agent-workflow` loop that checkpoints each LLM call with `jobs.Call[T]()`
- Uses `jobs.Sleep()` between research iterations so pacing survives restarts
- Waits for a durable human `approval` signal before taking the final action
- Calls `act-on-plan` only after approval, with the final plan assembled from checkpointed observations

## Prerequisites

- Go 1.25+
- No external services needed (uses SQLite)

## Running

```bash
go run ./examples/agent
```

The example runs for about 8 seconds while the agent sleeps between iterations and waits for approval.

## Expected Output

Framework INFO logs are written to stderr and elided here; the stdout
narrative below matches a real run (job IDs vary).

```
Enqueued agent workflow d53a7124
Starting worker...
[agent] Starting task CASE-42
  [llm] CASE-42 iteration 0
[agent] Iteration 0 result: Need product policy context.
[agent] Pacing the research loop with jobs.Sleep...
[agent] Starting task CASE-42
[agent] Iteration 0 result: Need product policy context.
[agent] Pacing the research loop with jobs.Sleep...
  [llm] CASE-42 iteration 1
[agent] Iteration 1 result: Need customer history context.
[agent] Pacing the research loop with jobs.Sleep...
Main sending human approval signal
[agent] Starting task CASE-42
[agent] Iteration 0 result: Need product policy context.
[agent] Pacing the research loop with jobs.Sleep...
[agent] Iteration 1 result: Need customer history context.
[agent] Pacing the research loop with jobs.Sleep...
  [llm] CASE-42 iteration 2
[agent] Iteration 2 result: Draft response with refund eligibility and next action.
[agent] Waiting for human approval before acting...
  [act] Sending final answer for CASE-42: Need product policy context. | Need customer history context. | Draft response with refund eligibility and next action.
Agent example complete: durable agent reached the final action
```

The repeated `Starting task` and `Iteration 0/1 result` lines are replays. The missing repeated `[llm]` lines are the point: completed LLM calls come from checkpoints instead of being executed again.

## Key Concepts

- **Durable agent loop** - The workflow can perform bounded research, sleep, resume, and continue without keeping a worker slot busy.
- **Checkpointed LLM calls** - `jobs.Call[T]()` stores each mocked completion. On replay, the workflow sees the same result without calling the model again.
- **Durable sleeps** - `jobs.Sleep()` records the wake deadline and parks the job, so a process crash does not restart the backoff from zero. Wake granularity is coarse (a ~5s polling backstop) — suitable for pacing and backoff, not sub-second timing.
- **Human approval** - `jobs.WaitForSignalTimeout[T]()` stores the approval gate in the database and resumes when `jobs.Signal()` arrives.
- **Final action checkpoint** - `jobs.Call[any]()` wraps the action step so a crash after approval does not require rebuilding the plan.

## Tips

- Put expensive or externally visible model calls behind `jobs.Call[T]()` so replay does not duplicate spend or side effects.
- Keep the loop bounded with a maximum iteration count. Durable retries should not create unbounded agent behavior.
- If the process crashes mid-run, a restarted worker resumes from the last checkpoint or sleep deadline. Already-completed LLM calls are replayed from storage, and unfinished work continues from the next durable boundary.
- The approval cannot be lost because it is stored as a signal row for the job. It may arrive before the workflow reaches the gate, while the job is sleeping, or while the process is down; the next replay consumes it before `act-on-plan` runs.

## Related Documentation

- [API Reference - Durable Workflows](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/workflows/)
- [API Reference - Workflow Signals](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/signals/)
- [Advanced - Durable Timers](https://jdziat.github.io/simple-durable-jobs/docs/advanced/durable-timers/)
- [Advanced - Workflow Versioning](https://jdziat.github.io/simple-durable-jobs/docs/advanced/workflow-versioning/)
- [pkg.go.dev - simple-durable-jobs](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs/v3)
