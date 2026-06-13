# Durable Signals

This example demonstrates human-in-the-loop workflow approval with durable signals.

## What It Does

- Sets up a SQLite-backed queue for an approval workflow
- Registers a `prepare-request` step and checkpoints it with `jobs.Call[T]()`
- Pre-buffers an `audit-note` signal before the worker starts
- Uses `jobs.CheckSignal[T]()` to consume the already-buffered signal without blocking
- Waits for a human `approval` signal and resumes the workflow after it arrives
- Drains audit notes with `jobs.DrainSignals[T]()` before the final side effect

## Prerequisites

- Go 1.25+
- No external services needed (uses SQLite)

## Running

```bash
go run ./examples/signals
```

The example runs for a few seconds and then shuts down after the approval workflow completes.

## Expected Output

Framework INFO logs are written to stderr and elided here; the stdout
narrative below matches a real run (job IDs vary).

```
Enqueued approval workflow 0a4ccb2a
Main pre-buffered audit note before the workflow checked signals
Starting worker...
[workflow] Starting approval workflow REQ-1001
  [call] Prepared approval packet for REQ-1001
[workflow] Checkpointed preparation result: packet-REQ-1001
[workflow] Audit note signal is already buffered: approved by alice@example.com
[workflow] Waiting for human approval...
Main observed workflow waiting; sending audit note and approval
[workflow] Starting approval workflow REQ-1001
[workflow] Checkpointed preparation result: packet-REQ-1001
[workflow] Audit note signal is already buffered: approved by alice@example.com
[workflow] Waiting for human approval...
[workflow] Drained audit note: approved by alice@example.com
[workflow] Drained audit note: approval confirmed while waiting
[workflow] Approved REQ-1001; final side effect would run here idempotently
Signals example complete: approval workflow finished successfully
```

The workflow re-enters after the wait, but the preparation step is not re-run because `jobs.Call[T]()` returns the checkpointed result.

## Key Concepts

- **Signals are durable** - `jobs.Signal()` stores values in SQLite until the target workflow consumes them.
- **Non-blocking checks** - `jobs.CheckSignal[T]()` lets a workflow consume an already-buffered signal without suspending.
- **Blocking waits** - `jobs.WaitForSignalTimeout[T]()` parks the job in a waiting state and frees the worker slot until a signal or timeout arrives.
- **Signal draining** - `jobs.DrainSignals[T]()` consumes all pending signals of a name in FIFO order.
- **Checkpointed preparation** - `jobs.Call[T]()` keeps the approval packet from being rebuilt on replay.

## Tips

- Send signals with stable names such as `approval`, `cancellation`, or `audit-note`; the name is part of the workflow contract.
- Treat handlers as at-least-once. Put external side effects behind checkpoints or make them idempotent.
- Use timeouts for human approval paths so abandoned workflows can escalate, fail with `jobs.NoRetry()`, or send reminders.
- Signals can arrive before the workflow waits. They are buffered and consumed later instead of being lost.

## Related Documentation

- [API Reference - Workflow Signals](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/signals/)
- [Advanced - Durable Timers](https://jdziat.github.io/simple-durable-jobs/docs/advanced/durable-timers/)
- [API Reference - Durable Workflows](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/workflows/)
- [pkg.go.dev - simple-durable-jobs](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs/v3)
