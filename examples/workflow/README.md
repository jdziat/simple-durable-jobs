# Durable Workflows

This example demonstrates multi-step workflows with automatic checkpointing and crash recovery.

## What It Does

- Builds an order processing workflow with 4 steps: validate, charge payment, ship, send confirmation
- Each step is a separate registered handler that can return typed results
- The `process-order` workflow orchestrates the steps using `jobs.Call[T]()` for checkpointing
- Simulates occasional payment failures (30% chance) to demonstrate retry behavior
- Enqueues 3 orders with up to 5 retries each

## Prerequisites

- Go 1.21+
- No external services needed (uses SQLite)

## Running

```bash
go run main.go
```

The example runs for up to 30 seconds and then shuts down.

## Expected Output

```
Enqueued workflow abc12345 for order ORD-001
Enqueued workflow def67890 for order ORD-002
Enqueued workflow ghi24680 for order ORD-003

Starting worker...

=== Processing Order ORD-001 ===
  [Step 1] Validating order ORD-001...
  [Step 1] Order ORD-001 validated
  [Step 2] Charging payment for order ORD-001 ($99.99)...
  [Step 2] Payment successful: txn_1234567890
  [Step 3] Shipping order ORD-001...
  [Step 3] Order shipped: TRACK123456
  [Step 4] Sending confirmation to alice@example.com...
  [Step 4] Confirmation sent
=== Order ORD-001 Complete ===
```

When a payment fails, the workflow retries but **skips already-completed steps** thanks to checkpointing.

## Key Concepts

- **`jobs.Call[T]()`** executes a step and saves the result as a checkpoint. On retry, it returns the cached result instead of re-executing.
- **Typed results** - Each step can return a value (`(T, error)`) that the next step uses. Use generics to specify the return type: `jobs.Call[string](ctx, "step-name", args)`.
- **Crash recovery** - If the process crashes between steps 2 and 3, the workflow resumes at step 3 on the next attempt. Steps 1 and 2 return their cached results instantly.
- **Retry configuration** - `jobs.Retries(5)` gives the workflow 5 attempts to complete all steps.

## Tips

- Design each step to be **idempotent** when possible. Even with checkpointing, external side effects (like charging a credit card) should handle duplicates gracefully.
- Use `jobs.Call[any]()` when a step doesn't return a meaningful value (e.g., sending a notification).
- Steps execute **synchronously within the workflow** -- they are not separate jobs. The checkpointing mechanism is what provides durability.
- Keep step handlers focused on a single responsibility. This makes it easy to test them independently.

## Related Documentation

- [Getting Started - Durable Workflows](https://jdziat.github.io/simple-durable-jobs/docs/getting-started/#durable-workflows)
- [API Reference - Durable Calls](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/#durable-calls)
- [Examples](https://jdziat.github.io/simple-durable-jobs/docs/examples/)
