# Rate Limit

This example demonstrates a per-queue rate limit and a per-key concurrency cap as two separate constraints.

## What It Does

- Sets up a SQLite-backed queue with one `limited-work` handler
- Runs Phase A with short 50ms work and a `3/sec` queue token bucket
- Shows Phase A admissions spaced by the queue rate limit while in-flight work stays at 1
- Runs Phase B with 700ms work, no queue rate limit, and a per-tenant cap of 2
- Shows Phase B in-flight work clamped to the tenant cap
- Prints a legend and deterministic success line after both constraints are observed

## Prerequisites

- Go 1.25+
- No external services needed (uses SQLite)

## Running

```bash
go run ./examples/ratelimit
```

The example completes in a few seconds and then shuts down both demonstration workers.

## Expected Output

```
Legend: elapsed=seconds since phase start; in-flight=active jobs for the tenant; avg-start-rate=first-to-last start spacing.

=== Phase A: queue rate limit binds ===
Enqueued phase A job c1902a5b item=01
Enqueued phase A job 47d550d8 item=02
Enqueued phase A job ad86d6bb item=03
Enqueued phase A job d7ba9339 item=04
Enqueued phase A job 3f5d931f item=05
Enqueued phase A job ce34d3fd item=06
Enqueued phase A job 0fd2fbc8 item=07
Enqueued phase A job da95e45d item=08
Starting phase A worker with 3/sec queue rate limit and burst=1...
[phase A elapsed=0.057s] start item=01 tenant=tenant-a in-flight=1
[phase A elapsed=0.407s] start item=02 tenant=tenant-a in-flight=1
[phase A elapsed=0.758s] start item=03 tenant=tenant-a in-flight=1
[phase A elapsed=1.108s] start item=04 tenant=tenant-a in-flight=1
[phase A elapsed=1.458s] start item=05 tenant=tenant-a in-flight=1
[phase A elapsed=1.807s] start item=06 tenant=tenant-a in-flight=1
[phase A elapsed=2.158s] start item=07 tenant=tenant-a in-flight=1
[phase A elapsed=2.507s] start item=08 tenant=tenant-a in-flight=1
Phase A observed starts: 8 jobs over 2.45s after first start (2.86 avg-start-rate)
Phase A maximum in-flight for tenant-a: 1
Phase A pacing governed by the queue rate limit (3/sec); 50ms work keeps concurrency from binding.

=== Phase B: per-key concurrency cap binds ===
Enqueued phase B job 1b25d5a3 item=01
Enqueued phase B job 470b70a6 item=02
Enqueued phase B job cf687f43 item=03
Enqueued phase B job c4b3a73a item=04
Enqueued phase B job edd8f925 item=05
Enqueued phase B job a63df243 item=06
Starting phase B worker with per-tenant cap=2 and no queue rate limit...
[phase B elapsed=0.062s] start item=01 tenant=tenant-a in-flight=1
[phase B elapsed=0.111s] start item=02 tenant=tenant-a in-flight=2
[phase B elapsed=0.811s] start item=03 tenant=tenant-a in-flight=1
[phase B elapsed=0.861s] start item=04 tenant=tenant-a in-flight=2
[phase B elapsed=1.561s] start item=05 tenant=tenant-a in-flight=2
[phase B elapsed=1.612s] start item=06 tenant=tenant-a in-flight=2
Phase B maximum observed in-flight for tenant-a: 2
Phase B pacing governed by the per-key concurrency cap (2); no queue rate limit is configured.
Rate limit example complete: queue rate limit and per-key concurrency cap both observed
```

Phase A is governed by admission pacing. Phase B is governed by shared in-flight ownership of the tenant key.

## Key Concepts

- **Queue rate limits** use `jobs.WithQueueRateLimit(queue, perSecond, burst)` to throttle dequeue admission per worker process.
- **Burst size** controls how many jobs can start immediately before the token bucket must refill.
- **Concurrency caps** use `jobs.ConcurrencyCap()` to limit in-flight jobs across workers when storage supports cap slots.
- **Per-key caps** use `jobs.CapKey()` to partition the cap by tenant, customer, account, or another job-derived key.
- **Independent constraints** are easiest to validate when work duration makes one limit bind while the other cannot.

## Tips

- Use queue rate limits for provider request ceilings such as API calls per second.
- Use concurrency caps for scarce resources such as tenant imports, GPU slots, or provider accounts.
- Remember that queue rate limits are per worker process. Fleet-wide rate limits require the storage-backed rate-limit feature described in the docs.
- Keep handler work idempotent because throttling controls admission, not at-least-once delivery semantics.

## Related Documentation

- [Advanced - Rate Limiting](https://jdziat.github.io/simple-durable-jobs/docs/advanced/rate-limiting/)
- [Advanced - Concurrency Caps](https://jdziat.github.io/simple-durable-jobs/docs/advanced/concurrency-caps/)
- [API Reference - Worker Options](https://jdziat.github.io/simple-durable-jobs/docs/api-reference/worker/)
- [pkg.go.dev - simple-durable-jobs](https://pkg.go.dev/github.com/jdziat/simple-durable-jobs)
