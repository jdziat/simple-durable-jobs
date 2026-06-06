---
title: "Rate Limiting"
weight: 10
---

## Local queue throttle

`WithQueueRateLimit(queue, perSecond, burst)` adds a per-worker token bucket for
one queue. The worker checks this bucket before dequeueing, so an empty bucket
means no database row is touched and no retry attempt is consumed.

```go
w := jobs.NewWorker(q,
	jobs.WorkerQueue("llm", jobs.Concurrency(10)),
	jobs.WithQueueRateLimit("llm", 5, 10),
)
```

This worker can dispatch an immediate burst of 10 jobs from `llm`, then refills
at 5 jobs per second. The limit is local to this worker process; running more
workers multiplies the total rate.

## Fleet-wide throttle

`RateLimit(name, perSecond, opts...)` adds a database-backed fixed-window limit
shared by every worker using a storage backend that implements the optional rate
limit capability. The bundled `GormStorage` implements it.

```go
w := jobs.NewWorker(q,
	jobs.WorkerQueue("payments", jobs.Concurrency(20)),
	jobs.RateLimit("stripe-api", 100),
)
```

If a job is dequeued while the fleet-wide limit is full, the worker releases it
back to pending and tries again on a later poll. That throttle wait does not
call failure hooks and does not consume a retry attempt.

## Per-key throttle

Use `RateLimitKey` when each tenant, account, or customer should have an
independent quota:

```go
w := jobs.NewWorker(q,
	jobs.WorkerQueue("sms", jobs.Concurrency(20)),
	jobs.RateLimit("sms-provider", 2, jobs.RateLimitKey(func(job *jobs.Job) string {
		return job.UniqueKey
	})),
)
```

The effective limit name is `sms-provider:<key>`, so tenant A and tenant B each
get their own 2-per-second allowance.

## Storage support

Fleet-wide rate limits are additive. Storage backends without the optional
capability continue to run jobs unchanged, and the worker logs once that
`RateLimit` enforcement is unavailable. Custom backends can opt in by
implementing the worker's optional rate-limit method.

TODO: Add these options to the worker API reference when that page is in scope.
