---
title: "Concurrency Caps"
weight: 9
---

## Per-worker vs fleet-wide limits

`Concurrency(n)` limits how many jobs a single worker process runs at once. If
you run 5 workers with `Concurrency(10)`, the fleet can run up to 50 jobs.

`ConcurrencyCap(name, limit, opts...)` adds an optional database-backed cap that
is shared by every worker using a storage backend that supports concurrency
slots. It is additive: storage backends without the optional capability keep the
existing per-worker behavior.

## Fleet-wide cap

Without a key function, the cap uses its name as the slot name. This limits the
job class across the whole fleet:

```go
w := jobs.NewWorker(q,
	jobs.WorkerQueue("emails", jobs.Concurrency(10)),
	jobs.ConcurrencyCap("email-provider", 25),
)
```

Even if many workers are running, at most 25 jobs admitted under
`email-provider` hold live slots at the same time.

## Per-key cap

Use `CapKey` when each customer, tenant, or account needs an independent cap:

```go
w := jobs.NewWorker(q,
	jobs.WorkerQueue("imports", jobs.Concurrency(20)),
	jobs.ConcurrencyCap("tenant-imports", 2, jobs.CapKey(func(job *jobs.Job) string {
		return job.UniqueKey
	})),
)
```

The effective slot name is `tenant-imports:<key>`, so tenant A can run 2 imports
while tenant B also runs 2.

## Admission caveat

Concurrency slots are acquired after a job is dequeued. If the cap is full, the
worker releases the job back to pending so another poll can try later.

Slots are leases. If a worker crashes, its slot is reclaimed automatically after
the lease expires, using the same database-clock anchoring pattern as job locks.
That makes crash recovery automatic, but it also means the effective cap can
briefly exceed `N` for up to the lease window if a crashed worker's job is
picked up again before the old slot expires.

The built-in GORM storage implements this optional capability. Custom storage
backends can opt in by implementing the worker's optional concurrency slot
methods; otherwise `ConcurrencyCap` is ignored and per-worker `Concurrency`
continues to apply.
