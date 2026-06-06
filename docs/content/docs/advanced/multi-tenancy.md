---
title: "Multi-tenancy without Schema Isolation"
weight: 66
toc: true
---

Simple Durable Jobs does not provide schema-level multi-tenancy. The built-in pattern is implicit tenancy: put tenant identity in queue names, job arguments, metrics labels, and worker admission controls while all jobs still live in the same database tables.

Use this when tenants share an application database and you need fair scheduling or noisy-neighbor controls. Do not use it as a substitute for regulatory, contractual, or security isolation.

## Pattern

Tenant-scoped queues make ownership visible and easy to filter:

```go
queueName := "tenant-" + tenantID
jobID, err := queue.Enqueue(ctx, "tenant.import", ImportArgs{
	TenantID: tenantID,
	FileID:   fileID,
}, jobs.QueueOpt(queueName), jobs.Unique("tenant:"+tenantID+":import:"+fileID))
```

Per-key concurrency caps limit in-flight work for each tenant across the worker fleet:

```go
worker := queue.NewWorker(
	jobs.WorkerQueue("tenant-a", jobs.Concurrency(10)),
	jobs.WorkerQueue("tenant-b", jobs.Concurrency(10)),
	jobs.ConcurrencyCap("tenant-imports", 2, jobs.CapKey(tenantFromJob)),
)
```

Per-key rate limits limit admission rate for each tenant when the storage backend supports rate-limit windows:

```go
worker := queue.NewWorker(
	jobs.WorkerQueue("tenant-a", jobs.Concurrency(10)),
	jobs.WorkerQueue("tenant-b", jobs.Concurrency(10)),
	jobs.RateLimit("tenant-api", 5, jobs.RateLimitKey(tenantFromJob)),
)
```

Metrics and dashboards should keep the tenant visible. Queue names are already part of queue depth and dashboard filtering, so `tenant-<id>` queues provide a practical operational handle. If you emit application metrics in handlers, include the same tenant label there.

## Worked Example

This example uses a tenant field in the job arguments as the shared key for queue selection, concurrency, and rate limiting.

```go
type ImportArgs struct {
	TenantID string `json:"tenant_id"`
	FileID   string `json:"file_id"`
}

func tenantFromJob(job *jobs.Job) string {
	var args ImportArgs
	if err := json.Unmarshal(job.Args, &args); err != nil || args.TenantID == "" {
		return "unknown"
	}
	return args.TenantID
}

queue.Register("tenant.import", func(ctx context.Context, args ImportArgs) error {
	// Load tenant-owned application rows by args.TenantID and args.FileID.
	// Keep side effects idempotent; jobs are at-least-once.
	return runImport(ctx, args.TenantID, args.FileID)
})

for _, tenantID := range []string{"acme", "globex"} {
	queueName := "tenant-" + tenantID
	if _, err := queue.Enqueue(ctx, "tenant.import", ImportArgs{
		TenantID: tenantID,
		FileID:   "file-001",
	}, jobs.QueueOpt(queueName), jobs.Unique(queueName+":file-001")); err != nil {
		return err
	}
}

worker := queue.NewWorker(
	jobs.WorkerQueue("tenant-acme", jobs.Concurrency(10)),
	jobs.WorkerQueue("tenant-globex", jobs.Concurrency(10)),
	jobs.ConcurrencyCap("tenant-imports", 2, jobs.CapKey(tenantFromJob)),
	jobs.RateLimit("tenant-import-starts", 1, jobs.RateLimitKey(tenantFromJob)),
	jobs.WithPollInterval(100*time.Millisecond),
)

return worker.Start(ctx)
```

With this setup, each tenant queue can be filtered in the dashboard, each tenant can have at most two imports running under the cap, and each tenant starts at roughly one admitted job per second under the fleet-wide rate limit. The cap and rate limit are partitioned by `tenantFromJob`, so tenant `acme` and tenant `globex` do not consume each other's slots.

For a runnable version of the cap and rate-limit mechanics, see the [rate-limit example](https://github.com/jdziat/simple-durable-jobs/tree/main/examples/ratelimit).

## What This Does Not Give

This pattern does not provide row-level isolation. Jobs from every tenant are rows in the same `jobs` table.

It does not provide separate retention policies per tenant. Retention runs by terminal status and age, not by tenant.

It does not prevent an operator with database access or dashboard access from seeing other tenants' job metadata. Protect the dashboard with application middleware and database access controls.

It does not provide schema-level tenancy. Separate schemas, separate databases, per-tenant encryption domains, or hard deletion guarantees are out of scope today.

If a tenant must have legally or operationally isolated storage, use separate databases or a separate deployment per tenant instead of this pattern.
