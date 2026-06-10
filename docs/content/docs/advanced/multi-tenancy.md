---
title: "Multi-tenancy without Schema Isolation"
weight: 66
toc: true
---

Simple Durable Jobs does not provide schema-level multi-tenancy. The built-in tenant support is a persisted owner label: set it with `jobs.WithTenant(...)`, read it from `Job.Tenant`, filter programmatically with `SearchJobs` and `JobFilter.Tenant`, and filter in the embedded dashboard's Jobs view. All jobs still live in the same database tables.

Use this when tenants share an application database and you need fair scheduling or noisy-neighbor controls. Do not use it as a substitute for regulatory, contractual, or security isolation.

For API details, see [Job Options]({{< relref "/docs/api-reference/job-options" >}}) for `WithTenant` and [Types]({{< relref "/docs/api-reference/types" >}}) for `Job.Tenant` and `JobFilter.Tenant`.

## Primary Pattern: Set `Job.Tenant`

Use `WithTenant` on every tenant-owned enqueue. Keep the tenant in your application arguments too when the handler needs it to load tenant-owned rows, but use `Job.Tenant` as the queue's operational label.

```go
jobID, err := queue.Enqueue(ctx, "tenant.import", ImportArgs{
	TenantID: tenantID,
	FileID:   fileID,
},
	jobs.WithTenant(tenantID),
	jobs.Unique("tenant:"+tenantID+":import:"+fileID),
)
if err != nil {
	return err
}
_ = jobID
```

Inside handlers or middleware, `JobFromContext` exposes the persisted job record:

```go
queue.Register("tenant.import", func(ctx context.Context, args ImportArgs) error {
	job := jobs.JobFromContext(ctx)
	if job == nil || job.Tenant == "" {
		return fmt.Errorf("missing tenant")
	}

	// Load tenant-owned application rows by job.Tenant and args.FileID.
	// Keep side effects idempotent; jobs are at-least-once.
	return runImport(ctx, job.Tenant, args.FileID)
})
```

`JobFilter.Tenant` is the storage filter field for exact tenant matches. `GormStorage.SearchJobs` applies it as `tenant = ?`, and code that calls UI-capable storage directly can use the `ui.JobFilter` alias. The embedded dashboard's Jobs view has the same tenant filter, and job detail pages show the persisted tenant and metadata key/value tags.

```go
matches, total, err := store.SearchJobs(ctx, ui.JobFilter{
	Tenant: "acme",
	Limit:  50,
})
if err != nil {
	return err
}
_ = matches
_ = total
```

## Fairness Controls

The tenant field is a label and query filter. Worker dequeue is still scoped by queue names, not by tenant, so use per-key worker controls when you need tenant-level fairness on a shared queue.

Per-key concurrency caps limit in-flight work for each tenant across the worker fleet:

```go
func tenantFromJob(job *jobs.Job) string {
	if job.Tenant == "" {
		return "unknown"
	}
	return job.Tenant
}

worker := queue.NewWorker(
	jobs.WorkerQueue("imports", jobs.Concurrency(20)),
	jobs.ConcurrencyCap("tenant-imports", 2, jobs.CapKey(tenantFromJob)),
)
```

Per-key rate limits limit admission rate for each tenant when the storage backend supports rate-limit windows:

```go
worker := queue.NewWorker(
	jobs.WorkerQueue("imports", jobs.Concurrency(20)),
	jobs.RateLimit("tenant-import-starts", 1, jobs.RateLimitKey(tenantFromJob)),
)
```

Metrics and dashboards should keep the tenant visible. `Job.Tenant` is available for job search/filtering in code and in the embedded dashboard, and application metrics emitted from handlers should include the same tenant label.

## Worked Example

This example uses `WithTenant` as the queue's tenant label, keeps the tenant in handler arguments for application data access, and partitions concurrency/rate limits by `Job.Tenant`.

```go
type ImportArgs struct {
	TenantID string `json:"tenant_id"`
	FileID   string `json:"file_id"`
}

func tenantFromJob(job *jobs.Job) string {
	if job.Tenant == "" {
		return "unknown"
	}
	return job.Tenant
}

queue.Register("tenant.import", func(ctx context.Context, args ImportArgs) error {
	job := jobs.JobFromContext(ctx)
	if job == nil || job.Tenant == "" {
		return fmt.Errorf("missing tenant")
	}

	// Load tenant-owned application rows by job.Tenant and args.FileID.
	// Keep side effects idempotent; jobs are at-least-once.
	return runImport(ctx, job.Tenant, args.FileID)
})

for _, tenantID := range []string{"acme", "globex"} {
	if _, err := queue.Enqueue(ctx, "tenant.import", ImportArgs{
		TenantID: tenantID,
		FileID:   "file-001",
	},
		jobs.QueueOpt("imports"),
		jobs.WithTenant(tenantID),
		jobs.Unique("tenant:"+tenantID+":import:file-001"),
	); err != nil {
		return err
	}
}

worker := queue.NewWorker(
	jobs.WorkerQueue("imports", jobs.Concurrency(20)),
	jobs.ConcurrencyCap("tenant-imports", 2, jobs.CapKey(tenantFromJob)),
	jobs.RateLimit("tenant-import-starts", 1, jobs.RateLimitKey(tenantFromJob)),
	jobs.WithPollInterval(100*time.Millisecond),
)

return worker.Start(ctx)
```

With this setup, each job stores its tenant in `Job.Tenant`, programmatic calls to `SearchJobs` can select tenant `acme` with `JobFilter.Tenant`, the dashboard can filter the Jobs view by tenant and display each job's metadata, each tenant can have at most two imports running under the cap, and each tenant starts at roughly one admitted job per second under the fleet-wide rate limit. The cap and rate limit are partitioned by `tenantFromJob`, so tenant `acme` and tenant `globex` do not consume each other's slots.

For a runnable version of the cap and rate-limit mechanics, see the [rate-limit example](https://github.com/jdziat/simple-durable-jobs/tree/main/examples/ratelimit).

## Optional: Queue-per-tenant Isolation

Tenant-scoped queues are still useful when you want queue-level routing, pause/resume, worker pools, or dashboard queue filters per tenant. Combine them with `WithTenant` instead of using queue names as the only tenant record:

```go
queueName := "tenant-" + tenantID
jobID, err := queue.Enqueue(ctx, "tenant.import", ImportArgs{
	TenantID: tenantID,
	FileID:   fileID,
},
	jobs.QueueOpt(queueName),
	jobs.WithTenant(tenantID),
	jobs.Unique("tenant:"+tenantID+":import:"+fileID),
)
if err != nil {
	return err
}
_ = jobID
```

This gives operators a queue handle such as `tenant-acme`, while still preserving the first-class `Job.Tenant` label for job records and tenant filters.

## What This Does Not Give

This pattern does not provide row-level isolation. Jobs from every tenant are rows in the same `jobs` table, even when `Job.Tenant` is set.

It does not provide tenant-scoped dequeue. Workers dequeue by queue name and job status; use queue-per-tenant routing or per-key concurrency/rate limits when dispatch must be partitioned.

It does not provide separate retention policies per tenant. Retention runs by terminal status and age, not by tenant.

It does not prevent an operator with database access or dashboard access from seeing other tenants' job metadata. Protect the dashboard with application middleware and database access controls.

It does not provide schema-level tenancy. Separate schemas, separate databases, per-tenant encryption domains, or hard deletion guarantees are out of scope today.

If a tenant must have legally or operationally isolated storage, use separate databases or a separate deployment per tenant instead of this pattern.
