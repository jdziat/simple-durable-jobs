---
title: "Connection Pool Configuration"
weight: 2
---

A job queue system opens many concurrent database connections -- one per worker
goroutine, plus connections for heartbeats, scheduling, and the stale lock
reaper. Without proper pool configuration the database can be overwhelmed by
connection churn, or workers can stall waiting for an available connection.

The `simple-durable-jobs` library exposes Go's `database/sql` pool settings
through a clean option-based API so you can tune connection usage to match your
workload.

## Creating Storage with Pool Configuration

### Using a Preset

The fastest way to get started is to pick a preset that matches your deployment
profile:

```go
storage, err := jobs.NewGormStorageWithPool(db, jobs.HighConcurrencyPoolConfig())
if err != nil {
    log.Fatal(err)
}
```

### Customizing Individual Settings

You can mix and match individual pool options:

```go
storage, err := jobs.NewGormStorageWithPool(db,
    jobs.MaxOpenConns(50),
    jobs.MaxIdleConns(20),
    jobs.ConnMaxLifetime(10 * time.Minute),
    jobs.ConnMaxIdleTime(2 * time.Minute),
)
if err != nil {
    log.Fatal(err)
}
```

### Configuring an Existing Database

If you already have a `*gorm.DB` that you opened elsewhere, you can apply pool
settings to it directly and then create storage normally:

```go
err := jobs.ConfigurePool(db, jobs.MaxOpenConns(100))
if err != nil {
    log.Fatal(err)
}
storage := jobs.NewGormStorage(db)
```

## Available Presets

Each preset returns a `PoolConfig` struct with values tuned for a specific
deployment scenario:

| Preset | MaxOpen | MaxIdle | MaxLifetime | MaxIdleTime | Use Case |
|--------|---------|---------|-------------|-------------|----------|
| `DefaultPoolConfig()` | 25 | 10 | 5 min | 1 min | General purpose |
| `HighConcurrencyPoolConfig()` | 100 | 25 | 10 min | 2 min | 50+ workers, high throughput |
| `LowLatencyPoolConfig()` | 50 | 40 | 15 min | 5 min | Latency-sensitive workloads |
| `ResourceConstrainedPoolConfig()` | 10 | 5 | 3 min | 30 s | Limited DB resources |

### DefaultPoolConfig

Suitable for most workloads. Allows up to 25 concurrent database connections
with 10 kept warm in the idle pool. Connections are recycled every 5 minutes to
prevent stale connections from accumulating.

### HighConcurrencyPoolConfig

Designed for deployments running 50 or more worker goroutines. Opens up to 100
connections and keeps 25 idle. Longer lifetimes reduce the overhead of
re-establishing connections under sustained load.

### LowLatencyPoolConfig

Optimized for workloads where connection acquisition time matters. Keeps 40 of
50 connections warm in the idle pool, so most dequeue and completion operations
hit a pre-established connection. Longer idle times prevent unnecessary
teardown.

### ResourceConstrainedPoolConfig

Use this when your database has strict connection limits (for example, a small
managed database instance or a serverless database with a low connection cap).
Limits the pool to 10 open and 5 idle connections with aggressive recycling.

## Individual Options

Each option sets a single field on the underlying `PoolConfig`:

| Option | Description |
|--------|-------------|
| `MaxOpenConns(n int)` | Maximum number of open database connections. Set to 0 for unlimited (not recommended for production). |
| `MaxIdleConns(n int)` | Maximum number of idle connections kept in the pool. Should be less than or equal to `MaxOpenConns`. |
| `ConnMaxLifetime(d time.Duration)` | Maximum amount of time a connection can be reused before it is closed and replaced. Set to 0 for no limit (not recommended). |
| `ConnMaxIdleTime(d time.Duration)` | Maximum amount of time a connection can sit idle before it is closed. Set to 0 for no limit. |

## Choosing the Right Configuration

Here are guidelines to help you pick the right settings:

### Match MaxOpenConns to Worker Concurrency

Every worker goroutine may hold a database connection while processing a job.
Add connections for heartbeats, the stale lock reaper, the scheduler, and the
dequeue poll loop. A safe starting point is:

```
MaxOpenConns >= total worker concurrency + 10
```

For example, if you run three queues with concurrency 20, 10, and 5 (total 35),
set `MaxOpenConns` to at least 45.

### Size the Idle Pool at 25-50% of MaxOpenConns

Idle connections avoid the latency of establishing a new TCP connection and TLS
handshake. For typical workloads, keeping 25-50% of connections warm provides a
good balance between responsiveness and resource usage.

### Set ConnMaxLifetime to 5-15 Minutes

Recycling connections periodically prevents issues with stale TCP sessions,
load-balancer idle timeouts, and DNS changes in cloud environments. Five minutes
is a good default; increase to 10-15 minutes under sustained high throughput to
reduce churn.

### Respect Cloud Database Limits

Many managed database services impose connection limits (for example, a small
PostgreSQL instance may allow only 100 connections shared across all
applications). Use `ResourceConstrainedPoolConfig` or manually set
`MaxOpenConns` to stay within your quota. Remember that each application replica
has its own pool, so divide the limit across replicas.
