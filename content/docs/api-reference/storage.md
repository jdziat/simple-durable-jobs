---
title: "Storage & Embedded UI"
weight: 10
---

## Embedded UI

### `ui.Handler(storage core.Storage, opts ...Option) http.Handler`

Creates an HTTP handler serving the dashboard and Connect-RPC API.

```go
import "github.com/jdziat/simple-durable-jobs/ui"

mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage,
    ui.WithQueue(queue),
    ui.WithContext(ctx),
    ui.WithStatsRetention(7 * 24 * time.Hour),
    ui.WithMiddleware(authMiddleware),
)))
```

### UI Options

| Option | Description |
|--------|-------------|
| `WithQueue(q)` | Provides queue for event streaming and scheduled jobs view |
| `WithContext(ctx)` | Lifecycle context for background goroutines; when cancelled, workers flush and exit |
| `WithStatsRetention(d)` | How long stats rows are kept (default: 7 days) |
| `WithMiddleware(mw)` | Wraps handler with middleware (auth, logging, etc.) |

---

## Storage

### `NewGormStorage(db *gorm.DB) *GormStorage`

Creates a new GORM-based storage implementation.

### `NewGormStorageWithPool(db *gorm.DB, opts ...PoolOption) (*GormStorage, error)`

Creates storage with connection pool configuration.

### Pool Presets

```go
jobs.DefaultPoolConfig()              // Sensible defaults
jobs.HighConcurrencyPoolConfig()      // High-concurrency workloads
jobs.LowLatencyPoolConfig()          // Low-latency optimized
jobs.ResourceConstrainedPoolConfig() // Limited resources
```

| Preset | MaxOpen | MaxIdle | MaxLifetime | MaxIdleTime |
|--------|---------|---------|-------------|-------------|
| `DefaultPoolConfig()` | 25 | 10 | 5min | 1min |
| `HighConcurrencyPoolConfig()` | 100 | 25 | 10min | 2min |
| `LowLatencyPoolConfig()` | 50 | 40 | 15min | 5min |
| `ResourceConstrainedPoolConfig()` | 10 | 5 | 3min | 30s |

### Pool Option Functions

### `MaxOpenConns(n int) PoolOption`

Sets the maximum number of open connections.

### `MaxIdleConns(n int) PoolOption`

Sets the maximum number of idle connections.

### `ConnMaxLifetime(d time.Duration) PoolOption`

Sets the maximum connection lifetime.

### `ConnMaxIdleTime(d time.Duration) PoolOption`

Sets the maximum idle time for connections.

### `ConfigurePool(db *gorm.DB, opts ...PoolOption) error`

Applies pool configuration to a GORM database connection.
