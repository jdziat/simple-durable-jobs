// Package worker provides the Worker job processor for the jobs package.
package worker

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// minStaleLockInterval is the floor for the stale-lock reaper cadence set via
// WithStaleLockInterval. It guards against a pathologically tight reaper
// hammering the database; the reaper itself can never be turned off.
const minStaleLockInterval = 1 * time.Second

const maxDequeueBatch = 1000

// WorkerOption configures a Worker.
type WorkerOption interface {
	ApplyWorker(*WorkerConfig)
}

type workerOptionFunc func(*WorkerConfig)

func (f workerOptionFunc) ApplyWorker(c *WorkerConfig) { f(c) }

// CapOption configures a fleet-wide concurrency cap.
type CapOption func(*ConcurrencyCapConfig)

// ConcurrencyCapConfig describes a DB-backed concurrency cap. If Key is nil,
// the cap is fleet-wide and uses Name as the slot name. If Key is set, the
// effective slot name is Name + ":" + Key(job), allowing per-tenant or per-key
// partitions under the same configured cap.
type ConcurrencyCapConfig struct {
	Name  string
	Limit int
	Key   func(*core.Job) string
}

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	Queues          map[string]int // queue name -> concurrency
	PollInterval    time.Duration
	WorkerID        string
	EnableScheduler bool
	currentQueue    string // internal: scopes Concurrency to this queue

	// ConcurrencyCaps are optional DB-backed caps enforced across the fleet when
	// the storage backend implements the worker's optional concurrency slot
	// capability. Backends without that capability keep existing per-process
	// behavior.
	ConcurrencyCaps []ConcurrencyCapConfig

	// DrainTimeout is how long Start waits after its context is cancelled for
	// in-flight handlers to finish and persist their result before forcing
	// cancellation. A non-positive value aborts immediately. Default: 30 seconds.
	DrainTimeout time.Duration

	// StorageRetry configures retry behavior for storage operations.
	// If nil, uses DefaultRetryConfig().
	StorageRetry *RetryConfig

	// DequeueRetry configures retry behavior specifically for dequeue operations.
	// Dequeue failures use backoff to prevent hammering the database.
	// If nil, uses a longer backoff config suitable for polling.
	DequeueRetry *RetryConfig

	// StaleLockInterval is how often the worker checks for stale running jobs
	// (jobs whose owning worker died) and reclaims them. The reaper is the
	// only mechanism that recovers jobs from crashed workers, so it is always
	// running and CANNOT be disabled — it can only be retuned. A non-positive
	// value keeps the default; values below minStaleLockInterval are clamped
	// up. Default: 5 minutes.
	StaleLockInterval time.Duration

	// StaleLockAge is how long a running job's lock must be expired before
	// it is reclaimed (reset to pending). Default: 45 minutes (matches lock duration).
	StaleLockAge time.Duration

	// FanOutRecoveryStaleAge is how old a pending fan-out must be before a
	// waiting parent is treated as wedged because not all sub-jobs were
	// persisted. The recovery loop cannot be disabled; non-positive values
	// fall back to the default. Default: 2 minutes.
	FanOutRecoveryStaleAge time.Duration

	// OwnershipAuditInterval is how often the worker checks whether any of
	// its in-flight jobs have been cancelled (e.g. by a fan-out failure on
	// another worker) or reclaimed (e.g. by a stale-lock reaper running on
	// another worker). Any orphaned local handler has its context cancelled.
	// This is the cross-worker counterpart of the same-worker cancellation
	// that completeFanOut and reapStaleLocks do directly.
	//
	// Default: 5 seconds. Set to 0 to disable. Lower values reduce the
	// cancellation latency for distributed fan-out failures but increase DB
	// query rate; the query is bounded by len(runningJobs), so the cost
	// scales with concurrency, not fleet size.
	OwnershipAuditInterval time.Duration

	// ownershipAuditSet records whether OwnershipAuditInterval was provided
	// explicitly (via WithOwnershipAuditInterval). NewWorker needs this to
	// tell "unset" (apply the 5s default) apart from an explicit 0, which
	// the documented contract treats as "disable the audit".
	ownershipAuditSet bool

	// LockDuration is how long a job is locked when dequeued or heartbeated.
	// Default: 45 minutes. If non-zero, the worker will configure the storage
	// backend with this duration at startup.
	LockDuration time.Duration

	// MaxRetryBackoff caps the exponential backoff between automatic retries of
	// a failing job (RetryAfter delays set by the handler are honored as-is).
	// Default: 1 minute. Raising it backs off harder on a persistently failing
	// dependency instead of re-attempting every minute until retries exhaust.
	MaxRetryBackoff time.Duration

	// JobBackoff configures retry delays for job re-execution after handler
	// errors. If nil, the worker uses DefaultBackoffPolicy().
	JobBackoff core.BackoffPolicy

	// DequeueBatchSize is the maximum number of jobs a worker asks storage to
	// claim in one polling round when the backend implements the optional batch
	// dequeue capability. Default: 1 (single-row dequeue).
	DequeueBatchSize int
}

// Concurrency sets the concurrency for a queue.
// Values are clamped to [1, MaxConcurrency].
func Concurrency(n int) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		clamped := security.ClampConcurrency(n)
		if c.currentQueue != "" {
			c.Queues[c.currentQueue] = clamped
		} else {
			for k := range c.Queues {
				c.Queues[k] = clamped
			}
		}
	})
}

// CapKey derives the partition key for a ConcurrencyCap from the dequeued job.
// The effective slot name is capName + ":" + key(job).
func CapKey(fn func(*core.Job) string) CapOption {
	return func(c *ConcurrencyCapConfig) {
		c.Key = fn
	}
}

// ConcurrencyCap limits concurrent jobs across the fleet when the storage
// backend supports DB-backed concurrency slots. Without CapKey, the cap is
// fleet-wide under name. With CapKey, the same limit applies independently to
// each derived key.
func ConcurrencyCap(name string, limit int, opts ...CapOption) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		cfg := ConcurrencyCapConfig{
			Name:  name,
			Limit: security.ClampConcurrency(limit),
		}
		for _, opt := range opts {
			opt(&cfg)
		}
		c.ConcurrencyCaps = append(c.ConcurrencyCaps, cfg)
	})
}

// WithScheduler enables the scheduler in the worker.
func WithScheduler(enabled bool) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.EnableScheduler = enabled
	})
}

// WorkerQueue adds a queue to process with optional concurrency.
func WorkerQueue(name string, opts ...WorkerOption) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if c.Queues == nil {
			c.Queues = make(map[string]int)
		}
		c.Queues[name] = 10 // default concurrency
		prev := c.currentQueue
		c.currentQueue = name
		for _, opt := range opts {
			opt.ApplyWorker(c)
		}
		c.currentQueue = prev
	})
}

// WithStorageRetry configures retry behavior for storage operations
// (Complete, Fail, SaveCheckpoint). This helps handle transient database errors.
func WithStorageRetry(config RetryConfig) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.StorageRetry = &config
	})
}

// WithDequeueRetry configures retry behavior specifically for dequeue operations.
// When the database is temporarily unavailable, this prevents tight-loop polling.
func WithDequeueRetry(config RetryConfig) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.DequeueRetry = &config
	})
}

// WithRetryAttempts sets the max retry attempts for storage operations.
// This is a convenience function; use WithStorageRetry for full control.
func WithRetryAttempts(attempts int) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if c.StorageRetry == nil {
			cfg := DefaultRetryConfig()
			c.StorageRetry = &cfg
		}
		c.StorageRetry.MaxAttempts = attempts
	})
}

// DisableRetry disables retry for storage operations.
func DisableRetry() WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		noRetry := RetryConfig{MaxAttempts: 1}
		c.StorageRetry = &noRetry
		c.DequeueRetry = &noRetry
	})
}

// WithPollInterval sets the interval between job polling attempts.
// Lower values increase throughput but also database load.
// Default is 100ms. Minimum is 50ms to prevent database overload.
func WithPollInterval(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if d >= 50*time.Millisecond {
			c.PollInterval = d
		}
	})
}

// WithDrainTimeout sets how long Start waits for in-flight handlers to finish
// after its context is cancelled. A non-positive duration aborts immediately.
func WithDrainTimeout(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.DrainTimeout = d
	})
}

// WithStaleLockInterval sets how often the worker checks for stale running
// jobs. The stale-lock reaper is what recovers jobs from crashed workers, so
// it CANNOT be disabled: a non-positive duration is ignored (the default is
// kept) and values below the 1s floor are clamped up to it. Default is 5
// minutes.
func WithStaleLockInterval(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if d <= 0 {
			return // can't disable the reaper — keep whatever default applies
		}
		if d < minStaleLockInterval {
			d = minStaleLockInterval
		}
		c.StaleLockInterval = d
	})
}

// WithStaleLockAge sets how long a lock must be expired before the job is reclaimed.
// Default is 45 minutes (matching the lock duration).
func WithStaleLockAge(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.StaleLockAge = d
	})
}

// WithFanOutRecoveryStaleAge sets how old a pending fan-out must be before
// the waiting parent is resumed for replay-based recovery. This recovery
// cannot be disabled; a non-positive duration is treated as the default by
// NewWorker.
func WithFanOutRecoveryStaleAge(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.FanOutRecoveryStaleAge = d
	})
}

// WithLockDuration sets how long a job is locked when dequeued or extended by
// a heartbeat. The worker propagates this to the storage backend if the backend
// implements SetLockDuration. Default is 45 minutes.
func WithLockDuration(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.LockDuration = d
	})
}

// WithMaxRetryBackoff caps the exponential backoff between automatic retries of
// a failing job. Default is 1 minute. Non-positive values keep the default.
func WithMaxRetryBackoff(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if d > 0 {
			c.MaxRetryBackoff = d
		}
	})
}

// WithBackoff configures the worker-default retry backoff policy for failed
// job re-execution. Per-handler policies and RetryAfter override it.
func WithBackoff(p BackoffPolicy) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.JobBackoff = p
	})
}

// WithDequeueBatchSize sets the per-poll cap for optional batch dequeue.
// Values are clamped to [1, maxDequeueBatch]. Backends that do not implement
// the batch capability continue using single-row dequeue.
func WithDequeueBatchSize(n int) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if n < 1 {
			n = 1
		}
		if n > maxDequeueBatch {
			n = maxDequeueBatch
		}
		c.DequeueBatchSize = n
	})
}

// WithOwnershipAuditInterval sets how often the worker checks whether its
// in-flight jobs have been cancelled or reclaimed by another worker.
// Default is 5 seconds. Set to 0 to disable.
//
// Lower values reduce cross-worker cancellation latency but increase the DB
// query rate. The query cost scales with this worker's concurrency
// (len(runningJobs)), not with fleet size.
func WithOwnershipAuditInterval(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.OwnershipAuditInterval = d
		c.ownershipAuditSet = true
	})
}
