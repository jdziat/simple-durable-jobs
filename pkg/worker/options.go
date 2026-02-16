// Package worker provides the Worker job processor for the jobs package.
package worker

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// WorkerOption configures a Worker.
type WorkerOption interface {
	ApplyWorker(*WorkerConfig)
}

type workerOptionFunc func(*WorkerConfig)

func (f workerOptionFunc) ApplyWorker(c *WorkerConfig) { f(c) }

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	Queues          map[string]int // queue name -> concurrency
	PollInterval    time.Duration
	WorkerID        string
	EnableScheduler bool
	currentQueue    string // internal: scopes Concurrency to this queue

	// StorageRetry configures retry behavior for storage operations.
	// If nil, uses DefaultRetryConfig().
	StorageRetry *RetryConfig

	// DequeueRetry configures retry behavior specifically for dequeue operations.
	// Dequeue failures use backoff to prevent hammering the database.
	// If nil, uses a longer backoff config suitable for polling.
	DequeueRetry *RetryConfig
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
