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
}

// Concurrency sets the concurrency for a queue.
// Values are clamped to [1, MaxConcurrency].
func Concurrency(n int) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		clamped := security.ClampConcurrency(n)
		for k := range c.Queues {
			c.Queues[k] = clamped
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
		for _, opt := range opts {
			opt.ApplyWorker(c)
		}
	})
}
