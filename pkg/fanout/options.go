package fanout

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// Option configures fan-out behavior.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

type config struct {
	strategy        core.FanOutStrategy
	threshold       float64
	queue           string
	priority        int
	retries         int
	subJobTimeout   time.Duration
	totalTimeout    time.Duration
	cancelOnFailure bool
}

func defaultConfig() *config {
	return &config{
		strategy:  core.StrategyFailFast,
		threshold: 1.0,
		retries:   3,
	}
}

// FailFast fails the parent on first sub-job failure.
func FailFast() Option {
	return optionFunc(func(c *config) {
		c.strategy = core.StrategyFailFast
	})
}

// CollectAll waits for all sub-jobs, returns partial results.
func CollectAll() Option {
	return optionFunc(func(c *config) {
		c.strategy = core.StrategyCollectAll
	})
}

// Threshold succeeds if at least pct% of sub-jobs succeed.
func Threshold(pct float64) Option {
	return optionFunc(func(c *config) {
		c.strategy = core.StrategyThreshold
		c.threshold = pct
	})
}

// WithQueue sets the queue for sub-jobs.
func WithQueue(q string) Option {
	return optionFunc(func(c *config) {
		c.queue = q
	})
}

// WithPriority sets the priority for sub-jobs.
func WithPriority(p int) Option {
	return optionFunc(func(c *config) {
		c.priority = p
	})
}

// WithRetries sets the retry count for sub-jobs.
func WithRetries(n int) Option {
	return optionFunc(func(c *config) {
		c.retries = n
	})
}

// WithSubJobTimeout sets timeout for each sub-job.
func WithSubJobTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.subJobTimeout = d
	})
}

// WithTimeout sets timeout for entire fan-out.
func WithTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.totalTimeout = d
	})
}

// CancelOnParentFailure cancels sub-jobs if parent fails.
func CancelOnParentFailure() Option {
	return optionFunc(func(c *config) {
		c.cancelOnFailure = true
	})
}
