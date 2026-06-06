package worker

import (
	"math/rand"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// BackoffPolicy computes the delay before a failed job is retried.
type BackoffPolicy = core.BackoffPolicy

// BackoffFunc adapts a function to BackoffPolicy.
type BackoffFunc func(attempt int, err error) time.Duration

// NextRetry computes the delay before the next job retry.
func (f BackoffFunc) NextRetry(attempt int, err error) time.Duration {
	return f(attempt, err)
}

// ExponentialBackoff computes exponential job retry delays with optional jitter.
type ExponentialBackoff struct {
	InitialInterval time.Duration
	Multiplier      float64
	MaxInterval     time.Duration
	JitterFraction  float64
}

// DefaultBackoffPolicy returns the library default job retry backoff policy.
func DefaultBackoffPolicy() BackoffPolicy {
	return ExponentialBackoff{
		InitialInterval: time.Second,
		Multiplier:      2,
		MaxInterval:     time.Minute,
		JitterFraction:  0.1,
	}
}

// NextRetry computes the delay before the next job retry.
func (b ExponentialBackoff) NextRetry(attempt int, _ error) time.Duration {
	initial := b.InitialInterval
	if initial <= 0 {
		initial = time.Second
	}
	multiplier := b.Multiplier
	if multiplier <= 0 {
		multiplier = 2
	}
	maxInterval := b.MaxInterval
	if maxInterval <= 0 {
		maxInterval = time.Minute
	}
	if attempt < 0 {
		attempt = 0
	}

	delay := initial
	for i := 0; i < attempt; i++ {
		next := time.Duration(float64(delay) * multiplier)
		if next <= 0 || next > maxInterval {
			delay = maxInterval
			break
		}
		delay = next
	}
	if delay > maxInterval {
		delay = maxInterval
	}

	jitterFraction := b.JitterFraction
	if jitterFraction <= 0 {
		return delay
	}
	if jitterFraction > 1 {
		jitterFraction = 1
	}
	jitter := time.Duration(float64(delay) * jitterFraction * (rand.Float64()*2 - 1))
	delay += jitter
	if delay < 0 {
		return 0
	}
	return delay
}
