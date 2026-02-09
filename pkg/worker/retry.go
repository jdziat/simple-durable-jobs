package worker

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// RetryConfig holds configuration for retry with backoff.
type RetryConfig struct {
	// MaxAttempts is the maximum number of retry attempts (including initial).
	// Default: 5
	MaxAttempts int

	// InitialBackoff is the initial backoff duration.
	// Default: 100ms
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration.
	// Default: 5s
	MaxBackoff time.Duration

	// BackoffMultiplier is the multiplier applied to backoff after each attempt.
	// Default: 2.0
	BackoffMultiplier float64

	// JitterFraction is the fraction of backoff to randomize (0.0 to 1.0).
	// Default: 0.1 (10% jitter)
	JitterFraction float64
}

// DefaultRetryConfig returns the default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.1,
	}
}

// retryWithBackoff executes the operation with exponential backoff on failure.
// It respects context cancellation and returns the last error if all attempts fail.
func retryWithBackoff(ctx context.Context, config RetryConfig, operation func() error) error {
	var lastErr error
	backoff := config.InitialBackoff

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		// Don't retry on context cancellation
		if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
			return lastErr
		}

		// Check if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			break
		}

		// Calculate backoff with jitter
		jitter := time.Duration(float64(backoff) * config.JitterFraction * (rand.Float64()*2 - 1))
		sleepDuration := backoff + jitter
		if sleepDuration < 0 {
			sleepDuration = backoff
		}

		// Wait for backoff or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
		}

		// Increase backoff for next attempt
		backoff = time.Duration(float64(backoff) * config.BackoffMultiplier)
		if backoff > config.MaxBackoff {
			backoff = config.MaxBackoff
		}
	}

	return lastErr
}

// IsRetryableError determines if an error is worth retrying.
// Returns false for errors that indicate permanent failures.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Most database errors are potentially transient and worth retrying:
	// - Connection errors
	// - Timeout errors
	// - Lock wait timeout
	// - Deadlock errors
	// We default to retrying unless we know it's permanent.
	return true
}
