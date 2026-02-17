package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()

	assert.Equal(t, 5, cfg.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, cfg.InitialBackoff)
	assert.Equal(t, 5*time.Second, cfg.MaxBackoff)
	assert.Equal(t, 2.0, cfg.BackoffMultiplier)
	assert.Equal(t, 0.1, cfg.JitterFraction)
}

func TestRetryWithBackoff_SuccessOnFirstAttempt(t *testing.T) {
	cfg := DefaultRetryConfig()
	var attempts int

	err := retryWithBackoff(context.Background(), cfg, func() error {
		attempts++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetryWithBackoff_SuccessAfterRetries(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.0, // No jitter for predictable testing
	}
	var attempts int

	err := retryWithBackoff(context.Background(), cfg, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("transient error")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestRetryWithBackoff_ExhaustsAttempts(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        50 * time.Millisecond,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.0,
	}
	var attempts int
	expectedErr := errors.New("persistent error")

	err := retryWithBackoff(context.Background(), cfg, func() error {
		attempts++
		return expectedErr
	})

	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 3, attempts)
}

func TestRetryWithBackoff_RespectsContextCancellation(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       10,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := retryWithBackoff(ctx, cfg, func() error {
		attempts.Add(1)
		return errors.New("keep failing")
	})

	assert.ErrorIs(t, err, context.Canceled)
	// Should have attempted at least once before cancellation
	assert.GreaterOrEqual(t, attempts.Load(), int32(1))
}

func TestRetryWithBackoff_StopsOnContextError(t *testing.T) {
	cfg := DefaultRetryConfig()
	var attempts int

	err := retryWithBackoff(context.Background(), cfg, func() error {
		attempts++
		return context.Canceled
	})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, attempts) // Should not retry on context errors
}

func TestRetryWithBackoff_BackoffGrowsExponentially(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       4,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.0,
	}

	var timestamps []time.Time
	err := retryWithBackoff(context.Background(), cfg, func() error {
		timestamps = append(timestamps, time.Now())
		return errors.New("fail")
	})

	assert.Error(t, err)
	require.Len(t, timestamps, 4)

	// Check backoff intervals are increasing
	// Interval 1: ~10ms, Interval 2: ~20ms, Interval 3: ~40ms
	interval1 := timestamps[1].Sub(timestamps[0])
	interval2 := timestamps[2].Sub(timestamps[1])
	interval3 := timestamps[3].Sub(timestamps[2])

	assert.Greater(t, interval2, interval1, "second interval should be longer than first")
	assert.Greater(t, interval3, interval2, "third interval should be longer than second")
}

func TestRetryWithBackoff_RespectsMaxBackoff(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        60 * time.Millisecond, // Very low max
		BackoffMultiplier: 10.0,                  // Aggressive multiplier
		JitterFraction:    0.0,
	}

	var timestamps []time.Time
	err := retryWithBackoff(context.Background(), cfg, func() error {
		timestamps = append(timestamps, time.Now())
		return errors.New("fail")
	})

	assert.Error(t, err)
	require.Len(t, timestamps, 5)

	// Later intervals should be capped at MaxBackoff
	// Allow some tolerance for timing
	for i := 2; i < len(timestamps); i++ {
		interval := timestamps[i].Sub(timestamps[i-1])
		assert.LessOrEqual(t, interval, 100*time.Millisecond, "interval should be capped near MaxBackoff")
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"context.Canceled", context.Canceled, false},
		{"context.DeadlineExceeded", context.DeadlineExceeded, false},
		{"generic error", errors.New("some error"), true},
		{"wrapped context.Canceled", errors.New("wrapped: " + context.Canceled.Error()), true}, // Not using errors.Is
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsRetryableError(tt.err))
		})
	}
}

func TestWithStorageRetry_Option(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:    10,
		InitialBackoff: 200 * time.Millisecond,
	}

	workerCfg := WorkerConfig{}
	WithStorageRetry(cfg).ApplyWorker(&workerCfg)

	require.NotNil(t, workerCfg.StorageRetry)
	assert.Equal(t, 10, workerCfg.StorageRetry.MaxAttempts)
	assert.Equal(t, 200*time.Millisecond, workerCfg.StorageRetry.InitialBackoff)
}

func TestWithDequeueRetry_Option(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Second,
	}

	workerCfg := WorkerConfig{}
	WithDequeueRetry(cfg).ApplyWorker(&workerCfg)

	require.NotNil(t, workerCfg.DequeueRetry)
	assert.Equal(t, 3, workerCfg.DequeueRetry.MaxAttempts)
	assert.Equal(t, 1*time.Second, workerCfg.DequeueRetry.InitialBackoff)
}

func TestWithRetryAttempts_Option(t *testing.T) {
	workerCfg := WorkerConfig{}
	WithRetryAttempts(7).ApplyWorker(&workerCfg)

	require.NotNil(t, workerCfg.StorageRetry)
	assert.Equal(t, 7, workerCfg.StorageRetry.MaxAttempts)
	// Should have default values for other fields
	assert.Equal(t, 100*time.Millisecond, workerCfg.StorageRetry.InitialBackoff)
}

func TestDisableRetry_Option(t *testing.T) {
	workerCfg := WorkerConfig{}
	DisableRetry().ApplyWorker(&workerCfg)

	require.NotNil(t, workerCfg.StorageRetry)
	require.NotNil(t, workerCfg.DequeueRetry)
	assert.Equal(t, 1, workerCfg.StorageRetry.MaxAttempts)
	assert.Equal(t, 1, workerCfg.DequeueRetry.MaxAttempts)
}
