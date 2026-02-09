package jobs_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestNoRetry_WrapsError(t *testing.T) {
	original := errors.New("invalid input")
	wrapped := jobs.NoRetry(original)

	var noRetryErr *jobs.NoRetryError
	assert.True(t, errors.As(wrapped, &noRetryErr))
	assert.Equal(t, original, noRetryErr.Unwrap())
}

func TestRetryAfter_HasDelay(t *testing.T) {
	original := errors.New("rate limited")
	wrapped := jobs.RetryAfter(30*time.Second, original)

	var retryErr *jobs.RetryAfterError
	assert.True(t, errors.As(wrapped, &retryErr))
	assert.Equal(t, 30*time.Second, retryErr.Delay)
}
