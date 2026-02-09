package core

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoRetryError(t *testing.T) {
	originalErr := errors.New("permanent failure")
	wrapped := NoRetry(originalErr)

	var noRetryErr *NoRetryError
	assert.True(t, errors.As(wrapped, &noRetryErr))
	assert.Equal(t, originalErr, noRetryErr.Unwrap())
	assert.Contains(t, noRetryErr.Error(), "no retry")
	assert.Contains(t, noRetryErr.Error(), "permanent failure")
}

func TestRetryAfterError(t *testing.T) {
	originalErr := errors.New("temporary failure")
	delay := 5 * time.Second
	wrapped := RetryAfter(delay, originalErr)

	var retryErr *RetryAfterError
	assert.True(t, errors.As(wrapped, &retryErr))
	assert.Equal(t, originalErr, retryErr.Unwrap())
	assert.Equal(t, delay, retryErr.Delay)
	assert.Contains(t, retryErr.Error(), "retry after")
	assert.Contains(t, retryErr.Error(), "5s")
}

func TestErrorVariables(t *testing.T) {
	// Verify all error variables are defined
	assert.NotNil(t, ErrInvalidJobTypeName)
	assert.NotNil(t, ErrJobTypeNameTooLong)
	assert.NotNil(t, ErrInvalidQueueName)
	assert.NotNil(t, ErrQueueNameTooLong)
	assert.NotNil(t, ErrJobArgsTooLarge)
	assert.NotNil(t, ErrJobNotOwned)
	assert.NotNil(t, ErrDuplicateJob)

	// Verify error messages
	assert.Contains(t, ErrInvalidJobTypeName.Error(), "invalid job type name")
	assert.Contains(t, ErrJobNotOwned.Error(), "not owned")
	assert.Contains(t, ErrDuplicateJob.Error(), "duplicate")
}
