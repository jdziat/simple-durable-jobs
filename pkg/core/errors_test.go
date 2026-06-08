package core

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestPauseErrors(t *testing.T) {
	assert.Error(t, ErrJobAlreadyPaused)
	assert.Error(t, ErrJobNotPaused)
	assert.Error(t, ErrQueueAlreadyPaused)
	assert.Error(t, ErrQueueNotPaused)
	assert.Error(t, ErrCannotPauseStatus)
}

func TestErrJobNotCompleted_Defined(t *testing.T) {
	require.Error(t, ErrJobNotCompleted)
	assert.Contains(t, ErrJobNotCompleted.Error(), "not completed")
}

func TestErrJobNotFound_IsMatchable(t *testing.T) {
	wrapped := fmt.Errorf("%w: abc", ErrJobNotFound)

	assert.True(t, errors.Is(wrapped, ErrJobNotFound))
	assert.Equal(t, ErrJobNotFound, SentinelErrorByMessage(ErrJobNotFound.Error()))
}

func TestErrJobFailedCancelled_Matchable(t *testing.T) {
	wrappedFailed := fmt.Errorf("%w: boom", ErrJobFailed)
	assert.True(t, errors.Is(wrappedFailed, ErrJobFailed))
	assert.False(t, errors.Is(wrappedFailed, ErrJobNotCompleted))
	assert.Equal(t, ErrJobFailed, SentinelErrorByMessage(ErrJobFailed.Error()))

	wrappedCancelled := fmt.Errorf("%w: job-123", ErrJobCancelled)
	assert.True(t, errors.Is(wrappedCancelled, ErrJobCancelled))
	assert.False(t, errors.Is(wrappedCancelled, ErrJobNotCompleted))
	assert.Equal(t, ErrJobCancelled, SentinelErrorByMessage(ErrJobCancelled.Error()))
}

func TestRehydrate_NoSentinelSwapForPlainUserError(t *testing.T) {
	user := errors.New(ErrDuplicateJob.Error())
	assert.False(t, errors.Is(user, ErrDuplicateJob))

	kind, delay := CheckpointErrorKind(user)
	assert.Empty(t, kind)
	assert.NotEqual(t, CheckpointErrorKindSentinel, kind)

	replayed := RehydrateCheckpointError(user.Error(), kind, delay)
	assert.False(t, errors.Is(replayed, ErrDuplicateJob))
	assert.Equal(t, user.Error(), replayed.Error())
}

func TestRehydrate_GenuineSentinelRoundTrips(t *testing.T) {
	kind, delay := CheckpointErrorKind(ErrDuplicateJob)
	assert.Equal(t, CheckpointErrorKindSentinel, kind)

	replayed := RehydrateCheckpointError(ErrDuplicateJob.Error(), kind, delay)
	assert.True(t, errors.Is(replayed, ErrDuplicateJob))
}

func TestRehydrate_NoRetryAndRetryAfterRoundTrip(t *testing.T) {
	noRetry := NoRetry(errors.New("boom"))
	kind, delay := CheckpointErrorKind(noRetry)
	assert.Equal(t, CheckpointErrorKindNoRetry, kind)
	assert.Zero(t, delay)

	replayedNoRetry := RehydrateCheckpointError(noRetry.Error(), kind, delay)
	var noRetryErr *NoRetryError
	require.True(t, errors.As(replayedNoRetry, &noRetryErr))
	assert.Contains(t, noRetryErr.Unwrap().Error(), "boom")

	retryAfter := RetryAfter(5*time.Second, errors.New("rl"))
	kind, delay = CheckpointErrorKind(retryAfter)
	assert.Equal(t, CheckpointErrorKindRetryAfter, kind)
	assert.Equal(t, 5*time.Second, delay)

	replayedRetryAfter := RehydrateCheckpointError(retryAfter.Error(), kind, delay)
	var retryAfterErr *RetryAfterError
	require.True(t, errors.As(replayedRetryAfter, &retryAfterErr))
	assert.Equal(t, 5*time.Second, retryAfterErr.Delay)
	assert.Contains(t, retryAfterErr.Unwrap().Error(), "rl")
}

func TestRehydrate_ReplayIdempotent(t *testing.T) {
	e := NoRetry(errors.New("x"))

	kind1, delay1 := CheckpointErrorKind(e)
	r1 := RehydrateCheckpointError(e.Error(), kind1, delay1)

	kind2, delay2 := CheckpointErrorKind(r1)
	r2 := RehydrateCheckpointError(r1.Error(), kind2, delay2)

	assert.Equal(t, r1.Error(), r2.Error())
	assert.Equal(t, errors.Is(r1, ErrDuplicateJob), errors.Is(r2, ErrDuplicateJob))

	var noRetry1 *NoRetryError
	var noRetry2 *NoRetryError
	require.True(t, errors.As(r1, &noRetry1))
	require.True(t, errors.As(r2, &noRetry2))
}
