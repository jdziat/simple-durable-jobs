package core

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// SentinelErrorByMessage — exercise every branch: each known sentinel matches
// by its message, and an unknown message returns nil.
// ---------------------------------------------------------------------------

func TestSentinelErrorByMessage_AllSentinels(t *testing.T) {
	sentinels := []error{
		ErrInvalidJobTypeName,
		ErrJobTypeNameTooLong,
		ErrInvalidQueueName,
		ErrQueueNameTooLong,
		ErrJobArgsTooLarge,
		ErrJobNotOwned,
		ErrDuplicateJob,
		ErrUniqueKeyTooLong,
		ErrJobAlreadyPaused,
		ErrJobNotPaused,
		ErrQueueAlreadyPaused,
		ErrQueueNotPaused,
		ErrCannotPauseStatus,
		ErrJobNotCompleted,
		ErrNoResult,
	}

	for _, want := range sentinels {
		got := SentinelErrorByMessage(want.Error())
		require.NotNil(t, got, "message %q should resolve to a sentinel", want.Error())
		// Identity: should return the exact same sentinel value.
		assert.True(t, errors.Is(got, want), "resolved sentinel should match original")
		assert.Equal(t, want.Error(), got.Error())
	}
}

func TestSentinelErrorByMessage_Unknown(t *testing.T) {
	assert.Nil(t, SentinelErrorByMessage("this is not a known sentinel message"))
	assert.Nil(t, SentinelErrorByMessage(""))
}

// ---------------------------------------------------------------------------
// CheckpointErrorKind — exercise all four return paths.
// ---------------------------------------------------------------------------

func TestCheckpointErrorKind_NoRetry(t *testing.T) {
	kind, delay := CheckpointErrorKind(NoRetry(errors.New("boom")))
	assert.Equal(t, CheckpointErrorKindNoRetry, kind)
	assert.Equal(t, time.Duration(0), delay)
}

func TestCheckpointErrorKind_RetryAfter(t *testing.T) {
	d := 7 * time.Second
	kind, delay := CheckpointErrorKind(RetryAfter(d, errors.New("transient")))
	assert.Equal(t, CheckpointErrorKindRetryAfter, kind)
	assert.Equal(t, d, delay)
}

func TestCheckpointErrorKind_NoRetryWins(t *testing.T) {
	// NoRetry is checked before RetryAfter; a NoRetry wrapping should resolve
	// to the no_retry kind even if some other error is nested inside.
	kind, delay := CheckpointErrorKind(NoRetry(ErrJobNotOwned))
	assert.Equal(t, CheckpointErrorKindNoRetry, kind)
	assert.Equal(t, time.Duration(0), delay)
}

func TestCheckpointErrorKind_Sentinel(t *testing.T) {
	kind, delay := CheckpointErrorKind(ErrDuplicateJob)
	assert.Equal(t, CheckpointErrorKindSentinel, kind)
	assert.Equal(t, time.Duration(0), delay)
}

func TestCheckpointErrorKind_Unknown(t *testing.T) {
	kind, delay := CheckpointErrorKind(errors.New("some arbitrary non-sentinel error"))
	assert.Equal(t, "", kind)
	assert.Equal(t, time.Duration(0), delay)
}

// ---------------------------------------------------------------------------
// RehydrateCheckpointError — round-trip every kind and the default path.
// ---------------------------------------------------------------------------

func TestRehydrateCheckpointError_NoRetry_SentinelCause(t *testing.T) {
	orig := NoRetry(ErrJobNotOwned)
	kind, delay := CheckpointErrorKind(orig)
	require.Equal(t, CheckpointErrorKindNoRetry, kind)

	rehydrated := RehydrateCheckpointError(orig.Error(), kind, delay)

	var noRetry *NoRetryError
	require.True(t, errors.As(rehydrated, &noRetry))
	// Wrapper kinds reconstruct plain causes instead of guessing sentinel identity from message text.
	assert.False(t, errors.Is(rehydrated, ErrJobNotOwned))
	assert.Equal(t, ErrJobNotOwned.Error(), noRetry.Unwrap().Error())
	assert.Equal(t, orig.Error(), rehydrated.Error())
}

func TestRehydrateCheckpointError_NoRetry_PlainCause(t *testing.T) {
	orig := NoRetry(errors.New("custom permanent failure"))
	rehydrated := RehydrateCheckpointError(orig.Error(), CheckpointErrorKindNoRetry, 0)

	var noRetry *NoRetryError
	require.True(t, errors.As(rehydrated, &noRetry))
	assert.Equal(t, orig.Error(), rehydrated.Error())
	assert.Equal(t, "custom permanent failure", noRetry.Unwrap().Error())
}

func TestRehydrateCheckpointError_RetryAfter_SentinelCause(t *testing.T) {
	d := 3 * time.Minute
	orig := RetryAfter(d, ErrNoResult)
	kind, delay := CheckpointErrorKind(orig)
	require.Equal(t, CheckpointErrorKindRetryAfter, kind)
	require.Equal(t, d, delay)

	rehydrated := RehydrateCheckpointError(orig.Error(), kind, delay)

	var retryAfter *RetryAfterError
	require.True(t, errors.As(rehydrated, &retryAfter))
	assert.Equal(t, d, retryAfter.Delay)
	assert.False(t, errors.Is(rehydrated, ErrNoResult))
	assert.Equal(t, ErrNoResult.Error(), retryAfter.Unwrap().Error())
	assert.Equal(t, orig.Error(), rehydrated.Error())
}

func TestRehydrateCheckpointError_RetryAfter_PlainCause(t *testing.T) {
	d := 250 * time.Millisecond
	orig := RetryAfter(d, errors.New("rate limited"))
	rehydrated := RehydrateCheckpointError(orig.Error(), CheckpointErrorKindRetryAfter, d)

	var retryAfter *RetryAfterError
	require.True(t, errors.As(rehydrated, &retryAfter))
	assert.Equal(t, d, retryAfter.Delay)
	assert.Equal(t, "rate limited", retryAfter.Unwrap().Error())
	assert.Equal(t, orig.Error(), rehydrated.Error())
}

func TestRehydrateCheckpointError_Sentinel(t *testing.T) {
	rehydrated := RehydrateCheckpointError(ErrCannotPauseStatus.Error(), CheckpointErrorKindSentinel, 0)
	assert.True(t, errors.Is(rehydrated, ErrCannotPauseStatus))
	assert.Equal(t, ErrCannotPauseStatus.Error(), rehydrated.Error())
}

func TestRehydrateCheckpointError_DefaultUnknownKind_PlainMessage(t *testing.T) {
	// Unknown kind falls through to default: returns the (plain) base error.
	rehydrated := RehydrateCheckpointError("totally unknown failure", "", 0)
	require.Error(t, rehydrated)
	assert.Equal(t, "totally unknown failure", rehydrated.Error())

	// Not a NoRetry / RetryAfter wrapper.
	var noRetry *NoRetryError
	var retryAfter *RetryAfterError
	assert.False(t, errors.As(rehydrated, &noRetry))
	assert.False(t, errors.As(rehydrated, &retryAfter))
}

func TestRehydrateCheckpointError_DefaultKind_SentinelMessage(t *testing.T) {
	// Default path with a message that matches a sentinel remains a plain error.
	rehydrated := RehydrateCheckpointError(ErrDuplicateJob.Error(), "", 0)
	assert.False(t, errors.Is(rehydrated, ErrDuplicateJob))
	assert.Equal(t, ErrDuplicateJob.Error(), rehydrated.Error())
}

func TestRehydrateCheckpointError_FullRoundTripMatrix(t *testing.T) {
	cases := []error{
		NoRetry(ErrInvalidQueueName),
		NoRetry(errors.New("opaque cause")),
		RetryAfter(2*time.Second, ErrQueueNotPaused),
		RetryAfter(0, errors.New("zero delay cause")),
		ErrJobAlreadyPaused, // sentinel, no wrapper
	}

	for i, orig := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			kind, delay := CheckpointErrorKind(orig)
			rehydrated := RehydrateCheckpointError(orig.Error(), kind, delay)
			require.Error(t, rehydrated)
			// Message must survive the round trip exactly.
			assert.Equal(t, orig.Error(), rehydrated.Error())
		})
	}
}
