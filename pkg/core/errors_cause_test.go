package core

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckpointErrorFields_CauseRoundTrip verifies that the explicit cause is
// captured and used to reconstruct the error without parsing the formatted
// message prefix (M1), across every supported kind.
func TestCheckpointErrorFields_CauseRoundTrip(t *testing.T) {
	t.Run("no_retry stores inner cause", func(t *testing.T) {
		orig := NoRetry(errors.New("downstream 500"))
		msg, cause, kind, delay := CheckpointErrorFields(orig)
		assert.Equal(t, CheckpointErrorKindNoRetry, kind)
		assert.Equal(t, "downstream 500", cause)
		assert.Zero(t, delay)

		rebuilt := RehydrateCheckpointErrorWithCause(msg, cause, kind, delay)
		var nr *NoRetryError
		require.ErrorAs(t, rebuilt, &nr)
		assert.Equal(t, "downstream 500", nr.Err.Error())
	})

	t.Run("retry_after stores inner cause and delay", func(t *testing.T) {
		orig := RetryAfter(3*time.Second, errors.New("rate limited"))
		msg, cause, kind, delay := CheckpointErrorFields(orig)
		assert.Equal(t, CheckpointErrorKindRetryAfter, kind)
		assert.Equal(t, "rate limited", cause)
		assert.Equal(t, 3*time.Second, delay)

		rebuilt := RehydrateCheckpointErrorWithCause(msg, cause, kind, delay)
		var ra *RetryAfterError
		require.ErrorAs(t, rebuilt, &ra)
		assert.Equal(t, "rate limited", ra.Err.Error())
		assert.Equal(t, 3*time.Second, ra.Delay)
	})

	t.Run("sentinel stores stable key and resolves by key", func(t *testing.T) {
		msg, cause, kind, _ := CheckpointErrorFields(ErrDuplicateJob)
		assert.Equal(t, CheckpointErrorKindSentinel, kind)
		assert.Equal(t, "ErrDuplicateJob", cause, "sentinel cause is the stable key, not the message")

		rebuilt := RehydrateCheckpointErrorWithCause(msg, cause, kind, 0)
		assert.ErrorIs(t, rebuilt, ErrDuplicateJob)
	})

	t.Run("sentinel resolves by key even if the message wording changes", func(t *testing.T) {
		// Simulate an old/garbled stored message: the stable key must still win.
		rebuilt := RehydrateCheckpointErrorWithCause("some stale wording", "ErrJobNotOwned", CheckpointErrorKindSentinel, 0)
		assert.ErrorIs(t, rebuilt, ErrJobNotOwned)
	})

	t.Run("plain error has no cause", func(t *testing.T) {
		msg, cause, kind, delay := CheckpointErrorFields(errors.New("boom"))
		assert.Empty(t, kind)
		assert.Empty(t, cause)
		assert.Zero(t, delay)
		assert.Equal(t, "boom", msg)
	})
}

// TestRehydrate_LegacyMessageFallback verifies that checkpoints written before
// the cause column existed (cause == "") still rehydrate via message parsing,
// preserving backward compatibility.
func TestRehydrate_LegacyMessageFallback(t *testing.T) {
	legacy := NoRetry(errors.New("legacy cause")).Error() // "no retry: legacy cause"
	rebuilt := RehydrateCheckpointErrorWithCause(legacy, "", CheckpointErrorKindNoRetry, 0)
	var nr *NoRetryError
	require.ErrorAs(t, rebuilt, &nr)
	assert.Equal(t, "legacy cause", nr.Err.Error())
}
