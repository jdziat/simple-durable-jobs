package worker

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkerStartFailsOnInvalidRateLimit (PKT-13 / codex A-04) proves an invalid
// RateLimit option fails Start loudly instead of being silently dropped (leaving
// the operator believing a cap is active).
func TestWorkerStartFailsOnInvalidRateLimit(t *testing.T) {
	t.Run("empty name", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), RateLimit("", 5))
		err := w.Start(context.Background())
		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid RateLimit option")
		assert.ErrorContains(t, err, "name must be non-empty")
	})
	t.Run("non-positive rate", func(t *testing.T) {
		w := NewWorker(queue.New(&mockStorage{}), RateLimit("llm", 0))
		err := w.Start(context.Background())
		require.Error(t, err)
		assert.ErrorContains(t, err, "rate must be positive")
	})
	t.Run("valid rate limit does not error on this check", func(t *testing.T) {
		// A valid RateLimit records no option error; Start may still fail later on
		// the storage capability check, but NOT on invalid-option grounds.
		w := NewWorker(queue.New(&mockStorage{}), RateLimit("llm", 5))
		err := w.Start(context.Background())
		require.Error(t, err) // mockStorage lacks the rate-limit capability
		assert.NotContains(t, err.Error(), "invalid RateLimit option")
	})
}
