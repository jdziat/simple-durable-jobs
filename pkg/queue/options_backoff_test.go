package queue

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
)

// stubBackoff is a minimal core.BackoffPolicy for option-wiring tests.
type stubBackoff struct{ d time.Duration }

func (s stubBackoff) NextRetry(attempt int, err error) time.Duration { return s.d }

func TestWithHandlerBackoff_SetsPolicy(t *testing.T) {
	opts := NewOptions()
	assert.Nil(t, opts.Backoff, "no backoff policy by default")

	policy := stubBackoff{d: 250 * time.Millisecond}
	WithHandlerBackoff(policy).Apply(opts)

	if assert.NotNil(t, opts.Backoff) {
		assert.Equal(t, 250*time.Millisecond, opts.Backoff.NextRetry(1, nil))
	}
	var _ core.BackoffPolicy = opts.Backoff
}
