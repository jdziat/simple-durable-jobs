package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewOptions_Defaults(t *testing.T) {
	opts := NewOptions()

	assert.Equal(t, "default", opts.Queue)
	assert.Equal(t, 0, opts.Priority)
	assert.Equal(t, DefaultJobRetries, opts.MaxRetries)
	assert.Zero(t, opts.Delay)
	assert.Nil(t, opts.RunAt)
	assert.Empty(t, opts.UniqueKey)
}

func TestQueueOpt(t *testing.T) {
	opts := NewOptions()
	QueueOpt("high-priority").Apply(opts)

	assert.Equal(t, "high-priority", opts.Queue)
}

func TestPriority(t *testing.T) {
	opts := NewOptions()
	Priority(10).Apply(opts)

	assert.Equal(t, 10, opts.Priority)
}

func TestRetries(t *testing.T) {
	opts := NewOptions()
	Retries(5).Apply(opts)

	assert.Equal(t, 5, opts.MaxRetries)
}

func TestRetries_Clamped(t *testing.T) {
	opts := NewOptions()
	Retries(1000).Apply(opts) // Should be clamped to 100

	assert.Equal(t, 100, opts.MaxRetries)
}

func TestDelay(t *testing.T) {
	opts := NewOptions()
	Delay(5 * time.Minute).Apply(opts)

	assert.Equal(t, 5*time.Minute, opts.Delay)
}

func TestAt(t *testing.T) {
	opts := NewOptions()
	runAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	At(runAt).Apply(opts)

	assert.NotNil(t, opts.RunAt)
	assert.Equal(t, runAt, *opts.RunAt)
}

func TestUnique(t *testing.T) {
	opts := NewOptions()
	Unique("user-123-email").Apply(opts)

	assert.Equal(t, "user-123-email", opts.UniqueKey)
}

func TestDeterminism(t *testing.T) {
	opts := NewOptions()
	Determinism(Strict).Apply(opts)

	assert.Equal(t, Strict, opts.Determinism)
}

func TestDeterminismModes(t *testing.T) {
	assert.Equal(t, DeterminismMode(0), ExplicitCheckpoints)
	assert.Equal(t, DeterminismMode(1), Strict)
	assert.Equal(t, DeterminismMode(2), BestEffort)
}

func TestMultipleOptions(t *testing.T) {
	opts := NewOptions()
	QueueOpt("emails").Apply(opts)
	Priority(5).Apply(opts)
	Retries(10).Apply(opts)

	assert.Equal(t, "emails", opts.Queue)
	assert.Equal(t, 5, opts.Priority)
	assert.Equal(t, 10, opts.MaxRetries)
}

func TestDefaultValues(t *testing.T) {
	assert.Equal(t, 2, DefaultJobRetries)
	assert.Equal(t, 3, DefaultCallRetries)
}
