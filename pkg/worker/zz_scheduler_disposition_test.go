package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The scheduler had exactly ONE disposition for a non-success outcome: log ERROR,
// do not advance the cursor, retry in 100ms. That is right for a transient DB
// failure and wrong for the two NON-transient outcomes that actually reach it —
// a unique-key dedup (a deliberate skip) and a schedule that can never fire.
//
// A Unique schedule blocked by its own still-running previous instance therefore
// cost one transaction and one ERROR log every tick, at 10Hz, for the entire
// runtime of that instance.
func TestScheduleFireRetryDelay_BacksOffAndSaturates(t *testing.T) {
	// The first retry is still one tick later, so a transient blip recovers as
	// fast as it did before the backoff existed.
	assert.Equal(t, scheduleFireRetryBase, scheduleFireRetryDelay(1))
	assert.Equal(t, scheduleFireRetryBase, scheduleFireRetryDelay(0),
		"a non-positive count must not produce a zero or negative delay")

	assert.Equal(t, 200*time.Millisecond, scheduleFireRetryDelay(2))
	assert.Equal(t, 400*time.Millisecond, scheduleFireRetryDelay(3))

	// Saturates rather than growing without bound: a schedule must resume
	// promptly once the database recovers.
	assert.Equal(t, scheduleFireRetryMax, scheduleFireRetryDelay(50))
	assert.Equal(t, scheduleFireRetryMax, scheduleFireRetryDelay(1000))

	// Monotonic up to the cap — the property that actually bounds the log/txn
	// rate. Asserting only the first and last values would miss a regression in
	// the middle.
	prev := time.Duration(0)
	for i := 1; i <= 20; i++ {
		d := scheduleFireRetryDelay(i)
		assert.GreaterOrEqual(t, d, prev, "delay must never decrease (failure %d)", i)
		assert.LessOrEqual(t, d, scheduleFireRetryMax, "delay must never exceed the cap (failure %d)", i)
		prev = d
	}
}
