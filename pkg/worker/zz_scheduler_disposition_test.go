package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"

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

// The three scheduled-fire dispositions had NO coverage: only the pure backoff
// helper was tested, so replacing the dedup case with "genuine failure" or
// dropping the never-fires guard left the whole suite green. The distinction is
// the entire point of the change — whether a blocked boundary is skipped ONCE or
// retried at 10 Hz for the runtime of the previous instance.
//
// FALSE-GREEN TRAP: asserting only that no panic occurs, or only on the backoff
// arithmetic, cannot see any of this. The discriminating observations are which
// of the three maps move: lastRun advancing is what stops the boundary being
// re-attempted, and retryAt being set is what stops the hot loop.
func TestApplyScheduleFireDisposition(t *testing.T) {
	boundary := time.Date(2026, 7, 27, 9, 0, 0, 0, time.UTC)
	now := boundary.Add(time.Second)

	newOutcome := func(err error) scheduleFireOutcome {
		return scheduleFireOutcome{
			name: "s", nextRun: boundary, now: now, err: err,
			failures: map[string]int{}, retryAt: map[string]time.Time{}, lastRun: map[string]time.Time{},
		}
	}
	w := NewWorker(queue.New(&mockStorage{}))

	t.Run("a unique-key dedup is a skip: the cursor advances", func(t *testing.T) {
		o := newOutcome(core.ErrDuplicateJob)
		w.applyScheduleFireDisposition(o)

		assert.Equal(t, boundary, o.lastRun["s"],
			"a deliberate skip must ADVANCE the cursor — not advancing is what retried the "+
				"boundary every 100ms tick for the whole runtime of the previous instance")
		assert.NotContains(t, o.retryAt, "s", "a skip is not a failure, so it schedules no backoff")
		assert.NotContains(t, o.failures, "s")
	})

	t.Run("a genuine failure retries the same boundary with backoff", func(t *testing.T) {
		o := newOutcome(errors.New("database fell over"))
		w.applyScheduleFireDisposition(o)

		assert.NotContains(t, o.lastRun, "s",
			"a genuine failure must NOT advance the cursor: the claim rolled back, so the "+
				"boundary must be retried rather than silently dropped")
		assert.Equal(t, 1, o.failures["s"])
		assert.True(t, o.retryAt["s"].After(now), "and it must be deferred, not retried at 10 Hz")
	})

	t.Run("success advances the cursor and clears any backoff", func(t *testing.T) {
		o := newOutcome(nil)
		o.failures["s"] = 3
		o.retryAt["s"] = now.Add(time.Hour)
		w.applyScheduleFireDisposition(o)

		assert.Equal(t, boundary, o.lastRun["s"])
		assert.NotContains(t, o.failures, "s", "a success must clear the failure streak")
		assert.NotContains(t, o.retryAt, "s")
	})

	t.Run("consecutive failures back off further each time", func(t *testing.T) {
		o := newOutcome(errors.New("still down"))
		w.applyScheduleFireDisposition(o)
		first := o.retryAt["s"]
		o.err = errors.New("still down")
		w.applyScheduleFireDisposition(o)

		assert.Equal(t, 2, o.failures["s"])
		assert.True(t, o.retryAt["s"].After(first),
			"the backoff must grow, or a persistent failure costs a transaction and an ERROR "+
				"log every tick")
	})
}
