package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// Retries(0) means RUN ONCE, and it has to survive the whole way to the column.
//
// The storage layer cannot infer that from the value: an int field left alone is
// also 0, and reading that as "do not retry" turns retries off for every
// application that enqueues a core.Job directly without mentioning the field.
// queue.Options has always tracked the difference internally as retriesSet; this
// pins that the intent now actually reaches the row, rather than being recorded
// and dropped one layer above the INSERT.
//
// Both directions, because a fix for either one alone is a fix for neither:
// Retries(0) storing 3 is a non-idempotent handler running three times, and an
// omitted value storing 0 is a job that dead-letters on its first transient
// failure instead of surviving it.
func TestEnqueueCarriesRetryIntentToTheColumn(t *testing.T) {
	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t)
	q := New(store)
	q.Register("charge", func(context.Context, string) error { return nil })

	storedRetries := func(t *testing.T, id core.UUID) int {
		t.Helper()
		var raw int
		// The raw column, not the model: a model read applies its own conversions
		// and would hide a value the database actually holds.
		require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", id).Scan(&raw).Error)
		return raw
	}

	t.Run("an explicit Retries(0) persists as zero", func(t *testing.T) {
		id, err := q.Enqueue(ctx, "charge", "pay_zero", Retries(0))
		require.NoError(t, err)
		require.Equal(t, 0, storedRetries(t, id),
			"jobs.Retries(0) marks a handler do-not-retry; stored as 3 it runs three times")
	})

	t.Run("no Retries option keeps the Go-layer default", func(t *testing.T) {
		id, err := q.Enqueue(ctx, "charge", "pay_default")
		require.NoError(t, err)
		require.Equal(t, DefaultJobRetries, storedRetries(t, id),
			"omitting Retries must keep the documented default, not collapse to zero")
	})

	t.Run("a non-zero Retries round-trips", func(t *testing.T) {
		id, err := q.Enqueue(ctx, "charge", "pay_seven", Retries(7))
		require.NoError(t, err)
		require.Equal(t, 7, storedRetries(t, id))
	})
}
