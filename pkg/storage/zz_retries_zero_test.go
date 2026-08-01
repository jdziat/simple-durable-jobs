package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// An explicit "do not retry" has to survive persistence.
//
// core.Job.MaxRetries used to carry a gorm `default:3` tag, and GORM SUBSTITUTES a
// declared default for any zero value — Select() does not override it. So a job
// enqueued with Retries(0) was stored with max_retries = 3 and a handler its author
// marked do-not-retry ran three times. Everything above this layer was already
// correct: queue.Options tracks retriesSet, and fanout checks !RetriesSet
// specifically so an intentional 0 is not treated as "unset". All of it was inert.
func TestEnqueuePreservesAnExplicitZeroRetries(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))

	job := &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "default",
		Status: core.StatusPending, MaxRetries: 0,
	}
	require.NoError(t, s.Enqueue(context.Background(), job))

	// Read the raw column: the model read would apply its own conversions.
	var raw int
	require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", job.ID).Scan(&raw).Error)
	require.Equal(t, 0, raw,
		"an explicit Retries(0) must persist as 0; stored as 3, a non-idempotent handler "+
			"the author marked do-not-retry runs three times")

	// A non-zero value must still round-trip, so the fix is not just "always 0".
	other := &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "default",
		Status: core.StatusPending, MaxRetries: 7,
	}
	require.NoError(t, s.Enqueue(context.Background(), other))
	require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", other.ID).Scan(&raw).Error)
	require.Equal(t, 7, raw)
}
