package jobs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
)

// TestEnqueueBatchMiddlewareCannotMisconfigureTheSurvivor is the PUBLIC-API form
// of the reported defect: no hand-built job, no direct storage call, real database.
//
// Queue.EnqueueBatch collapses two entries sharing a Unique key by giving the
// loser the winner's id, then hands BOTH to storage. Enqueue middleware that
// rewrites UniqueKey un-collapses them at the storage layer — two distinct final
// keys, one shared primary key — so storage's key-based skip no longer applies and
// the primary key suppresses one row.
//
// The suppressed entry's options must NOT land on the surviving row. Before the
// fix the survivor, which passed no Retries option and should carry the queue
// default, came out with the DROPPED entry's Retries(0).
//
// Note what this test does NOT assert: that both entries persist. They cannot —
// they share a primary key, and that collapse happens in the queue before storage
// ever sees them. That is the documented consequence of rewriting UniqueKey on the
// batch path; see Queue.EnqueueBatch's godoc.
func TestEnqueueBatchMiddlewareCannotMisconfigureTheSurvivor(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)

	q.UseEnqueueMiddleware(func(ctx context.Context, job *jobs.Job, next func(context.Context, *jobs.Job) error) error {
		job.UniqueKey = job.UniqueKey + "-" + job.Type
		return next(ctx, job)
	})

	ids, err := q.EnqueueBatch(ctx, []jobs.BatchEntry{
		{Name: "survivor", Args: map[string]string{"k": "v"}, Options: []jobs.Option{jobs.Unique("shared")}},
		{Name: "dropped", Args: map[string]string{"k": "v"}, Options: []jobs.Option{jobs.Unique("shared"), jobs.Retries(0)}},
	})
	require.NoError(t, err)
	require.Len(t, ids, 2)

	got, err := store.GetJob(ctx, ids[0])
	require.NoError(t, err)
	require.NotNil(t, got, "the surviving id must name a live row")
	require.Equal(t, "survivor", got.Type, "the first entry is the one that persists")
	require.Equalf(t, 2, got.MaxRetries,
		"the dropped entry's Retries(0) was applied to the surviving job, which passed no "+
			"Retries option and must carry the queue default")
}
