package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestDeleteConsumedSignalsOlderThanIsBounded pins the IN-list bound on the third
// retention sweep. RetentionBatchSize is clamped, but this method is EXPORTED, so
// a direct caller can pass any limit; unbounded, it exceeds the driver's
// bind-parameter ceiling and returns deleted=0 on every pass forever, which is
// precisely the silent-retention-death the clamp exists to prevent.
func TestDeleteConsumedSignalsOlderThanIsBounded(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const n = 40000 // above SQLite's ~32k variable ceiling
	old := time.Now().UTC().Add(-2 * time.Hour)

	// signals.job_id carries a FOREIGN KEY to jobs on Postgres and MySQL. SQLite
	// does not enforce it, so seeding bare signals passes locally and fails on
	// both real backends — seed a real owner and point every signal at it.
	owner := &core.Job{
		ID: core.NewID(), Type: "owner", Queue: "default", Status: core.StatusCompleted,
	}
	require.NoError(t, s.Enqueue(ctx, owner))

	rows := make([]core.Signal, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, core.Signal{
			ID: core.NewID(), JobID: owner.ID, Name: "sig", ConsumedAt: &old, CreatedAt: old,
		})
	}
	for i := 0; i < len(rows); i += 1000 {
		end := i + 1000
		if end > len(rows) {
			end = len(rows)
		}
		require.NoError(t, s.db.Create(rows[i:end]).Error)
	}

	deleted, err := s.DeleteConsumedSignalsOlderThan(ctx, time.Hour, n)
	require.NoErrorf(t, err, "an oversized limit must not exceed the driver's bind-parameter ceiling")
	require.Equalf(t, int64(n), deleted, "the sweep must drain the backlog, not stall at zero")
}
