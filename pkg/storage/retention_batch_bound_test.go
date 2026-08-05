package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestRetention_LargeLimitDoesNotExceedBindParameterCeiling pins the IN-list
// bound. DeleteTerminalJobsOlderThan plucks up to `limit` ids and then deletes by
// literal id list; without chunking, a limit above the driver's bind-parameter
// ceiling (SQLite ~32k, Postgres 65535) makes EVERY pass fail with "too many SQL
// variables" and deleted=0, so the terminal-row sweep is permanently dead exactly
// during the backlog the operator raised the batch size for.
func TestRetention_LargeLimitDoesNotExceedBindParameterCeiling(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	const n = 40000
	completed := time.Now().Add(-2 * time.Hour).UTC()
	rows := make([]*core.Job, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, &core.Job{
			ID: core.NewID(), Type: fmt.Sprintf("t%d", i), Queue: "q",
			Status: core.StatusCompleted, Args: []byte(`{}`),
			CompletedAt: &completed,
		})
	}
	require.NoError(t, s.db.CreateInBatches(rows, 500).Error)

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, n)
	require.NoError(t, err, "retention sweep must survive a batch size above the bind-parameter ceiling")
	require.Equal(t, int64(n), deleted)

	var left int64
	require.NoError(t, s.db.Model(&core.Job{}).Count(&left).Error)
	require.Equal(t, int64(0), left)
}

// TestUniqueLockSweep_LargeLimitDoesNotExceedBindParameterCeiling pins the same
// bound on the expired-window sweep. It matters more now that a LIVE window pins
// its job row: if this sweep dies, unique_locks grows forever on its own account.
func TestUniqueLockSweep_LargeLimitDoesNotExceedBindParameterCeiling(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	const n = 40000
	expired := time.Now().Add(-time.Hour).UTC()
	rows := make([]*core.UniqueLock, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, &core.UniqueLock{
			ScopeHash: scope64(fmt.Sprintf("sweep-%d-", i)), JobID: core.NewID(),
			ExpiresAt: expired,
		})
	}
	require.NoError(t, s.db.CreateInBatches(rows, 500).Error)

	deleted, err := s.DeleteExpiredUniqueLocks(ctx, n)
	require.NoError(t, err, "unique-lock sweep must survive a batch size above the bind-parameter ceiling")
	require.Equal(t, int64(n), deleted)
}
