package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Covers QueuePendingCounts / QueueRunningCounts (and the shared
// queueCountsByStatus helper), which feed the optional metrics exporter and the
// sdj CLI. Previously untested on every backend.

func seedCountJob(t *testing.T, ctx context.Context, s *GormStorage, id, queue string, status core.JobStatus) {
	t.Helper()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: core.UUID(signalUUID(id)), Type: "count.fixture", Queue: queue, Status: status,
	}).Error)
}

func TestQueuePendingCounts_GroupsByQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	seedCountJob(t, ctx, s, "p1", "emails", core.StatusPending)
	seedCountJob(t, ctx, s, "p2", "emails", core.StatusPending)
	seedCountJob(t, ctx, s, "p3", "reports", core.StatusPending)
	seedCountJob(t, ctx, s, "r1", "emails", core.StatusRunning) // excluded from pending
	seedCountJob(t, ctx, s, "c1", "emails", core.StatusCompleted)

	counts, err := s.QueuePendingCounts(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"emails": 2, "reports": 1}, counts)
}

func TestQueueRunningCounts_GroupsByQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	seedCountJob(t, ctx, s, "r1", "emails", core.StatusRunning)
	seedCountJob(t, ctx, s, "r2", "reports", core.StatusRunning)
	seedCountJob(t, ctx, s, "r3", "reports", core.StatusRunning)
	seedCountJob(t, ctx, s, "p1", "emails", core.StatusPending) // excluded from running

	counts, err := s.QueueRunningCounts(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"emails": 1, "reports": 2}, counts)
}

func TestQueueCounts_EmptyWhenNoMatchingJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	pending, err := s.QueuePendingCounts(ctx)
	require.NoError(t, err)
	assert.Empty(t, pending)

	running, err := s.QueueRunningCounts(ctx)
	require.NoError(t, err)
	assert.Empty(t, running)
}
