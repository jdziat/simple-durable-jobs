package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func TestQueueDeadLetterCounts(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)

	seedTerminalFailure(t, store, "alpha-1", "alpha")
	seedTerminalFailure(t, store, "alpha-2", "alpha")
	seedTerminalFailure(t, store, "beta-1", "beta")
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("failed-without-dlq"), Type: "work", Queue: "alpha", Status: core.StatusFailed}))

	counts, err := store.QueueDeadLetterCounts(ctx)
	require.NoError(t, err)

	assert.Equal(t, map[string]int{
		"alpha": 2,
		"beta":  1,
	}, counts)
}

func TestQueueOldestPendingAt(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	alphaOldest := now.Add(-3 * time.Hour)
	alphaNewer := now.Add(-time.Hour)
	betaOldest := now.Add(-2 * time.Hour)

	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("alpha-oldest"), Type: "work", Queue: "alpha", CreatedAt: alphaOldest}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("alpha-newer"), Type: "work", Queue: "alpha", CreatedAt: alphaNewer}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("beta-oldest"), Type: "work", Queue: "beta", CreatedAt: betaOldest}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("alpha-running"), Type: "work", Queue: "alpha", Status: core.StatusRunning, CreatedAt: now.Add(-4 * time.Hour)}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("archive-completed"), Type: "work", Queue: "archive", Status: core.StatusCompleted, CreatedAt: now.Add(-5 * time.Hour)}))

	oldestByQueue, err := store.QueueOldestPendingAt(ctx)
	require.NoError(t, err)

	require.Len(t, oldestByQueue, 2)
	assert.Equal(t, alphaOldest.Unix(), oldestByQueue["alpha"].Unix())
	assert.Equal(t, betaOldest.Unix(), oldestByQueue["beta"].Unix())
	assert.NotContains(t, oldestByQueue, "archive")
}

func seedTerminalFailure(t *testing.T, store *GormStorage, id, queueName string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.NewID(), Type: "work", Queue: queueName, MaxRetries: 1}))
	job, err := store.Dequeue(ctx, []string{queueName}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.NoError(t, store.Fail(ctx, job.ID, "worker-1", "boom", nil))
}
