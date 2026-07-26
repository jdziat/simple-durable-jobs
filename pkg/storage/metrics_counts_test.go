package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
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

// TestQueueOldestPendingAt_IgnoresNotYetDueJobs pins the backlog age to the
// DEQUEUE'S eligibility expression rather than created_at.
//
// The seeding order is the whole test. The obvious version — seed the scheduled
// job "now" — passes with the bug fully present, because MIN(created_at) picks
// the due job anyway and the missing due-ness predicate is never observed. Only
// a row created EARLIER than the due job but not due until later exposes it, and
// that is also the real production shape: a schedule enqueued at deploy time,
// next due next month.
func TestQueueOldestPendingAt_IgnoresNotYetDueJobs(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)
	now := time.Now()
	dueAt := now.Add(-30 * time.Minute)
	createdLongAgo := now.Add(-5 * time.Hour)
	notDueUntil := now.Add(24 * time.Hour)

	// "mixed": one genuinely-waiting job, plus a much older row that is not due yet.
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("pkt18-mixed-due"), Type: "work", Queue: "mixed", CreatedAt: dueAt}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("pkt18-mixed-scheduled"), Type: "work", Queue: "mixed", CreatedAt: createdLongAgo, RunAt: &notDueUntil}))
	// "scheduled-only": nothing is claimable, so there is no backlog to age.
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: testUUID("pkt18-future-only"), Type: "work", Queue: "scheduled-only", CreatedAt: createdLongAgo, RunAt: &notDueUntil}))

	oldestByQueue, err := store.QueueOldestPendingAt(ctx)
	require.NoError(t, err)

	// Before the fix this reported the scheduled row's created_at: 5h, not 30m.
	got, ok := oldestByQueue["mixed"]
	require.True(t, ok, "a queue holding a due job must report a backlog age")
	assert.WithinDuration(t, dueAt, got, time.Second,
		"backlog age must anchor to the oldest DUE job, not to a scheduled job's created_at")

	// Before the fix this queue was present and reported as 5h old — one scheduled
	// job pinning the alert on a queue with nothing to work is what made operators
	// mute the alert.
	assert.NotContains(t, oldestByQueue, "scheduled-only",
		"a queue holding only not-yet-due jobs has nothing claimable and must report no backlog age")
}
