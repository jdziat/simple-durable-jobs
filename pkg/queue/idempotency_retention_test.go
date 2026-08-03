package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestIdempotencyWindowSurvivesRetentionGC is the end-to-end shape of the harm:
// an operator's "never charge this invoice twice within 90 days" guard, a job that
// succeeded quickly, and the (much shorter) retention sweep running over it.
// docs/content/docs/api-reference/job-options.md promises the window keeps
// deduplicating until the TTL expires "even if the original job completed
// quickly"; before the fix retention deleted the job row and its lock together, so
// the replayed request enqueued a SECOND job and the card was charged twice.
func TestIdempotencyWindowSurvivesRetentionGC(t *testing.T) {
	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t)
	q := New(store)
	q.Register("charge", func(context.Context, string) error { return nil })

	first, err := q.Enqueue(ctx, "charge", "inv_1", IdempotencyKey("req-1", 90*24*time.Hour))
	require.NoError(t, err)

	// The job succeeded two hours ago.
	require.NoError(t, db.Model(&core.Job{}).Where("id = ?", first).
		Updates(map[string]any{
			"status":       core.StatusCompleted,
			"completed_at": time.Now().Add(-2 * time.Hour).UTC(),
		}).Error)

	// Precondition: the window still has ~90 days left.
	var expires time.Time
	require.NoError(t, db.Model(&core.UniqueLock{}).Where("job_id = ?", first).
		Select("expires_at").Scan(&expires).Error)
	require.True(t, expires.After(time.Now().Add(89*24*time.Hour)),
		"fixture must have a live 90-day window, got %s", expires)

	// A retention pass with a one-hour completed window (stricter than the stock
	// 30-day one and than jobs.DefaultRetention()'s 7-day preset).
	deleted, err := store.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.EqualValues(t, 0, deleted, "retention must not end a live idempotency window")

	second, err := q.Enqueue(ctx, "charge", "inv_1", IdempotencyKey("req-1", 90*24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, first, second, "the replayed request must dedup, not charge twice")

	var count int64
	require.NoError(t, db.Model(&core.Job{}).Count(&count).Error)
	assert.EqualValues(t, 1, count, "no second job row inside the window")
}

// TestIdempotencyWindowCollectedOnceExpired is the counterweight: the pin lasts
// exactly as long as the operator's TTL and no longer, so job rows guarded by a
// window are still bounded.
func TestIdempotencyWindowCollectedOnceExpired(t *testing.T) {
	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t)
	q := New(store)
	q.Register("charge", func(context.Context, string) error { return nil })

	first, err := q.Enqueue(ctx, "charge", "inv_1", IdempotencyKey("req-1", 90*24*time.Hour))
	require.NoError(t, err)
	require.NoError(t, db.Model(&core.Job{}).Where("id = ?", first).
		Updates(map[string]any{
			"status":       core.StatusCompleted,
			"completed_at": time.Now().Add(-2 * time.Hour).UTC(),
		}).Error)
	// The window lapses.
	require.NoError(t, db.Model(&core.UniqueLock{}).Where("job_id = ?", first).
		Update("expires_at", time.Now().Add(-time.Minute).UTC()).Error)

	deleted, err := store.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleted, "an expired window no longer pins its job row")

	var jobs, locks int64
	require.NoError(t, db.Model(&core.Job{}).Count(&jobs).Error)
	require.NoError(t, db.Model(&core.UniqueLock{}).Count(&locks).Error)
	assert.EqualValues(t, 0, jobs)
	assert.EqualValues(t, 0, locks)
}
