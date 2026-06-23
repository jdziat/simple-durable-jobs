package queue

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newSQLiteQueueForFireTest(t *testing.T) (*Queue, *storage.GormStorage) {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/fire.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return New(store), store
}

// TestEnqueueScheduledFire_RollsBackClaimOnEnqueueFailure is the regression test
// for teardown g8: the scheduler used to durably advance the fire cursor
// (ClaimScheduledFire) and only THEN enqueue, so a failed enqueue silently dropped
// a due run while it was recorded as fired. EnqueueScheduledFire now claims and
// enqueues in one transaction; a failed enqueue must roll the claim back so the
// boundary stays re-claimable.
func TestEnqueueScheduledFire_RollsBackClaimOnEnqueueFailure(t *testing.T) {
	q, store := newSQLiteQueueForFireTest(t)
	ctx := context.Background()
	fireTime := time.Now()

	// A channel cannot be JSON-marshaled, so buildJob — which runs inside
	// EnqueueTx, inside the claim transaction — fails, exercising the rollback.
	claimed, _, err := q.EnqueueScheduledFire(ctx, "sched-A", fireTime, "importjob", make(chan int))
	require.Error(t, err)
	assert.False(t, claimed, "a failed enqueue must not report a claimed boundary")

	// The claim must have rolled back: the SAME boundary is still claimable.
	reclaimed, err := store.ClaimScheduledFire(ctx, "sched-A", fireTime)
	require.NoError(t, err)
	assert.True(t, reclaimed,
		"boundary must be re-claimable after a failed enqueue (the claim was rolled back)")
}

// TestEnqueueScheduledFire_CommitsClaimAndEnqueueAtomically proves the happy path:
// claim + enqueue commit together, the job is persisted, and the boundary cursor
// is advanced so the same boundary cannot fire twice.
func TestEnqueueScheduledFire_CommitsClaimAndEnqueueAtomically(t *testing.T) {
	q, store := newSQLiteQueueForFireTest(t)
	ctx := context.Background()
	fireTime := time.Now()

	claimed, id, err := q.EnqueueScheduledFire(ctx, "sched-B", fireTime, "importjob", map[string]string{"k": "v"})
	require.NoError(t, err)
	assert.True(t, claimed)
	require.NotEqual(t, core.NilUUID, id)

	job, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job, "the fired job must be persisted")

	// The cursor advanced atomically with the enqueue: re-claiming the SAME
	// boundary now fails.
	reclaimed, err := store.ClaimScheduledFire(ctx, "sched-B", fireTime)
	require.NoError(t, err)
	assert.False(t, reclaimed,
		"the same boundary must not be re-claimable after a committed fire")
}
