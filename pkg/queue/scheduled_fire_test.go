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

// scheduledFireRow reads the raw persisted cursor + real-fire marker.
func scheduledFireRow(t *testing.T, store *storage.GormStorage, name string) core.ScheduledFire {
	t.Helper()
	var row core.ScheduledFire
	require.NoError(t, store.DB().Where("name = ?", name).First(&row).Error)
	return row
}

// A scheduled fire whose enqueue is deduplicated by an active Unique key is a
// deliberate SKIP, not a failure. It used to be retried in a tight loop; now the
// claim COMMITS so the cursor advances — but last_fired_at, the REAL-fire marker,
// is restored, because it feeds the dashboard's per-schedule last-run and its
// overdue/health indicator. Stamping it on a skip would render a schedule blocked
// for hours by a stuck unique job as perfectly healthy.
//
// This invariant shipped with no test at all; the gate flagged it as the one new
// behaviour it would not merge unverified.
//
// FALSE-GREEN TRAP: asserting only that the call returns ErrDuplicateJob passes
// without the restore, which is the subtle half. The cursor and the marker must be
// checked SEPARATELY — they are written together by the claim and only the
// marker is rolled back.
func TestEnqueueScheduledFire_DedupAdvancesCursorButNotTheRealFireMarker(t *testing.T) {
	q, store := newSQLiteQueueForFireTest(t)
	ctx := context.Background()

	// An in-flight job already holds the unique key the schedule enqueues under.
	first := time.Now().Truncate(time.Minute)
	claimed, id, err := q.EnqueueScheduledFire(ctx, "sched-U", first, "importjob", nil, Unique("the-key"))
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotEqual(t, core.NilUUID, id)

	afterRealFire := scheduledFireRow(t, store, "sched-U")
	require.NotNil(t, afterRealFire.LastFiredAt, "a real fire must stamp the marker")

	// The next boundary is blocked by that still-active unique key.
	second := first.Add(time.Minute)
	claimed2, id2, err2 := q.EnqueueScheduledFire(ctx, "sched-U", second, "importjob", nil, Unique("the-key"))

	require.ErrorIs(t, err2, core.ErrDuplicateJob,
		"a dedup is reported as ErrDuplicateJob, not a generic failure")
	assert.True(t, claimed2,
		"the claim must COMMIT on a skip, or the boundary is retried in a tight loop")
	assert.Equal(t, core.NilUUID, id2, "nothing was enqueued, so there is no job id")

	afterSkip := scheduledFireRow(t, store, "sched-U")
	assert.True(t, afterSkip.LastFireAt.After(afterRealFire.LastFireAt),
		"the CURSOR must advance past the skipped boundary")
	require.NotNil(t, afterSkip.LastFiredAt)
	assert.True(t, afterSkip.LastFiredAt.Equal(*afterRealFire.LastFiredAt),
		"the REAL-fire marker must be restored to the last actual fire: a schedule blocked "+
			"by a stuck unique job must not read as healthy on the dashboard")
}

// TestEnqueueScheduledFire_DedupOnFirstEverBoundaryLeavesMarkerNil covers the
// edge the restore has to get right: with no prior fire the marker is NULL, and
// restoring it means writing NULL back — a case a naive "only restore when
// non-nil" implementation skips, leaving the skip stamped as a real fire.
func TestEnqueueScheduledFire_DedupOnFirstEverBoundaryLeavesMarkerNil(t *testing.T) {
	q, store := newSQLiteQueueForFireTest(t)
	ctx := context.Background()

	// Occupy the unique key WITHOUT going through the schedule, so the schedule
	// has never had a real fire. Enqueue (unlike EnqueueScheduledFire) validates
	// that a handler exists, so register a no-op one.
	q.Register("importjob", func(context.Context, struct{}) error { return nil })
	_, err := q.Enqueue(ctx, "importjob", nil, Unique("blocker"))
	require.NoError(t, err)

	claimed, id, err := q.EnqueueScheduledFire(ctx, "sched-N", time.Now().Truncate(time.Minute),
		"importjob", nil, Unique("blocker"))
	require.ErrorIs(t, err, core.ErrDuplicateJob)
	assert.True(t, claimed)
	assert.Equal(t, core.NilUUID, id)

	assert.Nil(t, scheduledFireRow(t, store, "sched-N").LastFiredAt,
		"a schedule that has never actually fired must keep a NULL marker through a skip")
}
