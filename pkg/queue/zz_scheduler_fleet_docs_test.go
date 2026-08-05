package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// This file guards the fleet-scheduler tip in examples/scheduled/README.md.
//
// The shipped tip said: "The scheduler is per-worker. If you run multiple workers
// with the scheduler enabled, duplicate scheduled jobs may be enqueued. Use
// jobs.Unique() on your scheduled jobs or enable the scheduler on only one
// worker." Neither half held. Every boundary is claimed atomically in
// scheduled_fires, so N workers produce ONE job per boundary; and both offered
// remedies are harmful — a single scheduler worker is a single point of failure,
// and jobs.Unique() makes the queue deliberately SKIP boundaries whose prior
// instance is still live (the cursor advances, nothing runs).
//
// docs/content/docs/advanced/guarantees.md already listed "single-scheduler
// firing" as a chaos-asserted invariant, so the README contradicted it.

const scheduledExampleReadmePath = "../../examples/scheduled/README.md"

// newFleetQueues returns two INDEPENDENT Queue values over one database — two
// workers in a fleet.
func newFleetQueues(t *testing.T) (*Queue, *Queue, *storage.GormStorage) {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/fleet.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
		require.NoError(t, err)
		return db
	}
	storeA := storage.NewGormStorage(open())
	require.NoError(t, storeA.Migrate(context.Background()))
	storeB := storage.NewGormStorage(open())
	return New(storeA), New(storeB), storeA
}

// TestScheduleBoundaryIsClaimedOnceFleetWide pins the claim, not a timing
// observation: two peers offering the SAME boundary produce exactly one job.
func TestScheduleBoundaryIsClaimedOnceFleetWide(t *testing.T) {
	qA, qB, store := newFleetQueues(t)
	ctx := context.Background()
	boundary := time.Now().Truncate(time.Second)

	claimedA, idA, err := qA.EnqueueScheduledFire(ctx, "health-check", boundary, "importjob", map[string]string{"n": "1"})
	require.NoError(t, err)
	claimedB, idB, err := qB.EnqueueScheduledFire(ctx, "health-check", boundary, "importjob", map[string]string{"n": "1"})
	require.NoError(t, err)

	assert.True(t, claimedA, "the first peer to reach the boundary must claim it")
	assert.False(t, claimedB, "a peer must not re-claim a boundary another peer already fired")
	assert.NotEqual(t, core.NilUUID, idA)
	assert.Equal(t, core.NilUUID, idB)

	jobs, err := store.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Len(t, jobs, 1, "two scheduler workers must enqueue ONE job per boundary, not two")

	// A LATER boundary is still claimable by the peer that lost the first one —
	// which is why running the scheduler everywhere is the available-by-default
	// choice, not a duplication hazard.
	claimedNext, _, err := qB.EnqueueScheduledFire(ctx, "health-check", boundary.Add(time.Second), "importjob", map[string]string{"n": "2"})
	require.NoError(t, err)
	assert.True(t, claimedNext, "a peer must be able to fire the next boundary")
}

// TestUniqueOnScheduledFireSkipsTheBoundary pins the cost of the README's old
// remedy: with jobs.Unique() and a prior instance still live, the boundary is
// consumed and nothing runs.
func TestUniqueOnScheduledFireSkipsTheBoundary(t *testing.T) {
	q, _, store := newFleetQueues(t)
	ctx := context.Background()
	first := time.Now().Truncate(time.Second)

	claimed, id, err := q.EnqueueScheduledFire(ctx, "nightly", first, "importjob",
		map[string]string{"k": "v"}, Unique("nightly"))
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotEqual(t, core.NilUUID, id)

	// Next boundary, prior instance still pending (never ran).
	claimed2, id2, err := q.EnqueueScheduledFire(ctx, "nightly", first.Add(time.Second), "importjob",
		map[string]string{"k": "v"}, Unique("nightly"))
	assert.True(t, claimed2, "the boundary is CONSUMED — the cursor advances")
	assert.Equal(t, core.NilUUID, id2, "but no job is enqueued: the run is skipped")
	assert.ErrorIs(t, err, core.ErrDuplicateJob, "the skip is reported through the sentinel")

	jobs, err := store.GetJobsByStatus(ctx, core.StatusPending, 100)
	require.NoError(t, err)
	assert.Len(t, jobs, 1, "two boundaries, one job: Unique(key) drops runs, it does not de-duplicate peers")
}

// TestScheduledExampleReadmeDoesNotWarnOfDuplicates requires the page to have
// shed the false warning and both harmful remedies.
func TestScheduledExampleReadmeDoesNotWarnOfDuplicates(t *testing.T) {
	b, err := os.ReadFile(scheduledExampleReadmePath)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it rather than deleting it", scheduledExampleReadmePath)
	doc := string(b)

	assert.NotContains(t, doc, "duplicate scheduled jobs may be enqueued",
		"boundaries are claimed atomically fleet-wide; there are no duplicates to warn about")
	assert.NotContains(t, doc, "enable the scheduler on only one worker",
		"a single scheduler worker is a single point of failure, not a safety measure")
	assert.Contains(t, doc, "claimed exactly once fleet-wide",
		"the page must state the real guarantee")
	assert.Contains(t, doc, "SKIPPED",
		"the page must state what Unique() on a scheduled job actually does")
}
