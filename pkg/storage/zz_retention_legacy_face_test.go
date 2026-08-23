package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedCompletedOnFace marks a job completed at `at`, rendered on the clock face of
// `at` itself — raw SQL, because GORM would normalize a time.Time and erase the very
// thing under test.
func seedCompletedOnFace(t *testing.T, ctx context.Context, s *GormStorage, at time.Time) core.UUID {
	t.Helper()
	job := &core.Job{Type: "ret.face", Queue: "default", Args: []byte(`{}`), MaxRetries: 3}
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.db.WithContext(ctx).Exec(
		`UPDATE jobs SET status='completed', completed_at=? WHERE id=?`,
		at.Format("2006-01-02 15:04:05.999999999-07:00"), job.ID).Error)
	return job.ID
}

// Retention must not delete a job that is still inside its window, whatever clock
// face the row was written on.
//
// completed_at was normalized to UTC without a backfill, so an upgraded database
// holds both faces — and on SQLite the column is TEXT compared LEXICALLY. With a
// bare `completed_at < ?`:
//
//	west of UTC  a legacy row sorts BELOW the cutoff and is DELETED, together with
//	             its checkpoints, signals and fan-outs. Measured: a job that
//	             completed 10 minutes ago, removed under a 1-hour window.
//	east of UTC  it sorts ABOVE and is never collected, so retention silently stops
//	             draining pre-upgrade rows.
//
// The first is silent data loss on an ordinary upgrade, which is why this is a
// guard rather than a note. `liveUniqueLockGuard` in the same function was already
// face-aware and documented the failure; this term sat bare 40 lines below it.
//
// Vacuously green under TZ=UTC — the non-UTC CI legs are what make it a gate.
func TestRetentionDoesNotDeleteLegacyFacedJobsInsideTheWindow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("DB-clock backends compare native timestamps against the server clock")
	}

	// Completed TEN MINUTES ago, on the local face an older release would have used.
	legacy := seedCompletedOnFace(t, ctx, s, time.Now().Add(-10*time.Minute))
	// The same instant, on the face this release writes. The control matters: if
	// BOTH vanished the fixture would be wrong, not the code.
	control := seedCompletedOnFace(t, ctx, s, time.Now().UTC().Add(-10*time.Minute))

	// Retention keeps completed jobs for an hour. Neither job is eligible.
	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err, "retention reports success even when it destroys live data")

	assert.Zero(t, deleted,
		"retention deleted %d job(s) that completed 10 minutes ago under a 1h window", deleted)

	var legacyLeft, controlLeft int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", legacy).Count(&legacyLeft).Error)
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", control).Count(&controlLeft).Error)

	assert.Equal(t, int64(1), controlLeft,
		"the UTC-faced control must survive; if it does not, this fixture is wrong rather than the code")
	assert.Equal(t, int64(1), legacyLeft,
		"DATA LOSS: a job written on the pre-upgrade clock face, completed 10 minutes "+
			"ago, was deleted under a 1-hour retention window — along with its "+
			"checkpoints, signals and fan-outs")
}

// The mirror direction, which is not data loss but is a silent stall: a legacy-faced
// job that IS past its window must still be collectable. East of UTC a bare compare
// sorts it above the cutoff, so retention never drains pre-upgrade rows at all —
// which also invalidates the dead-letter path's stated mitigation that legacy rows
// drain with retention.
func TestRetentionStillCollectsLegacyFacedJobsPastTheWindow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("DB-clock backends compare native timestamps against the server clock")
	}

	legacy := seedCompletedOnFace(t, ctx, s, time.Now().Add(-3*time.Hour))

	deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
	require.NoError(t, err)
	assert.Positive(t, deleted, "a legacy-faced job three hours past a one-hour window must be collected")

	var left int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", legacy).Count(&left).Error)
	assert.Zero(t, left,
		"the legacy-faced row was not collected; retention has silently stopped draining "+
			"every row written before the clock-face normalization")
}
