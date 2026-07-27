package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A schedule with an EXPLICIT location produces fire boundaries in that location,
// and `last_fire_at < ?` is a TEXT comparison on SQLite — so the boundary and the
// stored cursor must be rendered on one clock face or the compare is nonsense.
//
// Measured before the fix: an anchor of 16:00 UTC stores as "2026-07-26
// 16:00:00+00:00", while DailyIn(America/New_York, 13, 0) yields 13:00-04:00 —
// the SAME 17:00 UTC instant, genuinely AFTER the anchor, which Go agrees with.
// Lexically "13:00:00-04:00" sorts BELOW "16:00:00+00:00", so the claim matched
// nothing and the schedule silently never fired. That reached every API this wave
// added — CronIn, DailyIn, WeeklyIn and the CRON_TZ= prefix.
//
// FALSE-GREEN TRAP: a UTC schedule cannot show this, because the boundary and the
// cursor already share a face. The schedule's location has to differ from the
// host's, and the offset has to move the rendered hour ACROSS the anchor's — a
// boundary an hour later in a zone one hour off would still sort correctly.
func TestClaimScheduledFire_BoundaryInAnotherLocationStillClaims(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	base := time.Date(2026, 7, 26, 16, 0, 0, 0, time.UTC)
	_, err = s.SeedScheduledFire(ctx, "tz-sched", base)
	require.NoError(t, err)

	boundary := schedule.DailyIn(ny, 13, 0).Next(base)
	require.True(t, boundary.After(base), "fixture: the boundary must genuinely be after the anchor")

	won, err := s.ClaimScheduledFire(ctx, "tz-sched", boundary)
	require.NoError(t, err)
	assert.True(t, won,
		"a boundary computed in a non-host location must still claim: it is a real instant "+
			"after the cursor, and rendering it on a different clock face must not hide that")
}

// TestClaimScheduledFire_DoesNotClaimAnEarlierBoundary is the negative control.
// Normalizing both sides must not make everything claimable — a boundary genuinely
// BEFORE the cursor must still be rejected, whatever zone it is expressed in.
func TestClaimScheduledFire_DoesNotClaimAnEarlierBoundary(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	base := time.Date(2026, 7, 26, 16, 0, 0, 0, time.UTC)
	_, err = s.SeedScheduledFire(ctx, "tz-sched-back", base)
	require.NoError(t, err)

	earlier := base.Add(-2 * time.Hour).In(tokyo)
	won, err := s.ClaimScheduledFire(ctx, "tz-sched-back", earlier)
	require.NoError(t, err)
	assert.False(t, won, "a boundary before the cursor must not be claimable in any zone")
}
