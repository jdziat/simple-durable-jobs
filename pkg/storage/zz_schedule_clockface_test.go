package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
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

// TestClaimScheduledFire_LegacyUTCCursorStillClaims is the UPGRADE path, and it
// is the reason this column normalizes to UTC rather than to time.Local like
// run_at does.
//
// Every cursor already in a database was written on the UTC face: all the default
// constructors pin it (Daily/Weekly/Cron all resolve to UTC). Normalizing to local
// instead makes the stored cursor and the new bind different faces, and since the
// comparison is lexical TEXT on SQLite the claim then matches nothing — silently
// stopping every already-running schedule on upgrade. Measured before this was
// corrected: a stored "2026-07-26 23:00:00+00:00" cursor claimed FALSE against the
// very next hourly boundary.
//
// FALSE-GREEN TRAP: writing the legacy row through SeedScheduledFire would
// normalize it on the way in and prove nothing. The row has to be inserted
// directly, the way an older binary left it.
func TestClaimScheduledFire_LegacyUTCCursorStillClaims(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	stored := time.Date(2026, 7, 26, 23, 0, 0, 0, time.UTC)
	require.NoError(t, s.DB().Create(&core.ScheduledFire{Name: "legacy", LastFireAt: stored}).Error)

	won, err := s.ClaimScheduledFire(ctx, "legacy", stored.Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, won,
		"a cursor written by an older binary on the UTC face must still be claimable, or every "+
			"existing schedule silently stops firing on upgrade")
}

// TestClaimScheduledFire_LegacyLocalFacedCursorStillClaims is the OTHER upgrade
// path, and the one that proves a write-side clock face cannot be the fix.
//
// Existing databases hold a MIXTURE. Daily/Weekly/Cron pin UTC, so their cursors
// are UTC-faced (covered above). But `Every` seeds from time.Now() via
// establishScheduleBase, and everySchedule.Next is from.Truncate(d).Add(d) which
// PRESERVES the location — so every Every cursor is on the host's LOCAL face.
//
// Measured against a real v4.7.0 build under TZ=Asia/Tokyo: normalizing writes to
// UTC made nine consecutive hourly boundaries claim FALSE with a nil error, i.e.
// the schedule silently stopped for the length of the UTC offset before
// self-healing. Normalizing to local instead breaks the UTC family the same way.
// The comparison has to be face-independent, which is what datetime() gives.
//
// FALSE-GREEN TRAP: a fixed zone whose offset is NEGATIVE relative to UTC still
// sorts correctly as text, so it passes with the bug present. The probe zone must
// be POSITIVE (east of UTC), which is where the lexical order inverts.
func TestClaimScheduledFire_LegacyLocalFacedCursorStillClaims(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	east := time.FixedZone("probe+9", 9*3600)
	stored := time.Date(2026, 7, 27, 11, 0, 0, 0, east) // 02:00Z, written by an older binary
	require.NoError(t, s.DB().Create(&core.ScheduledFire{Name: "every-legacy", LastFireAt: stored}).Error)

	// The next hourly boundary an Every schedule produces keeps that same location.
	next := stored.Add(time.Hour)
	won, err := s.ClaimScheduledFire(ctx, "every-legacy", next)
	require.NoError(t, err)
	assert.True(t, won,
		"an Every cursor written on a positive-offset local face must still claim — forcing "+
			"either clock face at write time stalls one of the two schedule families on upgrade")
}
