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
// The comparison has to be face-independent, which is what the shipped predicate
// gives: raw text when the two offsets match, strftime-normalized instants when
// they differ. (Not datetime() — that truncates to whole SECONDS and stalls
// sub-second schedules; see scheduleCursorLess.)
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

	// The boundary is rendered on a DIFFERENT face — what a schedule carrying its
	// own location (CronIn/DailyIn/WeeklyIn, or a CRON_TZ= prefix) produces against
	// this legacy cursor. A same-face boundary would compare correctly under every
	// predicate ever shipped here, including the broken ones, so it would prove
	// nothing about the upgrade.
	next := stored.Add(time.Hour).UTC()
	won, err := s.ClaimScheduledFire(ctx, "every-legacy", next)
	require.NoError(t, err)
	assert.True(t, won,
		"an Every cursor written on a positive-offset local face must still claim a boundary "+
			"expressed elsewhere — forcing either clock face at write time stalls one of the two "+
			"schedule families on upgrade")
}

// TestScheduleCursorLess_NormalizesEveryStoredShape pins the normalizing
// expression against every value the column can actually hold.
//
// The claim predicate is only correct if strftime() renders the SAME UTC instant
// for the same moment however it was written — and the driver can have written it
// on any offset, with or without sub-second digits, and older rows may predate
// conventions this wave assumes. A shape it silently got wrong would resurrect the
// stalled-schedule bug for exactly the deployments that have been running longest.
//
// FALSE-GREEN TRAP: comparing each rendering to a hardcoded string tests the
// format, not the property. The property is that equal instants render EQUAL and
// ordered instants render ORDERED, which is what the claim compares.
func TestScheduleCursorLess_NormalizesEveryStoredShape(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("the normalizing expression is SQLite-only; PG/MySQL compare real timestamps")
	}
	const expr = `strftime('%Y-%m-%d %H:%M:%f', ?)`

	render := func(v any) *string {
		var out *string
		require.NoError(t, s.DB().Raw("SELECT "+expr, v).Scan(&out).Error)
		return out
	}

	// The same instant, written every way the driver and history can produce it.
	sameInstant := []string{
		"2026-07-26 23:00:00+00:00", // UTC face — Daily/Weekly/Cron cursors
		"2026-07-27 08:00:00+09:00", // positive local face — Every cursors east of UTC
		"2026-07-26 16:00:00-07:00", // negative local face — Every cursors west
		"2026-07-26 23:00:00",       // no offset suffix at all
		"2026-07-26T23:00:00Z",      // RFC3339 with Z
	}
	want := render(sameInstant[0])
	require.NotNil(t, want)
	for _, v := range sameInstant[1:] {
		got := render(v)
		require.NotNil(t, got, "%q must render, not NULL", v)
		assert.Equal(t, *want, *got,
			"%q is the same instant as %q and must normalize identically, or a cursor written "+
				"on that face stalls its schedule", v, sameInstant[0])
	}

	// Ordering must survive the normalization, in both directions across faces.
	later := render("2026-07-27 09:00:00+09:00") // one hour after
	require.NotNil(t, later)
	assert.Less(t, *want, *later, "an instant an hour later must sort after, across clock faces")

	// Shapes that must not error or produce nonsense.
	for _, v := range []string{
		"0001-01-01 00:00:00+00:00", // Go's zero time
		"1969-07-20 20:17:00+00:00", // pre-epoch
		"9999-12-31 23:59:59+00:00", // far future
		"2026-03-08 02:30:00-05:00", // inside a US spring-forward fold
	} {
		assert.NotNil(t, render(v), "%q must normalize rather than yielding NULL", v)
	}

	// A NULL cursor must fail CLOSED: the predicate yields NULL, the row does not
	// match, and the boundary is simply not claimed — never an error or a claim.
	assert.Nil(t, render(nil), "NULL must stay NULL so the claim fails closed")
}

// TestClaimScheduledFire_SubMillisecondBoundariesStillAdvance guards a regression
// the instant-normalization introduced.
//
// Normalizing BOTH sides truncates to the expression's resolution: strftime('%f')
// keeps milliseconds, datetime() only whole seconds. Two boundaries inside one
// millisecond then compare "not less than", the claim matches nothing, and the
// schedule stops entirely — measured against a released v4.7.0 binary,
// Every(100µs) went from 20/20 boundaries to 2/20 and Every(1ns) to 0/20. A
// silent permanent stall is exactly the failure this whole change set exists to
// remove, so trading it for the cross-face fix would be no trade at all.
//
// The predicate is face-aware: identical offsets compare as raw text, which is
// exact to the nanoseconds the driver wrote.
//
// FALSE-GREEN TRAP: boundaries a second or more apart pass under every variant,
// including the ones that stall. The gap has to be sub-millisecond.
func TestClaimScheduledFire_SubMillisecondBoundariesStillAdvance(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	base := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	_, err := s.SeedScheduledFire(ctx, "sub-ms", base)
	require.NoError(t, err)

	// The floor is the COLUMN's own resolution, and it differs per dialect. Steps
	// below it cannot be represented at all, so failing to advance there is
	// physics rather than a defect. Measured directly:
	//
	//   SQLite    text-encoded  -> nanoseconds round-trip
	//   Postgres  timestamptz   -> MICROSECOND (a 1ns and a 100ns step round away)
	//   MySQL     datetime(3)   -> MILLISECOND
	//
	// Only SQLite was ever at risk from the comparison expression truncating,
	// because only there does the column hold more precision than a normalized
	// rendering would keep.
	floor := time.Millisecond // MySQL datetime(3)
	switch {
	case s.isSQLite:
		floor = time.Nanosecond
	case s.db.Name() == "postgres":
		floor = time.Microsecond
	}
	steps := []time.Duration{floor, 100 * floor, 500 * floor}
	for _, step := range steps {
		t.Run(step.String(), func(t *testing.T) {
			name := "sub-ms-" + step.String()
			_, err := s.SeedScheduledFire(ctx, name, base)
			require.NoError(t, err)

			cur := base
			for i := range 20 {
				cur = cur.Add(step)
				won, err := s.ClaimScheduledFire(ctx, name, cur)
				require.NoError(t, err)
				require.True(t, won,
					"boundary %d at +%v must be claimable: a schedule whose period is below the "+
						"comparison's resolution stops firing entirely and silently", i+1, step)
			}
		})
	}
}

// TestClaimScheduledFire_VariableWidthFractionsCompareByValue pins the property
// the same-offset fast path silently depends on.
//
// mattn/go-sqlite3 renders the fraction with Go's ".999999999" verb, which ELIDES
// trailing zeros — so the stored width VARIES with the value:
//
//	500000000ns -> "...10:00:00.5+00:00"
//	450000000ns -> "...10:00:00.45+00:00"
//	        0ns -> "...10:00:00+00:00"      (no fraction at all)
//
// A lexical comparison of decimal fractions is only correct if the character that
// TERMINATES a short fraction sorts below every digit. It does, but by luck of the
// format rather than by design: the terminator is the offset sign, '+' (0x2B) or
// '-' (0x2D), and both are below '0' (0x30). If the driver ever rendered a
// zero-padded fixed-width fraction, or terminated with 'Z' (0x5A, ABOVE the
// digits), the fast path would silently invert and a schedule would stop
// advancing inside a second.
//
// FALSE-GREEN TRAP: pairs that differ in the whole-seconds part never reach the
// fraction, and equal-width fractions compare the same either way. Only pairs
// whose fractions differ in WIDTH exercise this.
func TestClaimScheduledFire_VariableWidthFractionsCompareByValue(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("the text fast path is SQLite-only")
	}
	ctx := context.Background()
	base := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name       string
		cursor, to time.Duration
		wantClaim  bool
	}{
		{"no-fraction to .5", 0, 500 * time.Millisecond, true},
		{".05 to .5", 50 * time.Millisecond, 500 * time.Millisecond, true},
		{".45 to .5", 450 * time.Millisecond, 500 * time.Millisecond, true},
		{".5 to .55", 500 * time.Millisecond, 550 * time.Millisecond, true},
		{".5 back to .45", 500 * time.Millisecond, 450 * time.Millisecond, false},
		{".55 back to .5", 550 * time.Millisecond, 500 * time.Millisecond, false},
		{".5 back to no-fraction", 500 * time.Millisecond, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "frac-" + tc.name
			_, err := s.SeedScheduledFire(ctx, name, base.Add(tc.cursor))
			require.NoError(t, err)

			won, err := s.ClaimScheduledFire(ctx, name, base.Add(tc.to))
			require.NoError(t, err)
			assert.Equal(t, tc.wantClaim, won,
				"fractions of DIFFERENT WIDTH must compare by value, not by character count: "+
					"cursor .%09d vs boundary .%09d", tc.cursor.Nanoseconds(), tc.to.Nanoseconds())
		})
	}
}

// TestClaimScheduledFire_CrossFaceSubSecondBoundaries covers the ONE branch the
// rest of this file cannot reach.
//
// The predicate is face-aware: when the offsets MATCH it compares raw text, which
// is exact to nanoseconds. Every other sub-second test here is same-face, so it
// takes that branch and never touches the normalizing expression at all —
// substituting datetime() for strftime('%f') left the whole file green even though
// datetime() truncates to whole SECONDS.
//
// Only a boundary that is BOTH cross-face AND sub-second apart exercises the
// normalizer's resolution. That combination is not exotic: it is what a schedule
// carrying its own location produces against a cursor written elsewhere, at any
// period below a second.
//
// FALSE-GREEN TRAP: same-face sub-second pairs take the text branch; cross-face
// pairs an hour apart survive second-truncation. Both are needed at once.
func TestClaimScheduledFire_CrossFaceSubSecondBoundaries(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("the normalizing expression is SQLite-only")
	}
	ctx := context.Background()

	east := time.FixedZone("probe+9", 9*3600)
	base := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)

	// Cursor on one face, boundary 100ms later on ANOTHER face.
	_, err := s.SeedScheduledFire(ctx, "xf-subsec", base)
	require.NoError(t, err)
	won, err := s.ClaimScheduledFire(ctx, "xf-subsec", base.Add(100*time.Millisecond).In(east))
	require.NoError(t, err)
	assert.True(t, won,
		"a boundary 100ms later on a DIFFERENT clock face must claim — an expression that "+
			"truncates to whole seconds collapses it into the cursor and stalls the schedule")

	// And the negative control: 100ms EARLIER, cross-face, must not claim.
	_, err = s.SeedScheduledFire(ctx, "xf-subsec-back", base)
	require.NoError(t, err)
	won, err = s.ClaimScheduledFire(ctx, "xf-subsec-back", base.Add(-100*time.Millisecond).In(east))
	require.NoError(t, err)
	assert.False(t, won, "an earlier boundary must not claim, cross-face or not")
}
