package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cron accepted a crontab-style CRON_TZ=/TZ= prefix, let robfig/cron parse the
// location out of it, and then OVERWROTE that location with UTC — so a
// timezone-aware schedule fired hours off with no error at all.
//
// FALSE-GREEN TRAP: asserting Cron returns a non-nil Schedule passes with the
// bug fully present. The discriminating assertion is the fire INSTANT.
func TestCron_HonoursExplicitTimezonePrefix(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	s, err := Cron("CRON_TZ=America/New_York 0 9 * * *")
	require.NoError(t, err)

	// Noon UTC on 2026-07-20 is 08:00 in New York, so the next 09:00 local is
	// later the SAME day — 13:00 UTC. Discarding the prefix yielded 09:00 UTC the
	// NEXT day, four hours and a date off.
	from := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	got := s.Next(from)
	assert.Equal(t, time.Date(2026, 7, 20, 9, 0, 0, 0, ny).UTC(), got.UTC(),
		"an explicit CRON_TZ prefix must be honoured, not silently replaced with UTC")

	tz, err := Cron("TZ=Asia/Tokyo 0 9 * * *")
	require.NoError(t, err)
	assert.NotEqual(t, time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC), tz.Next(from).UTC(),
		"TZ= must be honoured too")
}

// A prefix with no schedule fields after it made robfig slice with a negative
// index and PANIC — in a constructor that returns an error.
func TestCron_DoesNotPanicOnAMalformedPrefix(t *testing.T) {
	for _, expr := range []string{
		"CRON_TZ=America/New_York",
		"TZ=Europe/Berlin",
		"CRON_TZ=",
		"TZ=",
	} {
		assert.NotPanics(t, func() {
			_, err := Cron(expr)
			assert.Error(t, err, "%q must be an error, never a panic", expr)
		}, "Cron(%q) must not panic — it returns an error", expr)
	}
}

// Without a prefix the schedule must stay UTC. robfig defaults an unpinned
// schedule to the host's LOCAL zone, so dropping the pin would silently move
// every existing prefix-free schedule to whatever TZ the host happens to set.
func TestCron_WithoutPrefixStaysUTC(t *testing.T) {
	s, err := Cron("0 9 * * *")
	require.NoError(t, err)
	from := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC), s.Next(from).UTC(),
		"a prefix-free expression must remain UTC, not follow the host timezone")
}

func TestCronIn_RejectsAConflictingPrefix(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	_, err = CronIn(ny, "TZ=Asia/Tokyo 0 9 * * *")
	assert.Error(t, err, "two conflicting locations must be rejected, not silently resolved")

	_, err = CronIn(nil, "0 9 * * *")
	assert.Error(t, err, "a nil location must be an error, not a nil-deref")

	s, err := CronIn(ny, "0 9 * * *")
	require.NoError(t, err)
	from := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, time.Date(2026, 7, 20, 9, 0, 0, 0, ny).UTC(), s.Next(from).UTC())
}

// DailyIn/WeeklyIn advance by rolling the calendar DAY, not the instant. With
// AddDate on a DST spring-forward day the requested 02:30 normalizes to 01:30 and
// is then carried into the next day, firing TWICE there.
func TestDailyIn_FiresOncePerDayAcrossASpringForward(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	s := DailyIn(ny, 2, 30)
	cur := time.Date(2026, 3, 6, 12, 0, 0, 0, ny)

	seen := map[string]int{}
	for range 6 {
		cur = s.Next(cur)
		seen[cur.In(ny).Format("2006-01-02")]++
	}
	for day, n := range seen {
		assert.Equal(t, 1, n, "DailyIn must fire exactly once on %s, got %d", day, n)
	}
}
