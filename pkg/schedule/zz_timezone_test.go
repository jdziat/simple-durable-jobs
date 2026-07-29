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
		// The four above ALL land on the `sep < 0` branch ("a timezone but no
		// schedule fields"), so the separate empty-NAME check below it was
		// unreachable from this table: deleting it and falling through to UTC kept
		// the whole package green. These two carry a separator, so the name really
		// is empty and the check is the only thing standing between them and a
		// schedule that silently runs in UTC instead of being rejected.
		"TZ= 0 9 * * *",
		"CRON_TZ=  0 9 * * *",
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
// A prefix-free expression must be evaluated in UTC and NOT in the host's zone,
// so one expression fires at the same instant on every node in a fleet.
//
// FALSE-GREEN TRAP, and the SECOND attempt at this test. robfig/cron's
// SpecSchedule.Next contains `if loc == time.Local { loc = t.Location() }` — an
// UNPINNED schedule evaluates in the INPUT's location. So feeding a UTC-faced
// `from` makes pinned-UTC and unpinned agree no matter what TZ the process runs
// in, which is why the first version of this test stayed green with the pin
// removed AND why re-executing under TZ=Asia/Tokyo did not help either: the input
// was still UTC-faced.
//
// The discriminating input is a `from` on a NON-UTC clock face. Then a pinned
// schedule answers in UTC while an unpinned one answers in the input's zone, and
// the two differ by the offset. No TZ manipulation is needed.
func TestCron_WithoutPrefixStaysUTC(t *testing.T) {
	s, err := Cron("0 9 * * *")
	require.NoError(t, err)

	// Deliberately NOT UTC. from is 12:00+09:00 == 03:00 UTC, so:
	//   pinned UTC   -> the next 09:00 UTC is the SAME day, 2026-07-20 09:00Z
	//   unpinned     -> the next 09:00 in +09:00 is 2026-07-21 09:00+09:00,
	//                   i.e. 2026-07-21 00:00Z — fifteen hours apart.
	east := time.FixedZone("probe+9", 9*3600)
	from := time.Date(2026, 7, 20, 12, 0, 0, 0, east)

	assert.Equal(t, time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC), s.Next(from).UTC(),
		"a prefix-free expression must be evaluated in UTC, not in the zone of the instant it "+
			"is asked about and not in the host's zone")
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

// DailyIn and WeeklyIn document "loc must be non-nil" and enforce it with a panic,
// but CronIn was the only one of the three whose nil case was tested — replacing
// either panic with a silent `loc = time.UTC` left the whole package green. A
// silent UTC fallback is precisely the bug this wave fixed in Cron: the schedule
// runs, at the wrong hour, and nothing says so.
func TestDailyInWeeklyIn_RejectANilLocation(t *testing.T) {
	assert.Panics(t, func() { DailyIn(nil, 9, 0) },
		"DailyIn documents a non-nil location; falling back to UTC would fire at the wrong hour silently")
	assert.Panics(t, func() { WeeklyIn(nil, time.Monday, 9, 0) },
		"WeeklyIn documents a non-nil location; falling back to UTC would fire at the wrong hour silently")
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

// Daily and Weekly advance by rolling the CALENDAR day, not by adding 24h (or
// 168h) to a normalized instant. In a DST zone those differ: time.Date
// normalizes a time that does not exist on a spring-forward day (02:30 -> 01:30),
// and AddDate on the normalized instant then carries the shift forward, so the
// schedule can fire twice on one day or skip one entirely.
//
// FALSE-GREEN TRAP, confirmed by a reviewer: the calendar roll had NO test.
// Replacing it with AddDate left the whole repo green, because every other
// schedule test runs in UTC where the two are identical. These use a real DST
// zone and step across the transition.
func TestDailyIn_RollsTheCalendarDayAcrossSpringForward(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// 2026-03-08 is the US spring-forward date: 02:00 -> 03:00, so 02:30 does not
	// exist. A schedule at 02:30 must still advance one calendar day at a time.
	s := DailyIn(ny, 2, 30)
	from := time.Date(2026, 3, 7, 12, 0, 0, 0, ny)

	seen := map[string]int{}
	for range 4 {
		from = s.Next(from)
		seen[from.In(ny).Format("2006-01-02")]++
	}
	for day, n := range seen {
		assert.Equal(t, 1, n,
			"a daily schedule must fire exactly once per calendar day; %s fired %d times "+
				"(adding 24h to a DST-normalized instant double-fires)", day, n)
	}
	assert.Len(t, seen, 4, "four advances must land on four distinct days")
}

func TestWeeklyIn_RollsSevenCalendarDaysAcrossSpringForward(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	s := WeeklyIn(ny, time.Sunday, 2, 30)
	from := time.Date(2026, 3, 1, 12, 0, 0, 0, ny)

	fires := make([]time.Time, 0, 5)
	for range 5 {
		from = s.Next(from)
		fires = append(fires, from.In(ny))
	}

	// 2026-03-08 02:30 does not exist (spring forward), so time.Date normalizes
	// that ONE fire to 01:30 EST. Every fire after it must be back at 02:30.
	seen := map[string]bool{}
	for i, f := range fires {
		assert.Equal(t, time.Sunday, f.Weekday(), "fire %d landed on %s", i+1, f.Weekday())

		date := f.Format("2006-01-02")
		assert.False(t, seen[date], "fire %d repeats %s — a weekly schedule fired twice in one week", i+1, date)
		seen[date] = true

		if i == 0 {
			continue // the normalized transition fire
		}
		assert.Equal(t, "02:30", f.Format("15:04"),
			"fire %d is at %s: rolling the INSTANT rather than the calendar day carries the "+
				"DST-normalized 01:30 forward permanently, so the schedule runs an hour early "+
				"for the rest of its life", i+1, f.Format("15:04 MST"))
	}
}
