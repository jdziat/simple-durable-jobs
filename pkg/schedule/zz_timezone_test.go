package schedule

import (
	"os"
	"os/exec"
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
// A prefix-free expression must be evaluated in UTC and NOT in the host's zone,
// so one expression fires at the same instant on every node in a fleet.
//
// FALSE-GREEN TRAP: under TZ=UTC — which is what CI runs — "defaults to UTC" and
// "defaults to the host zone" are behaviourally IDENTICAL, so no in-process
// assertion can tell them apart, and removing the UTC pin left this test green.
// The only way to make it discriminate is to evaluate under a zone that is not
// UTC, so the test re-executes itself once with TZ set. Go resolves time.Local
// from TZ at process start, which is why this needs a child process rather than
// os.Setenv.
func TestCron_WithoutPrefixStaysUTC(t *testing.T) {
	const childEnv = "SDJ_CRON_TZ_CHILD"
	if os.Getenv(childEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run", "^TestCron_WithoutPrefixStaysUTC$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1", "TZ=Asia/Tokyo")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err,
			"the prefix-free path must stay UTC when the host zone is NOT UTC:\n%s", out)
		return
	}

	// In the child, time.Local is Asia/Tokyo (UTC+9), so the two candidate
	// behaviours give answers nine hours apart.
	require.NotEqual(t, time.UTC.String(), time.Local.String(),
		"the child must run in a non-UTC zone or this test proves nothing")

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

	first := s.Next(from)
	second := s.Next(first)

	assert.Equal(t, time.Sunday, first.In(ny).Weekday(), "must land on the requested weekday")
	assert.Equal(t, time.Sunday, second.In(ny).Weekday(),
		"and must STILL land on it after crossing the DST boundary — adding 168h to a "+
			"normalized instant drifts the weekday")
	assert.Equal(t, 7, int(second.In(ny).Sub(first.In(ny)).Hours()/24+0.5),
		"consecutive fires must be seven calendar days apart")
}
