package schedule

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadZone loads an IANA zone, skipping the subtest when the host's tzdata is
// too old to carry it. America/Coyhaique, for one, only appeared in 2025b — a
// missing zone must not be reported as a scheduling defect.
func loadZone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Skipf("host tzdata does not carry %s: %v", name, err)
	}
	return loc
}

// midnightSpringForwardZones are the zones whose spring-forward transition lands
// on local MIDNIGHT, found by ENUMERATING every zone in the host's tzdata rather
// than by trusting a list: for these, the requested 00:00 does not exist and
// time.Date normalizes it BACKWARD across midnight into the previous calendar
// day. Every other DST zone normalizes within the day, which is why the
// pre-existing America/New_York fixtures never caught this.
var midnightSpringForwardZones = []struct {
	zone string
	// date is a spring-forward date in that zone on which local 00:00 is skipped.
	y, m, d int
}{
	{"America/Santiago", 2024, 9, 8},
	{"America/Coyhaique", 2024, 9, 8},
	{"America/Asuncion", 2024, 10, 6},
	{"America/Havana", 2024, 3, 10},
	{"Atlantic/Azores", 2024, 3, 31},
	{"America/Scoresbysund", 2024, 3, 31},
}

// FINDING A. schedule.Every panics at construction on a non-positive interval
// precisely because a Next that returns its input "would otherwise make Next
// return its input unchanged, driving a ~10Hz ClaimScheduledFire busy-loop".
// DailyIn reaches that same fixed point without any invalid configuration: at a
// midnight spring-forward, time.Date(y, m, d+1, 0, 0, ...) normalizes back into
// day d and hands back exactly the instant it was given.
func TestDailyIn_NextAlwaysAdvancesAcrossAMidnightSpringForward(t *testing.T) {
	for _, z := range midnightSpringForwardZones {
		t.Run(z.zone, func(t *testing.T) {
			loc := loadZone(t, z.zone)
			s := DailyIn(loc, 0, 0)

			// Walk a week across the transition. Every step must strictly advance;
			// one fixed point wedges the scheduler cursor forever.
			cur := time.Date(z.y-0, time.Month(z.m), z.d-3, 12, 0, 0, 0, loc)
			for i := range 8 {
				next := s.Next(cur)
				require.Truef(t, next.After(cur),
					"step %d: DailyIn(%s, 00:00).Next(%s) returned %s, which is NOT after its input — "+
						"the scheduler cursor can never advance past this boundary",
					i, z.zone, cur.Format(time.RFC3339), next.Format(time.RFC3339))
				cur = next
			}
		})
	}
}

// FINDING A, the user-visible half: exactly one fire per calendar DAY is the
// documented contract at both DST edges. A fire that normalizes backward across
// midnight lands on the previous calendar day, so that day gets two fires and the
// transition day gets none.
func TestDailyIn_FiresExactlyOncePerCalendarDayAcrossAMidnightSpringForward(t *testing.T) {
	for _, z := range midnightSpringForwardZones {
		t.Run(z.zone, func(t *testing.T) {
			loc := loadZone(t, z.zone)
			s := DailyIn(loc, 0, 0)

			cur := time.Date(z.y, time.Month(z.m), z.d-3, 12, 0, 0, 0, loc)
			seen := map[string]int{}
			for range 6 {
				cur = s.Next(cur)
				seen[cur.In(loc).Format("2006-01-02")]++
			}
			for day, n := range seen {
				assert.Equalf(t, 1, n, "%s: %s fired %d times, want exactly 1", z.zone, day, n)
			}
			assert.Lenf(t, seen, 6, "%s: six advances must land on six distinct calendar days, got %v",
				z.zone, seen)
		})
	}
}

// FINDING B. WeeklyIn documents that "the day, hour and minute are interpreted in
// loc, honouring that location's DST rules". The same backward normalization moves
// the instant into the PREVIOUS calendar day, so a Sunday schedule fires on
// Saturday — a weekend maintenance window running a day early, once a year.
func TestWeeklyIn_FiresOnTheRequestedCalendarWeekday(t *testing.T) {
	for _, z := range midnightSpringForwardZones {
		t.Run(z.zone, func(t *testing.T) {
			loc := loadZone(t, z.zone)
			for day := time.Sunday; day <= time.Saturday; day++ {
				s := WeeklyIn(loc, day, 0, 0)
				cur := time.Date(z.y, time.Month(z.m), z.d-9, 8, 0, 0, 0, loc)
				for range 3 {
					cur = s.Next(cur)
					assert.Equalf(t, day, cur.In(loc).Weekday(),
						"WeeklyIn(%s, %s 00:00) fired at %s, a %s",
						z.zone, day, cur.In(loc).Format(time.RFC3339), cur.In(loc).Weekday())
				}
			}
		})
	}
}

// The CONTRACT, stated positively and pinned with instants derived from a
// second-by-second brute-force scan of the real time package rather than from
// belief about what time.Date does: DailyIn fires at the EARLIEST instant on that
// calendar day whose local clock reads at or after hour:minute.
//
//   - the wall time exists once  -> that instant
//   - it does not exist (gap)    -> the instant the clock jumps over it
//   - it exists twice (fall back) -> the FIRST, earlier occurrence
//
// time.Date resolves none of these three reliably: it answers 01:30 EST for the
// New York gap (an hour EARLY), 02:45 for the Lord Howe gap (a value on neither
// side of it), and the SECOND occurrence for the Berlin and Lord Howe repeats.
func TestDailyIn_FiresAtTheEarliestRealInstantAtOrAfterTheWallTime(t *testing.T) {
	for _, c := range []struct {
		name      string
		zone      string
		hour, min int
		fromY     int
		fromM     time.Month
		fromD     int
		wantUTC   string
		wantInLoc string
	}{
		{
			name: "spring forward gap fires when the clock reaches the wall time",
			zone: "America/New_York", hour: 2, min: 30,
			fromY: 2026, fromM: time.March, fromD: 7,
			wantUTC: "2026-03-08T07:00:00Z", wantInLoc: "2026-03-08 03:00:00 -0400 EDT",
		},
		{
			name: "midnight spring forward stays on its own calendar day",
			zone: "America/Santiago", hour: 0, min: 0,
			fromY: 2024, fromM: time.September, fromD: 7,
			wantUTC: "2024-09-08T04:00:00Z", wantInLoc: "2024-09-08 01:00:00 -0300 -03",
		},
		{
			name: "midnight spring forward in a northern-hemisphere zone",
			zone: "America/Havana", hour: 0, min: 0,
			fromY: 2024, fromM: time.March, fromD: 9,
			wantUTC: "2024-03-10T05:00:00Z", wantInLoc: "2024-03-10 01:00:00 -0400 CDT",
		},
		{
			name: "a thirty-minute DST shift is not assumed to be an hour",
			zone: "Australia/Lord_Howe", hour: 2, min: 15,
			fromY: 2026, fromM: time.October, fromD: 3,
			wantUTC: "2026-10-03T15:30:00Z", wantInLoc: "2026-10-04 02:30:00 +1100 +11",
		},
		{
			name: "fall back fires the FIRST of the two occurrences",
			zone: "Europe/Berlin", hour: 2, min: 30,
			fromY: 2026, fromM: time.October, fromD: 24,
			wantUTC: "2026-10-25T00:30:00Z", wantInLoc: "2026-10-25 02:30:00 +0200 CEST",
		},
		{
			name: "a thirty-minute fall back also fires the first occurrence",
			zone: "Australia/Lord_Howe", hour: 1, min: 45,
			fromY: 2026, fromM: time.April, fromD: 4,
			wantUTC: "2026-04-04T14:45:00Z", wantInLoc: "2026-04-05 01:45:00 +1100 +11",
		},
		{
			name: "a gap that time.Date happens to normalize forward is unchanged",
			zone: "Africa/Cairo", hour: 0, min: 0,
			fromY: 2023, fromM: time.April, fromD: 27,
			wantUTC: "2023-04-27T22:00:00Z", wantInLoc: "2023-04-28 01:00:00 +0300 EEST",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			loc := loadZone(t, c.zone)
			want, err := time.Parse(time.RFC3339, c.wantUTC)
			require.NoError(t, err)

			from := time.Date(c.fromY, c.fromM, c.fromD, 12, 0, 0, 0, loc)
			got := DailyIn(loc, c.hour, c.min).Next(from)

			assert.Truef(t, want.Equal(got), "want %s (%s), got %s (%s)",
				want.Format(time.RFC3339), c.wantInLoc,
				got.UTC().Format(time.RFC3339), got.In(loc).Format("2006-01-02 15:04:05 -0700 MST"))
			assert.Equal(t, c.wantInLoc, got.In(loc).Format("2006-01-02 15:04:05 -0700 MST"))
		})
	}
}

// MINIMALITY, swept rather than spot-checked. Every fire must be the EARLIEST
// instant satisfying the contract: one second earlier must be either on an
// earlier calendar day or before the requested wall time. This is what
// distinguishes "fires when the clock reaches 02:30" from "fires an hour late",
// and no fixture in the repo asserted it.
func TestDailyIn_EveryFireIsTheEarliestQualifyingInstant(t *testing.T) {
	zones := []string{
		"America/New_York", "Europe/Berlin", "Australia/Sydney", "Australia/Lord_Howe",
		"Africa/Cairo", "Pacific/Chatham", "America/Santiago", "America/Havana",
		"Atlantic/Azores", "America/Asuncion", "UTC",
	}
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			loc := loadZone(t, zone)
			for _, hm := range [][2]int{{0, 0}, {0, 30}, {1, 45}, {2, 15}, {2, 30}, {3, 0}, {23, 30}} {
				s := DailyIn(loc, hm[0], hm[1])
				// Walk two full years so every transition in both directions is crossed.
				cur := time.Date(2026, 1, 1, 12, 0, 0, 0, loc)
				prevDay := ""
				for range 730 {
					next := s.Next(cur)
					label := fmt.Sprintf("%s %02d:%02d from %s", zone, hm[0], hm[1], cur.Format(time.RFC3339))

					require.Truef(t, next.After(cur), "%s: Next did not advance (got %s)", label, next)

					l := next.In(loc)
					// The fire is at or after the requested wall time on its own day.
					assert.Truef(t, l.Hour() > hm[0] || (l.Hour() == hm[0] && l.Minute() >= hm[1]),
						"%s: fired at %s, before the requested wall time", label, l.Format("15:04:05 -0700"))

					// Minimality: one second earlier must not also qualify on that day.
					before := next.Add(-time.Second).In(loc)
					sameDay := before.Year() == l.Year() && before.Month() == l.Month() && before.Day() == l.Day()
					stillAfterWall := before.Hour() > hm[0] || (before.Hour() == hm[0] && before.Minute() >= hm[1])
					assert.Falsef(t, sameDay && stillAfterWall,
						"%s: fired at %s but %s also qualifies — the fire is not the earliest qualifying instant",
						label, l.Format("2006-01-02 15:04:05 -0700"), before.Format("2006-01-02 15:04:05 -0700"))

					// Exactly one fire per calendar day: days must strictly increase.
					day := l.Format("2006-01-02")
					assert.NotEqualf(t, prevDay, day, "%s: fired twice on %s", label, day)
					prevDay = day

					cur = next
				}
			}
		})
	}
}

// The weekly contract mirrors the daily one and adds the calendar weekday.
func TestWeeklyIn_EveryFireIsOnTheRequestedWeekdayAndAdvances(t *testing.T) {
	zones := []string{
		"America/New_York", "Europe/Berlin", "Australia/Lord_Howe",
		"America/Santiago", "America/Havana", "Atlantic/Azores", "UTC",
	}
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			loc := loadZone(t, zone)
			for day := time.Sunday; day <= time.Saturday; day++ {
				for _, hm := range [][2]int{{0, 0}, {2, 30}, {23, 45}} {
					s := WeeklyIn(loc, day, hm[0], hm[1])
					cur := time.Date(2026, 1, 1, 9, 0, 0, 0, loc)
					for range 110 {
						next := s.Next(cur)
						require.Truef(t, next.After(cur),
							"WeeklyIn(%s, %s %02d:%02d).Next(%s) did not advance",
							zone, day, hm[0], hm[1], cur.Format(time.RFC3339))
						l := next.In(loc)
						assert.Equalf(t, day, l.Weekday(),
							"WeeklyIn(%s, %s %02d:%02d) fired on %s (%s)",
							zone, day, hm[0], hm[1], l.Weekday(), l.Format(time.RFC3339))
						// Consecutive fires are exactly seven calendar days apart.
						assert.Truef(t, next.Sub(cur) <= 8*24*time.Hour,
							"WeeklyIn(%s, %s) skipped a week: %s -> %s", zone, day,
							cur.Format(time.RFC3339), next.Format(time.RFC3339))
						cur = next
					}
				}
			}
		})
	}
}

// The instant DailyIn/WeeklyIn return is FACED IN loc, not in UTC and not in the
// caller's zone. That is not cosmetic: UPGRADE.md documents that these
// constructors "produce boundaries in *their* location", and the SQLite storage
// path compares the rendered face lexically — a boundary handed back on a foreign
// face is the shape that once made a schedule silently never fire.
//
// FALSE-GREEN TRAP: every other assertion in this file compares instants, and
// time.Time comparison ignores the face entirely, so returning best.UTC() keeps
// all of them green.
func TestDailyInWeeklyIn_ReturnBoundariesFacedInTheirOwnLocation(t *testing.T) {
	ny := loadZone(t, "America/New_York")
	from := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	assert.Same(t, ny, DailyIn(ny, 9, 0).Next(from).Location(),
		"DailyIn must return its boundary faced in loc")
	assert.Same(t, ny, WeeklyIn(ny, time.Tuesday, 9, 0).Next(from).Location(),
		"WeeklyIn must return its boundary faced in loc")

	// And the UTC constructors keep answering in UTC.
	assert.Same(t, time.UTC, Daily(9, 0).Next(from).Location())
	assert.Same(t, time.UTC, Weekly(time.Tuesday, 9, 0).Next(from).Location())

	// A spring-forward boundary is faced in loc too — that path builds the instant
	// from a different source than the ordinary one.
	santiago := loadZone(t, "America/Santiago")
	assert.Same(t, santiago, DailyIn(santiago, 0, 0).
		Next(time.Date(2024, 9, 7, 12, 0, 0, 0, santiago)).Location())
}

// A calendar day can hold NO instant at or after the requested reading: a
// location that jumps the clock across midnight skips the tail of the day, and
// one that crosses the date line can skip the day outright. Such a day has no
// fire, and — the part that bites — the fire must not slide onto the FOLLOWING
// day, which already has its own and would then run twice.
func TestDailyInWeeklyIn_SkipACalendarDayThatCannotHoldTheFire(t *testing.T) {
	t.Run("tail of the day skipped", func(t *testing.T) {
		// Africa/Algiers jumped 1971-04-25 23:00 straight to 1971-04-26 00:00, so
		// 23:30 never arrives on the 25th.
		algiers := loadZone(t, "Africa/Algiers")
		s := DailyIn(algiers, 23, 30)

		cur := time.Date(1971, 4, 23, 12, 0, 0, 0, algiers)
		seen := map[string]int{}
		for range 5 {
			cur = s.Next(cur)
			seen[cur.In(algiers).Format("2006-01-02")]++
		}
		for day, n := range seen {
			assert.Equalf(t, 1, n, "%s fired %d times, want exactly 1", day, n)
		}
		assert.Zerof(t, seen["1971-04-25"],
			"1971-04-25 has no instant at or after 23:30 in Africa/Algiers, so it must hold no fire: %v", seen)
		assert.Equalf(t, 1, seen["1971-04-26"],
			"the skipped day's fire must not slide onto 1971-04-26, which has its own: %v", seen)
	})

	t.Run("whole day skipped", func(t *testing.T) {
		// Pacific/Kwajalein crossed the date line and skipped Saturday 1993-08-21
		// entirely. A Saturday schedule must wait a week, not fire on the Sunday.
		kwajalein := loadZone(t, "Pacific/Kwajalein")
		s := WeeklyIn(kwajalein, time.Saturday, 0, 0)

		cur := time.Date(1993, 8, 10, 12, 0, 0, 0, kwajalein)
		for range 4 {
			cur = s.Next(cur)
			l := cur.In(kwajalein)
			assert.Equalf(t, time.Saturday, l.Weekday(),
				"a Saturday schedule fired on %s (%s)", l.Weekday(), l.Format(time.RFC3339))
			assert.NotEqualf(t, "1993-08-21", l.Format("2006-01-02"),
				"1993-08-21 does not exist in Pacific/Kwajalein")
		}
	})
}

// Next must return an instant strictly after its argument for ANY hour/minute,
// including out-of-range ones. This needs no DST at all: an hour of -1 made the
// old form a fixed point in plain UTC, because it rebuilt the reading on the
// following calendar day, that reading normalized back onto the current one, and
// the result was returned without re-checking that it had advanced. That is the
// same ~10Hz ClaimScheduledFire busy-loop schedule.Every panics to avoid.
//
// Out-of-range fields keep normalizing exactly as time.Date normalizes them, so
// DailyIn(loc, 25, 0) still means 01:00 the following day.
func TestDailyInWeeklyIn_AdvanceForOutOfRangeHourAndMinute(t *testing.T) {
	ny := loadZone(t, "America/New_York")
	for _, loc := range []*time.Location{time.UTC, ny} {
		for _, hm := range [][2]int{{-1, 0}, {0, -30}, {25, 0}, {0, 90}, {23, 75}, {48, 30}} {
			label := fmt.Sprintf("%s h=%d m=%d", loc, hm[0], hm[1])

			cur := time.Date(2026, 1, 1, 12, 0, 0, 0, loc)
			for i := range 5 {
				next := DailyIn(loc, hm[0], hm[1]).Next(cur)
				require.Truef(t, next.After(cur), "%s: daily step %d did not advance (%s -> %s)",
					label, i, cur.Format(time.RFC3339), next.Format(time.RFC3339))
				cur = next
			}

			cur = time.Date(2026, 1, 1, 12, 0, 0, 0, loc)
			for i := range 5 {
				next := WeeklyIn(loc, time.Monday, hm[0], hm[1]).Next(cur)
				require.Truef(t, next.After(cur), "%s: weekly step %d did not advance (%s -> %s)",
					label, i, cur.Format(time.RFC3339), next.Format(time.RFC3339))
				cur = next
			}
		}
	}

	// The normalization itself is unchanged: 25:00 is 01:00 the next day.
	assert.Equal(t,
		time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC),
		DailyIn(time.UTC, 25, 0).Next(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
		"an out-of-range hour must normalize exactly as time.Date normalizes it")
}
