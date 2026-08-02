package schedule

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule defines when a job should run next.
type Schedule interface {
	Next(from time.Time) time.Time
}

// everySchedule runs at fixed intervals.
type everySchedule struct {
	interval time.Duration
}

// Every creates a schedule that runs at fixed intervals. The interval must be
// positive; like time.NewTicker, Every panics on a non-positive duration. A
// zero or negative interval is a configuration error that would otherwise make
// Next return its input unchanged, driving a ~10Hz ClaimScheduledFire busy-loop.
func Every(d time.Duration) Schedule {
	if d <= 0 {
		panic("jobs: schedule.Every requires a positive interval")
	}
	return &everySchedule{interval: d}
}

func (s *everySchedule) Next(from time.Time) time.Time {
	if s.interval <= 0 {
		// Defense in depth — Every rejects this at construction. Never return
		// `from` (which busy-loops the scheduler); a non-positive interval here
		// effectively disables the schedule.
		return from.AddDate(100, 0, 0)
	}
	return from.Truncate(s.interval).Add(s.interval)
}

// wallProbeSpan bounds how far either side of a requested clock reading a
// location's UTC offset is sampled. Every UTC offset in the IANA database lies
// within (-13h, +15h), so an instant this far either side of the reading — read
// as if the reading were UTC — is outside every possible resolution of it.
const wallProbeSpan = 26 * time.Hour

// maxWallOffsets caps the offset closure below. A location has a small, fixed
// number of distinct UTC offsets in effect around any one instant (two, at a DST
// transition); the cap only exists so a malformed location cannot spin.
const maxWallOffsets = 8

// maxCalendarProbes bounds the walk over candidate calendar days (or weeks) in
// Next. Two are enough in every ordinary case — the fire for `from`'s own day has
// already passed, so the following day's is next. More are needed only when a
// location has no instant at all on a calendar day, which happens when it crosses
// the date line: Pacific/Kwajalein skipped 1993-08-21 outright.
const maxCalendarProbes = 16

// firstInstantAtOrAfterWall returns the EARLIEST instant whose clock reading in
// loc has reached the reading (y, mo, d, hour, minute), and reports whether that
// instant is still on the requested calendar day.
//
// This is the whole DST contract of DailyIn and WeeklyIn, and it is resolved by
// construction because time.Date resolves none of its three cases reliably. For
// a reading inside a spring-forward gap time.Date can answer an hour EARLY
// (America/New_York 02:30 -> 01:30 EST), a reading on NEITHER side of the gap
// (Australia/Lord_Howe 02:15 -> 02:45), or an instant on the PREVIOUS CALENDAR
// DAY (America/Santiago 00:00 -> 23:00 the day before — which made Next return
// its own input, a fixed point that wedges the scheduler cursor). For a reading
// that occurs twice it can answer either occurrence: the first in
// America/New_York, the second in Europe/Berlin.
//
// Every candidate here is VALIDATED against loc before it is considered, so a
// candidate that does not survive is simply dropped: probing too widely is
// harmless, probing too narrowly would miss an occurrence.
func firstInstantAtOrAfterWall(loc *time.Location, y int, mo time.Month, d, hour, minute int) (time.Time, bool) {
	// The requested reading, read as if it were UTC. time.Date normalizes
	// out-of-range fields here exactly as it did when the reading was built
	// directly in loc, so DailyIn(loc, 25, 0) keeps meaning 01:00 the next day.
	wall := time.Date(y, mo, d, hour, minute, 0, 0, time.UTC)

	// resolve reads the reading under one candidate UTC offset and returns the
	// instant FACED IN loc. The face matters beyond cosmetics: DailyIn/WeeklyIn
	// boundaries are documented to be produced in their own location, and the
	// storage layer's lexical comparisons were designed around that.
	resolve := func(off int) time.Time {
		return wall.Add(-time.Duration(off) * time.Second).In(loc)
	}

	// Fast path, and the only path for a fixed-offset location such as UTC: when
	// the zone period containing the reading's obvious resolution spans the whole
	// probe window there is no transition anywhere near, so the reading exists
	// exactly once and that resolution is it. ZoneBounds reports that period's
	// real bounds, so this is a proof rather than a sample — but it only reports
	// them for a time faced in loc, which is why every instant here is faced first.
	_, off := wall.In(loc).Zone()
	if c := resolve(off); wallReadsExactly(c, wall) {
		start, end := c.ZoneBounds()
		if (start.IsZero() || !start.After(wall.Add(-wallProbeSpan))) &&
			(end.IsZero() || !end.Before(wall.Add(wallProbeSpan))) {
			return c, true
		}
	}

	// Sample every UTC offset in effect around the reading, then close the set
	// under "the offset actually in effect at the instant this offset implies".
	offsets := make([]int, 0, maxWallOffsets)
	addOffset := func(off int) {
		for _, existing := range offsets {
			if existing == off {
				return
			}
		}
		if len(offsets) < maxWallOffsets {
			offsets = append(offsets, off)
		}
	}
	for _, delta := range []time.Duration{-wallProbeSpan, -wallProbeSpan / 2, 0, wallProbeSpan / 2, wallProbeSpan} {
		_, sampled := wall.Add(delta).In(loc).Zone()
		addOffset(sampled)
	}
	for i := 0; i < len(offsets); i++ {
		_, sampled := resolve(offsets[i]).Zone()
		addOffset(sampled)
	}

	// Each offset implies the one instant that would read exactly `wall` under it.
	// That instant sits in some zone period, and a period's own bounds are the
	// only other instants at which the clock can FIRST reach the reading: when a
	// gap swallows the reading the clock reaches it by jumping, and a jump only
	// ever happens at a period boundary.
	var best time.Time
	consider := func(t time.Time) {
		if t.IsZero() || !wallReached(t, wall) {
			return
		}
		if best.IsZero() || t.Before(best) {
			best = t
		}
	}
	for _, o := range offsets {
		c := resolve(o)
		// ZoneBounds answers for the location t is FACED in, so a UTC-faced
		// instant would report no bounds at all however many transitions loc has.
		start, end := c.ZoneBounds()
		consider(c)
		consider(start.In(loc))
		consider(end.In(loc))
	}
	if best.IsZero() {
		return time.Time{}, false
	}

	// The earliest instant that reaches the reading can be on a LATER calendar
	// day — a date-line crossing can swallow the rest of the day, or all of it.
	// Such a day holds no fire at all and the caller moves on to the next one.
	if ly, lm, ld := best.Date(); ly != wall.Year() || lm != wall.Month() || ld != wall.Day() {
		return time.Time{}, false
	}
	return best, true
}

// wallReadsExactly reports whether t's own clock reading is exactly `wall`. t is
// expected to be faced in the schedule's location already.
func wallReadsExactly(t, wall time.Time) bool {
	ly, lm, ld := t.Date()
	wy, wm, wd := wall.Date()
	return ly == wy && lm == wm && ld == wd && t.Hour() == wall.Hour() && t.Minute() == wall.Minute()
}

// wallReached reports whether t's own clock reading has reached `wall`, comparing
// the calendar date first and the clock time second. Seconds are not compared: a
// reading names a minute, and the clock has reached it as soon as that minute
// begins. t is expected to be faced in the schedule's location already.
func wallReached(t, wall time.Time) bool {
	l := t
	ly, lm, ld := l.Date()
	wy, wm, wd := wall.Date()
	switch {
	case ly != wy:
		return ly > wy
	case lm != wm:
		return lm > wm
	case ld != wd:
		return ld > wd
	case l.Hour() != wall.Hour():
		return l.Hour() > wall.Hour()
	default:
		return l.Minute() >= wall.Minute()
	}
}

// dailySchedule runs at a specific time each day.
type dailySchedule struct {
	hour   int
	minute int
	loc    *time.Location
}

// Daily creates a schedule that runs at a specific time each day. The hour and
// minute are interpreted in UTC, not the host's local timezone.
func Daily(hour, minute int) Schedule {
	return DailyIn(time.UTC, hour, minute)
}

// DailyIn is Daily with the hour and minute interpreted in loc, honouring that
// location's DST rules — DailyIn(newYork, 9, 0) fires at 09:00 New York time
// year-round, not at a fixed UTC offset. loc must be non-nil.
//
// Exactly one fire per calendar day at both DST edges. The fire is the EARLIEST
// instant on that calendar day whose clock in loc has reached hour:minute:
//
//   - the reading exists once — the usual case — and the fire is at it;
//   - the reading does not exist because the clock jumped over it (spring
//     forward), and the fire is at the instant of the jump. DailyIn(newYork, 2, 30)
//     fires at 03:00 EDT on a US spring-forward day, and a schedule at a reading
//     the clock jumps over at MIDNIGHT stays on its own calendar day rather than
//     moving to the day before;
//   - the reading exists twice (fall back), and the FIRST occurrence fires.
//
// Do not use time.Date to predict any of these: it resolves the first case only.
// For a reading inside a gap it can answer an hour early, an instant on neither
// side of the gap, or an instant on the previous calendar day, and for a repeated
// reading it answers different occurrences in different zones.
//
// Next always returns an instant strictly after its argument, so a schedule can
// never stall. Cron with a CRON_TZ= prefix differs — robfig skips a nonexistent
// reading entirely and fires twice on a repeated one — so prefer DailyIn when
// "once a day, no matter what" is what you want.
func DailyIn(loc *time.Location, hour, minute int) Schedule {
	if loc == nil {
		panic("jobs: schedule.DailyIn requires a non-nil location")
	}
	return &dailySchedule{hour: hour, minute: minute, loc: loc}
}

func (s *dailySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)
	y, mo, d := from.Date()
	// Walk calendar DAYS, not instants. The two differ in a DST zone, and the day
	// is the unit the contract is written in: rebuilding from the calendar day is
	// what keeps a normalized clock reading from being carried forward and firing
	// twice on the following day. Identical in UTC. A day is skipped only when it
	// holds no instant at or after the reading at all.
	for range maxCalendarProbes {
		if next, ok := firstInstantAtOrAfterWall(s.loc, y, mo, d, s.hour, s.minute); ok && next.After(from) {
			return next
		}
		d++
	}
	// Defense in depth, as in everySchedule.Next: never return a value at or
	// before `from`. That fixed point is what drives a ~10Hz ClaimScheduledFire
	// busy-loop, and it is exactly what a midnight spring-forward used to produce.
	return from.AddDate(100, 0, 0)
}

// weeklySchedule runs at a specific day and time each week.
type weeklySchedule struct {
	day    time.Weekday
	hour   int
	minute int
	loc    *time.Location
}

// Weekly creates a schedule that runs at a specific day and time each week. The
// day, hour, and minute are interpreted in UTC, not the host's local timezone.
func Weekly(day time.Weekday, hour, minute int) Schedule {
	return WeeklyIn(time.UTC, day, hour, minute)
}

// WeeklyIn is Weekly with the day, hour and minute interpreted in loc, honouring
// that location's DST rules. loc must be non-nil; the DST notes on DailyIn apply.
//
// The fire always lands on the requested CALENDAR weekday in loc. That is not
// automatic: resolving a reading the clock jumps over at midnight with time.Date
// moves the instant into the previous day, so a Sunday schedule used to fire on
// Saturday once a year in the zones whose spring-forward is at midnight.
func WeeklyIn(loc *time.Location, day time.Weekday, hour, minute int) Schedule {
	if loc == nil {
		panic("jobs: schedule.WeeklyIn requires a non-nil location")
	}
	return &weeklySchedule{day: day, hour: hour, minute: minute, loc: loc}
}

func (s *weeklySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)
	y, mo, d := from.Date()

	daysUntil := int(s.day - from.Weekday())
	if daysUntil < 0 {
		daysUntil += 7
	}
	d += daysUntil

	// Roll seven calendar days at a time, not the instant — see dailySchedule.Next.
	for range maxCalendarProbes {
		if next, ok := firstInstantAtOrAfterWall(s.loc, y, mo, d, s.hour, s.minute); ok && next.After(from) {
			return next
		}
		d += 7
	}
	return from.AddDate(100, 0, 0)
}

// cronSchedule wraps a cron expression.
type cronSchedule struct {
	schedule cron.Schedule
}

// cronParser is the 5-field parser (minute hour day-of-month month day-of-week)
// every cron constructor here uses. cron.Parser is an immutable value holding
// only its option bits, so one package-level instance is safe to share.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// cronTZPrefixes are the crontab-style timezone prefixes an expression may
// carry. Matching is case-sensitive, as in crontab and in robfig/cron.
var cronTZPrefixes = []string{"CRON_TZ=", "TZ="}

func hasCronTZPrefix(expr string) bool {
	trimmed := strings.TrimSpace(expr)
	for _, p := range cronTZPrefixes {
		if strings.HasPrefix(trimmed, p) {
			return true
		}
	}
	return false
}

// splitCronTZ separates an optional CRON_TZ=/TZ= prefix from a cron expression,
// returning the named location (nil when absent) and the bare expression.
//
// robfig/cron understands this prefix itself, but the split has to happen here
// for two reasons. The location it derives is exactly what Cron used to
// overwrite with UTC, so the schedule fired at the wrong hour with no error. And
// a prefix not followed by a space makes robfig slice with a negative index and
// PANIC — unacceptable in a constructor that returns an error.
func splitCronTZ(expr string) (*time.Location, string, error) {
	trimmed := strings.TrimSpace(expr)
	name := ""
	prefixed := false
	for _, p := range cronTZPrefixes {
		if strings.HasPrefix(trimmed, p) {
			name, prefixed = trimmed[len(p):], true
			break
		}
	}
	if !prefixed {
		return nil, expr, nil
	}
	sep := strings.IndexAny(name, " \t")
	if sep < 0 {
		return nil, "", fmt.Errorf("jobs: cron expression %q carries a timezone but no schedule fields", expr)
	}
	rest := strings.TrimSpace(name[sep:])
	name = name[:sep]
	if name == "" {
		return nil, "", fmt.Errorf("jobs: cron expression %q carries an empty timezone name", expr)
	}
	// A SECOND timezone prefix is ambiguous and must not be resolved by picking
	// one. robfig's parser understands TZ=/CRON_TZ= itself, so it would strip the
	// inner name and set the location from it — and parseCronIn then overwrites
	// that with the OUTER name. The result is that one of the two timezones the
	// caller wrote is silently discarded and the job fires in the other, with no
	// error and no log line. Rejecting is the only answer that cannot be wrong.
	for _, p := range cronTZPrefixes {
		if strings.HasPrefix(rest, p) {
			return nil, "", fmt.Errorf(
				"jobs: cron expression %q names more than one timezone; use exactly one CRON_TZ= or TZ= prefix", expr)
		}
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, "", fmt.Errorf("jobs: cron expression %q names an unknown timezone %q: %w", expr, name, err)
	}
	return loc, rest, nil
}

// parseCronIn parses a bare expression and pins evaluation to loc. Pinning is
// NOT optional: robfig defaults an unpinned schedule to the host's local
// timezone, which this package never uses.
func parseCronIn(expr string, loc *time.Location) (Schedule, error) {
	parsed, err := cronParser.Parse(expr)
	if err != nil {
		return nil, err
	}
	if spec, ok := parsed.(*cron.SpecSchedule); ok {
		spec.Location = loc
	}
	return &cronSchedule{schedule: parsed}, nil
}

// Cron creates a schedule from a 5-field cron expression
// (minute hour day-of-month month day-of-week), evaluated in UTC by default —
// not the host's local timezone.
//
// An expression may carry an explicit crontab-style timezone prefix —
// "CRON_TZ=America/New_York 0 9 * * *" or "TZ=Europe/Berlin 0 9 * * *" — and is
// then evaluated in that location, honouring its DST rules, so the job fires at
// 09:00 local time year-round. Previously the prefix was parsed and then
// silently discarded, firing hours off with no error. The name is resolved with
// time.LoadLocation, so the host needs the IANA database (import _ "time/tzdata"
// to embed a copy); an unresolvable name is an error, never a silent fallback.
//
// Use CronIn to supply a location without embedding it in the expression.
func Cron(expr string) (Schedule, error) {
	loc, bare, err := splitCronTZ(expr)
	if err != nil {
		return nil, err
	}
	if loc == nil {
		loc = time.UTC
	}
	return parseCronIn(bare, loc)
}

// CronIn is Cron with the evaluation location supplied separately, for callers
// holding a *time.Location rather than a name. loc must be non-nil.
//
// expr must be bare: a CRON_TZ=/TZ= prefix would be a second, possibly
// conflicting instruction, so it is rejected rather than silently resolved in
// favour of one of the two.
func CronIn(loc *time.Location, expr string) (Schedule, error) {
	if loc == nil {
		return nil, fmt.Errorf("jobs: schedule.CronIn requires a non-nil location (pass time.UTC for UTC)")
	}
	if hasCronTZPrefix(expr) {
		return nil, fmt.Errorf("jobs: schedule.CronIn: expression %q carries its own CRON_TZ=/TZ= timezone; pass the location or the prefix, not both", expr)
	}
	return parseCronIn(expr, loc)
}

// MustCron creates a schedule from a cron expression and panics if invalid.
func MustCron(expr string) Schedule {
	schedule, err := Cron(expr)
	if err != nil {
		panic("invalid cron expression: " + err.Error())
	}
	return schedule
}

// MustCronIn creates a schedule evaluated in loc and panics if the expression or
// the location is invalid.
func MustCronIn(loc *time.Location, expr string) Schedule {
	schedule, err := CronIn(loc, expr)
	if err != nil {
		panic("invalid cron expression: " + err.Error())
	}
	return schedule
}

func (s *cronSchedule) Next(from time.Time) time.Time {
	return s.schedule.Next(from)
}
