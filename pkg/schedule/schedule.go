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
// Exactly one fire per calendar day at both DST edges. On a spring-forward day
// the named wall time may not exist and time.Date resolves it to a real instant
// just before the gap; on a fall-back day it exists twice and the first
// occurrence fires. Cron with a CRON_TZ= prefix differs — robfig skips a
// nonexistent time and fires twice on a repeated one — so prefer DailyIn when
// "once a day, no matter what" is what you want.
func DailyIn(loc *time.Location, hour, minute int) Schedule {
	if loc == nil {
		panic("jobs: schedule.DailyIn requires a non-nil location")
	}
	return &dailySchedule{hour: hour, minute: minute, loc: loc}
}

func (s *dailySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)
	next := time.Date(from.Year(), from.Month(), from.Day(), s.hour, s.minute, 0, 0, s.loc)
	if !next.After(from) {
		// Roll the calendar DAY, not the resulting instant. In a DST zone the two
		// differ: on a spring-forward day the requested wall time may not exist and
		// time.Date normalizes it (02:30 -> 01:30), so AddDate would carry the
		// normalized 01:30 into the next day and fire TWICE there. Rebuilding from
		// the calendar day keeps exactly one fire per day. Identical in UTC.
		next = time.Date(from.Year(), from.Month(), from.Day()+1, s.hour, s.minute, 0, 0, s.loc)
	}
	return next
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
func WeeklyIn(loc *time.Location, day time.Weekday, hour, minute int) Schedule {
	if loc == nil {
		panic("jobs: schedule.WeeklyIn requires a non-nil location")
	}
	return &weeklySchedule{day: day, hour: hour, minute: minute, loc: loc}
}

func (s *weeklySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)

	daysUntil := int(s.day - from.Weekday())
	if daysUntil < 0 {
		daysUntil += 7
	}

	next := time.Date(from.Year(), from.Month(), from.Day()+daysUntil, s.hour, s.minute, 0, 0, s.loc)
	if !next.After(from) {
		// Roll the calendar day, not the instant — see dailySchedule.Next.
		next = time.Date(from.Year(), from.Month(), from.Day()+daysUntil+7, s.hour, s.minute, 0, 0, s.loc)
	}
	return next
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
