package schedule

import (
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
	return &dailySchedule{hour: hour, minute: minute, loc: time.UTC}
}

func (s *dailySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)
	next := time.Date(from.Year(), from.Month(), from.Day(), s.hour, s.minute, 0, 0, s.loc)
	if !next.After(from) {
		next = next.AddDate(0, 0, 1)
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
	return &weeklySchedule{day: day, hour: hour, minute: minute, loc: time.UTC}
}

func (s *weeklySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)

	daysUntil := int(s.day - from.Weekday())
	if daysUntil < 0 {
		daysUntil += 7
	}

	next := time.Date(from.Year(), from.Month(), from.Day()+daysUntil, s.hour, s.minute, 0, 0, s.loc)
	if !next.After(from) {
		next = next.AddDate(0, 0, 7)
	}
	return next
}

// cronSchedule wraps a cron expression.
type cronSchedule struct {
	schedule cron.Schedule
}

// Cron creates a schedule from a 5-field cron expression
// (minute hour day-of-month month day-of-week). The expression is evaluated in
// UTC, not the host's local timezone.
func Cron(expr string) (Schedule, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil, err
	}
	if spec, ok := schedule.(*cron.SpecSchedule); ok {
		spec.Location = time.UTC
	}
	return &cronSchedule{schedule: schedule}, nil
}

// MustCron creates a schedule from a cron expression and panics if invalid.
func MustCron(expr string) Schedule {
	schedule, err := Cron(expr)
	if err != nil {
		panic("invalid cron expression: " + err.Error())
	}
	return schedule
}

func (s *cronSchedule) Next(from time.Time) time.Time {
	return s.schedule.Next(from)
}
