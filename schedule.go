package jobs

import (
	"time"

	"github.com/robfig/cron/v3"
)

// everySchedule runs at fixed intervals.
type everySchedule struct {
	interval time.Duration
}

// Every creates a schedule that runs at fixed intervals.
func Every(d time.Duration) Schedule {
	return &everySchedule{interval: d}
}

func (s *everySchedule) Next(from time.Time) time.Time {
	return from.Add(s.interval)
}

// dailySchedule runs at a specific time each day.
type dailySchedule struct {
	hour   int
	minute int
	loc    *time.Location
}

// Daily creates a schedule that runs at a specific time each day.
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

// Weekly creates a schedule that runs at a specific day and time each week.
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

// Cron creates a schedule from a cron expression.
func Cron(expr string) Schedule {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expr)
	if err != nil {
		panic("invalid cron expression: " + err.Error())
	}
	return &cronSchedule{schedule: schedule}
}

func (s *cronSchedule) Next(from time.Time) time.Time {
	return s.schedule.Next(from)
}
