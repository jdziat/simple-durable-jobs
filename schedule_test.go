package jobs_test

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestEvery_CalculatesNextRun(t *testing.T) {
	schedule := jobs.Every(time.Hour)
	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)

	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 11, 30, 0, 0, time.UTC), next)
}

func TestEvery_ShortInterval(t *testing.T) {
	schedule := jobs.Every(5 * time.Minute)
	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)

	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 10, 35, 0, 0, time.UTC), next)
}

func TestEvery_ZeroDuration(t *testing.T) {
	schedule := jobs.Every(0)
	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)

	next := schedule.Next(now)

	// Should return immediate time for zero duration
	assert.Equal(t, now, next)
}

func TestEvery_LongInterval(t *testing.T) {
	schedule := jobs.Every(24 * time.Hour)
	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)

	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 9, 10, 30, 0, 0, time.UTC), next)
}

func TestDaily_CalculatesNextRun(t *testing.T) {
	schedule := jobs.Daily(9, 0)

	// Before 9am
	now := time.Date(2026, 2, 8, 8, 0, 0, 0, time.UTC)
	next := schedule.Next(now)
	assert.Equal(t, time.Date(2026, 2, 8, 9, 0, 0, 0, time.UTC), next)

	// After 9am
	now = time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	next = schedule.Next(now)
	assert.Equal(t, time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC), next)
}

func TestDaily_Midnight(t *testing.T) {
	schedule := jobs.Daily(0, 0)

	now := time.Date(2026, 2, 8, 23, 59, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC), next)
}

func TestDaily_WithMinutes(t *testing.T) {
	schedule := jobs.Daily(14, 30) // 2:30 PM

	now := time.Date(2026, 2, 8, 14, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 14, 30, 0, 0, time.UTC), next)
}

func TestDaily_ExactTime(t *testing.T) {
	schedule := jobs.Daily(9, 0)

	// Exactly at 9am - should schedule for next day
	now := time.Date(2026, 2, 8, 9, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC), next)
}

func TestWeekly_CalculatesNextRun(t *testing.T) {
	schedule := jobs.Weekly(time.Monday, 9, 0)

	// Sunday before Monday
	now := time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC) // Sunday
	next := schedule.Next(now)

	// Next Monday is Feb 9
	assert.Equal(t, time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC), next)
}

func TestWeekly_SameDayBeforeTime(t *testing.T) {
	schedule := jobs.Weekly(time.Monday, 14, 0)

	// Monday before 2pm
	now := time.Date(2026, 2, 9, 10, 0, 0, 0, time.UTC) // Monday
	next := schedule.Next(now)

	// Should be same day at 2pm
	assert.Equal(t, time.Date(2026, 2, 9, 14, 0, 0, 0, time.UTC), next)
}

func TestWeekly_SameDayAfterTime(t *testing.T) {
	schedule := jobs.Weekly(time.Monday, 9, 0)

	// Monday after 9am
	now := time.Date(2026, 2, 9, 10, 0, 0, 0, time.UTC) // Monday
	next := schedule.Next(now)

	// Should be next Monday
	assert.Equal(t, time.Date(2026, 2, 16, 9, 0, 0, 0, time.UTC), next)
}

func TestWeekly_Friday(t *testing.T) {
	schedule := jobs.Weekly(time.Friday, 17, 0) // 5pm Friday

	// Monday
	now := time.Date(2026, 2, 9, 10, 0, 0, 0, time.UTC) // Monday
	next := schedule.Next(now)

	// Friday Feb 13
	assert.Equal(t, time.Date(2026, 2, 13, 17, 0, 0, 0, time.UTC), next)
}

func TestCron_ParsesExpression(t *testing.T) {
	schedule := jobs.Cron("0 9 * * *") // 9am daily

	now := time.Date(2026, 2, 8, 8, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 9, 0, 0, 0, time.UTC), next)
}

func TestCron_EveryHour(t *testing.T) {
	schedule := jobs.Cron("0 * * * *") // Top of every hour

	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 11, 0, 0, 0, time.UTC), next)
}

func TestCron_EveryMinute(t *testing.T) {
	schedule := jobs.Cron("* * * * *") // Every minute

	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 10, 31, 0, 0, time.UTC), next)
}

func TestCron_WeekdaysOnly(t *testing.T) {
	schedule := jobs.Cron("0 9 * * 1-5") // 9am Mon-Fri

	// Saturday
	now := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC) // Saturday
	next := schedule.Next(now)

	// Next Monday Feb 9
	assert.Equal(t, time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC), next)
}

func TestCron_MonthlyFirstDay(t *testing.T) {
	schedule := jobs.Cron("0 0 1 * *") // Midnight on 1st of month

	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), next)
}

func TestCron_InvalidExpression(t *testing.T) {
	// The Cron function should panic on invalid expression
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for invalid cron expression")
		}
	}()

	jobs.Cron("invalid cron expression")
}

func TestCron_SpecificDays(t *testing.T) {
	schedule := jobs.Cron("30 14 * * 1,3,5") // 2:30pm Mon, Wed, Fri

	// Tuesday
	now := time.Date(2026, 2, 10, 10, 0, 0, 0, time.UTC) // Tuesday
	next := schedule.Next(now)

	// Next Wednesday Feb 11
	assert.Equal(t, time.Date(2026, 2, 11, 14, 30, 0, 0, time.UTC), next)
}

func TestCron_MultipleHours(t *testing.T) {
	schedule := jobs.Cron("0 9,12,17 * * *") // 9am, noon, 5pm daily

	now := time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	// Next is noon
	assert.Equal(t, time.Date(2026, 2, 8, 12, 0, 0, 0, time.UTC), next)
}
