package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestEvery_NonPositiveInterval_Panics covers the construction-time rejection of
// a non-positive interval (teardown g8): a zero/negative interval would drive a
// ~10Hz claim busy-loop, so Every panics rather than building such a schedule.
func TestEvery_NonPositiveInterval_Panics(t *testing.T) {
	assert.Panics(t, func() { Every(0) })
	assert.Panics(t, func() { Every(-5 * time.Minute) })
}

// TestEverySchedule_NonPositiveInterval_NextDoesNotBusyLoop covers the
// defense-in-depth guard in Next for a directly-constructed everySchedule: it
// must NOT return `from` (which would busy-loop the scheduler) but a far-future
// time that effectively disables the schedule.
func TestEverySchedule_NonPositiveInterval_NextDoesNotBusyLoop(t *testing.T) {
	s := &everySchedule{interval: 0}
	from := time.Date(2024, 3, 15, 8, 17, 42, 0, time.UTC)

	next := s.Next(from)
	assert.True(t, next.After(from.AddDate(50, 0, 0)), "non-positive interval must not busy-loop; got %v", next)
}

// TestWeekly_TargetDayEarlierInWeek covers the daysUntil < 0 wrap-around branch
// in weeklySchedule.Next: the target weekday occurs earlier in the week than the
// current day, so daysUntil must wrap by adding 7.
func TestWeekly_TargetDayEarlierInWeek(t *testing.T) {
	s := Weekly(time.Monday, 10, 0)
	// 2024-01-03 is a Wednesday. Monday (1) - Wednesday (3) = -2, which must
	// wrap to +5 days -> the following Monday 2024-01-08.
	from := time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC)

	next := s.Next(from)
	assert.Equal(t, time.Date(2024, 1, 8, 10, 0, 0, 0, time.UTC), next)
	assert.Equal(t, time.Monday, next.Weekday())
}

// TestWeekly_SundayTargetFromSaturday is another wrap-around case: Sunday (0)
// minus Saturday (6) = -6, wrapping to the next day.
func TestWeekly_SundayTargetFromSaturday(t *testing.T) {
	s := Weekly(time.Sunday, 8, 30)
	// 2024-01-06 is a Saturday.
	from := time.Date(2024, 1, 6, 9, 0, 0, 0, time.UTC)

	next := s.Next(from)
	assert.Equal(t, time.Date(2024, 1, 7, 8, 30, 0, 0, time.UTC), next)
	assert.Equal(t, time.Sunday, next.Weekday())
}

// TestMustCron_ValidExpression covers the happy path of MustCron, which the
// existing test only exercises via the panic branch.
func TestMustCron_ValidExpression(t *testing.T) {
	var s Schedule
	assert.NotPanics(t, func() {
		s = MustCron("0 9 * * *")
	})
	assert.NotNil(t, s)

	from := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	next := s.Next(from)
	assert.Equal(t, 9, next.Hour())
	assert.Equal(t, 0, next.Minute())
}
