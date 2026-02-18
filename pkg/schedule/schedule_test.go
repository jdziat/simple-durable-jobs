package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvery(t *testing.T) {
	s := Every(5 * time.Minute)
	now := time.Now()
	next := s.Next(now)

	assert.Equal(t, now.Add(5*time.Minute), next)
}

func TestEvery_MultipleNext(t *testing.T) {
	s := Every(time.Hour)
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	next1 := s.Next(start)
	next2 := s.Next(next1)
	next3 := s.Next(next2)

	assert.Equal(t, time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC), next1)
	assert.Equal(t, time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC), next2)
	assert.Equal(t, time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC), next3)
}

func TestDaily(t *testing.T) {
	s := Daily(9, 30) // 9:30 AM UTC
	from := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	next := s.Next(from)

	assert.Equal(t, time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC), next)
}

func TestDaily_NextDay(t *testing.T) {
	s := Daily(9, 30)
	from := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC) // After 9:30
	next := s.Next(from)

	assert.Equal(t, time.Date(2024, 1, 2, 9, 30, 0, 0, time.UTC), next)
}

func TestWeekly(t *testing.T) {
	s := Weekly(time.Monday, 10, 0)
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // Monday

	next := s.Next(from)
	assert.Equal(t, time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), next)
}

func TestWeekly_NextWeek(t *testing.T) {
	s := Weekly(time.Monday, 10, 0)
	from := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC) // Monday after 10:00

	next := s.Next(from)
	assert.Equal(t, time.Date(2024, 1, 8, 10, 0, 0, 0, time.UTC), next)
}

func TestWeekly_DifferentDay(t *testing.T) {
	s := Weekly(time.Friday, 17, 0) // Friday 5 PM
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // Monday

	next := s.Next(from)
	assert.Equal(t, time.Date(2024, 1, 5, 17, 0, 0, 0, time.UTC), next)
}

func TestCron(t *testing.T) {
	s := Cron("0 9 * * *") // Every day at 9 AM
	from := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	next := s.Next(from)

	assert.Equal(t, 9, next.Hour())
	assert.Equal(t, 0, next.Minute())
}

func TestCron_MultipleFields(t *testing.T) {
	s := Cron("30 14 * * 1-5") // 2:30 PM on weekdays
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // Monday

	next := s.Next(from)
	assert.Equal(t, 14, next.Hour())
	assert.Equal(t, 30, next.Minute())
}

func TestCron_InvalidExpression_Panics(t *testing.T) {
	assert.Panics(t, func() {
		Cron("invalid cron")
	})
}

func TestScheduleInterface(t *testing.T) {
	// All schedule types implement Schedule interface
	var _ Schedule = Every(time.Minute)   //nolint:staticcheck // interface conformance check
	var _ Schedule = Daily(9, 0)          //nolint:staticcheck // interface conformance check
	var _ Schedule = Weekly(time.Monday, 9, 0) //nolint:staticcheck // interface conformance check
	var _ Schedule = Cron("* * * * *")   //nolint:staticcheck // interface conformance check
}
