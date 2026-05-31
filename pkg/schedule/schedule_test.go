package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvery(t *testing.T) {
	s := Every(5 * time.Minute)
	now := time.Date(2024, 1, 1, 12, 2, 30, 0, time.UTC)
	next := s.Next(now)

	assert.Equal(t, time.Date(2024, 1, 1, 12, 5, 0, 0, time.UTC), next)
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

func TestEvery_NextAlignsToIntervalBoundary(t *testing.T) {
	s := Every(5 * time.Minute)
	fromA := time.Date(2024, 1, 1, 12, 2, 10, 0, time.UTC)
	fromB := time.Date(2024, 1, 1, 12, 4, 59, 0, time.UTC)

	want := time.Date(2024, 1, 1, 12, 5, 0, 0, time.UTC)
	assert.Equal(t, want, s.Next(fromA))
	assert.Equal(t, want, s.Next(fromB))
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
	s := Weekly(time.Friday, 17, 0)                     // Friday 5 PM
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // Monday

	next := s.Next(from)
	assert.Equal(t, time.Date(2024, 1, 5, 17, 0, 0, 0, time.UTC), next)
}

func TestCron(t *testing.T) {
	s, err := Cron("0 9 * * *") // Every day at 9 AM
	assert.NoError(t, err)
	from := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	next := s.Next(from)

	assert.Equal(t, 9, next.Hour())
	assert.Equal(t, 0, next.Minute())
}

func TestCron_MultipleFields(t *testing.T) {
	s, err := Cron("30 14 * * 1-5") // 2:30 PM on weekdays
	assert.NoError(t, err)
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // Monday

	next := s.Next(from)
	assert.Equal(t, 14, next.Hour())
	assert.Equal(t, 30, next.Minute())
}

func TestCron_InvalidExpression_ReturnsError(t *testing.T) {
	s, err := Cron("invalid cron")
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestMustCron_InvalidExpression_Panics(t *testing.T) {
	assert.Panics(t, func() {
		MustCron("invalid cron")
	})
}

func TestCron_UsesUTC(t *testing.T) {
	t.Setenv("TZ", "America/Los_Angeles")
	origLocal := time.Local
	defer func() { time.Local = origLocal }()
	time.Local = time.FixedZone("PST", -8*60*60)

	cronSched, err := Cron("0 9 * * *")
	assert.NoError(t, err)
	dailySched := Daily(9, 0)
	from := time.Date(2024, 1, 1, 8, 30, 0, 0, time.UTC)

	assert.Equal(t, dailySched.Next(from), cronSched.Next(from))
}

func TestScheduleInterface(t *testing.T) {
	// All schedule types implement Schedule interface
	var _ Schedule = Every(time.Minute)        //nolint:staticcheck // interface conformance check
	var _ Schedule = Daily(9, 0)               //nolint:staticcheck // interface conformance check
	var _ Schedule = Weekly(time.Monday, 9, 0) //nolint:staticcheck // interface conformance check
	var _ Schedule = MustCron("* * * * *")     //nolint:staticcheck // interface conformance check
}
