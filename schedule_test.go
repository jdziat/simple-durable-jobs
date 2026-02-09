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

func TestCron_ParsesExpression(t *testing.T) {
	schedule := jobs.Cron("0 9 * * *") // 9am daily

	now := time.Date(2026, 2, 8, 8, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 9, 0, 0, 0, time.UTC), next)
}
