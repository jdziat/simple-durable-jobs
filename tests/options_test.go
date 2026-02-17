package jobs_test

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestOptions_Queue(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.QueueOpt("emails").Apply(opts)
	assert.Equal(t, "emails", opts.Queue)
}

func TestOptions_Priority(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Priority(100).Apply(opts)
	assert.Equal(t, 100, opts.Priority)
}

func TestOptions_Priority_Negative(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Priority(-10).Apply(opts)
	assert.Equal(t, -10, opts.Priority)
}

func TestOptions_Retries(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Retries(5).Apply(opts)
	assert.Equal(t, 5, opts.MaxRetries)
}

func TestOptions_Retries_Zero(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Retries(0).Apply(opts)
	assert.Equal(t, 0, opts.MaxRetries)
}

func TestOptions_Delay(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Delay(time.Hour).Apply(opts)
	assert.Equal(t, time.Hour, opts.Delay)
}

func TestOptions_Delay_Zero(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Delay(0).Apply(opts)
	assert.Equal(t, time.Duration(0), opts.Delay)
}

func TestOptions_At(t *testing.T) {
	opts := jobs.NewOptions()
	runTime := time.Date(2026, 12, 25, 12, 0, 0, 0, time.UTC)
	jobs.At(runTime).Apply(opts)
	assert.NotNil(t, opts.RunAt)
	assert.Equal(t, runTime, *opts.RunAt)
}

func TestOptions_Unique(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Unique("user-123-daily-report").Apply(opts)
	assert.Equal(t, "user-123-daily-report", opts.UniqueKey)
}

func TestOptions_Determinism(t *testing.T) {
	opts := jobs.NewOptions()

	jobs.Determinism(jobs.ExplicitCheckpoints).Apply(opts)
	assert.Equal(t, jobs.ExplicitCheckpoints, opts.Determinism)

	jobs.Determinism(jobs.Strict).Apply(opts)
	assert.Equal(t, jobs.Strict, opts.Determinism)

	jobs.Determinism(jobs.BestEffort).Apply(opts)
	assert.Equal(t, jobs.BestEffort, opts.Determinism)
}

func TestOptions_NewOptions_Defaults(t *testing.T) {
	opts := jobs.NewOptions()

	assert.Equal(t, "default", opts.Queue)
	assert.Equal(t, 0, opts.Priority)
	assert.Equal(t, jobs.DefaultJobRetries, opts.MaxRetries)
	assert.Equal(t, time.Duration(0), opts.Delay)
	assert.Nil(t, opts.RunAt)
	assert.Empty(t, opts.UniqueKey)
}

func TestOptions_MultipleApply(t *testing.T) {
	opts := jobs.NewOptions()

	jobs.QueueOpt("critical").Apply(opts)
	jobs.Priority(100).Apply(opts)
	jobs.Retries(10).Apply(opts)
	jobs.Delay(5 * time.Minute).Apply(opts)
	jobs.Unique("task-123").Apply(opts)

	assert.Equal(t, "critical", opts.Queue)
	assert.Equal(t, 100, opts.Priority)
	assert.Equal(t, 10, opts.MaxRetries)
	assert.Equal(t, 5*time.Minute, opts.Delay)
	assert.Equal(t, "task-123", opts.UniqueKey)
}

func TestOptions_Override(t *testing.T) {
	opts := jobs.NewOptions()

	// Apply first set
	jobs.QueueOpt("first").Apply(opts)
	jobs.Priority(50).Apply(opts)

	// Override with new values
	jobs.QueueOpt("second").Apply(opts)
	jobs.Priority(100).Apply(opts)

	assert.Equal(t, "second", opts.Queue)
	assert.Equal(t, 100, opts.Priority)
}

func TestDefaultValues(t *testing.T) {
	assert.Equal(t, 2, jobs.DefaultJobRetries)
	assert.Equal(t, 3, jobs.DefaultCallRetries)
}

func TestDeterminismMode_Values(t *testing.T) {
	// Verify the enum values
	assert.Equal(t, jobs.DeterminismMode(0), jobs.ExplicitCheckpoints)
	assert.Equal(t, jobs.DeterminismMode(1), jobs.Strict)
	assert.Equal(t, jobs.DeterminismMode(2), jobs.BestEffort)
}
