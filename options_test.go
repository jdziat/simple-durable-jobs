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

func TestOptions_Retries(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Retries(5).Apply(opts)
	assert.Equal(t, 5, opts.MaxRetries)
}

func TestOptions_Delay(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Delay(time.Hour).Apply(opts)
	assert.Equal(t, time.Hour, opts.Delay)
}
