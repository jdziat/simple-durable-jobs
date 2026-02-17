package jobs_test

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestJob_HasRequiredFields(t *testing.T) {
	job := jobs.Job{
		ID:        "test-123",
		Type:      "send-email",
		Args:      []byte(`{"to":"user@example.com"}`),
		Queue:     "default",
		Priority:  0,
		Status:    jobs.StatusPending,
		Attempt:   0,
		MaxRetries: 3,
		CreatedAt: time.Now(),
	}

	assert.Equal(t, "test-123", job.ID)
	assert.Equal(t, "send-email", job.Type)
	assert.Equal(t, jobs.StatusPending, job.Status)
}

func TestCheckpoint_HasRequiredFields(t *testing.T) {
	cp := jobs.Checkpoint{
		ID:        "cp-123",
		JobID:     "job-456",
		CallIndex: 0,
		CallType:  "charge-card",
		Result:    []byte(`{"receipt":"abc"}`),
	}

	assert.Equal(t, "cp-123", cp.ID)
	assert.Equal(t, "job-456", cp.JobID)
	assert.Equal(t, 0, cp.CallIndex)
}
