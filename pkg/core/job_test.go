package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJobStatus_Values(t *testing.T) {
	assert.Equal(t, JobStatus("pending"), StatusPending)
	assert.Equal(t, JobStatus("running"), StatusRunning)
	assert.Equal(t, JobStatus("completed"), StatusCompleted)
	assert.Equal(t, JobStatus("failed"), StatusFailed)
	assert.Equal(t, JobStatus("retrying"), StatusRetrying)
}

func TestJob_Defaults(t *testing.T) {
	job := &Job{}
	assert.Empty(t, job.ID)
	assert.Empty(t, job.Type)
	assert.Empty(t, job.Queue)
	assert.Equal(t, 0, job.Priority)
	assert.Equal(t, JobStatus(""), job.Status)
	assert.Equal(t, 0, job.Attempt)
	assert.Equal(t, 0, job.MaxRetries)
}

func TestJob_WithValues(t *testing.T) {
	now := time.Now()
	job := &Job{
		ID:         "test-123",
		Type:       "send-email",
		Args:       []byte(`{"to":"user@example.com"}`),
		Queue:      "emails",
		Priority:   10,
		Status:     StatusPending,
		Attempt:    0,
		MaxRetries: 3,
		RunAt:      &now,
	}

	assert.Equal(t, "test-123", job.ID)
	assert.Equal(t, "send-email", job.Type)
	assert.Equal(t, "emails", job.Queue)
	assert.Equal(t, 10, job.Priority)
	assert.Equal(t, StatusPending, job.Status)
	assert.NotNil(t, job.RunAt)
}

func TestCheckpoint_Fields(t *testing.T) {
	cp := &Checkpoint{
		ID:        "cp-123",
		JobID:     "job-456",
		CallIndex: 0,
		CallType:  "fetch-data",
		Result:    []byte(`{"count":42}`),
		Error:     "",
	}

	assert.Equal(t, "cp-123", cp.ID)
	assert.Equal(t, "job-456", cp.JobID)
	assert.Equal(t, 0, cp.CallIndex)
	assert.Equal(t, "fetch-data", cp.CallType)
	assert.NotEmpty(t, cp.Result)
	assert.Empty(t, cp.Error)
}

func TestCheckpoint_WithError(t *testing.T) {
	cp := &Checkpoint{
		ID:        "cp-err",
		JobID:     "job-789",
		CallIndex: 1,
		CallType:  "api-call",
		Error:     "connection refused",
	}

	assert.Empty(t, cp.Result)
	assert.Equal(t, "connection refused", cp.Error)
}
