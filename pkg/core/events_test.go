package core

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJobStarted_ImplementsEvent(t *testing.T) {
	var e Event = &JobStarted{
		Job:       &Job{ID: "test"},
		Timestamp: time.Now(),
	}
	assert.NotNil(t, e)
}

func TestJobCompleted_ImplementsEvent(t *testing.T) {
	var e Event = &JobCompleted{
		Job:       &Job{ID: "test"},
		Duration:  time.Second,
		Timestamp: time.Now(),
	}
	assert.NotNil(t, e)
}

func TestJobFailed_ImplementsEvent(t *testing.T) {
	var e Event = &JobFailed{
		Job:       &Job{ID: "test"},
		Error:     errors.New("failed"),
		Timestamp: time.Now(),
	}
	assert.NotNil(t, e)
}

func TestJobRetrying_ImplementsEvent(t *testing.T) {
	var e Event = &JobRetrying{
		Job:       &Job{ID: "test"},
		Attempt:   1,
		Error:     errors.New("temp error"),
		NextRunAt: time.Now().Add(time.Minute),
		Timestamp: time.Now(),
	}
	assert.NotNil(t, e)
}

func TestCheckpointSaved_ImplementsEvent(t *testing.T) {
	var e Event = &CheckpointSaved{
		JobID:     "job-123",
		CallIndex: 0,
		CallType:  "fetch",
		Timestamp: time.Now(),
	}
	assert.NotNil(t, e)
}
