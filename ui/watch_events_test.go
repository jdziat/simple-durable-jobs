package ui

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoreEventToProto_AllTypes(t *testing.T) {
	job := &core.Job{ID: "j1", Queue: "default", Type: "test"}
	now := time.Now()

	tests := []struct {
		event    core.Event
		wantType string
	}{
		{&core.JobStarted{Job: job, Timestamp: now}, "job.started"},
		{&core.JobCompleted{Job: job, Timestamp: now}, "job.completed"},
		{&core.JobFailed{Job: job, Timestamp: now}, "job.failed"},
		{&core.JobRetrying{Job: job, Timestamp: now}, "job.retrying"},
		{&core.JobPaused{Job: job, Timestamp: now}, "job.paused"},
		{&core.JobResumed{Job: job, Timestamp: now}, "job.resumed"},
		{&core.QueuePaused{Queue: "default", Timestamp: now}, "queue.paused"},
		{&core.QueueResumed{Queue: "default", Timestamp: now}, "queue.resumed"},
		{&core.WorkerPaused{WorkerID: "w1", Timestamp: now}, "worker.paused"},
		{&core.WorkerResumed{WorkerID: "w1", Timestamp: now}, "worker.resumed"},
	}

	for _, tt := range tests {
		ev := coreEventToProto(tt.event)
		require.NotNil(t, ev, "event type %T should produce a proto event", tt.event)
		assert.Equal(t, tt.wantType, ev.Type)
	}
}

func TestCoreEventToProto_JobFields(t *testing.T) {
	job := &core.Job{
		ID:       "j1",
		Queue:    "emails",
		Type:     "send-email",
		Status:   core.StatusCompleted,
		Priority: 5,
		Attempt:  2,
	}
	now := time.Now()

	ev := coreEventToProto(&core.JobCompleted{Job: job, Timestamp: now})
	require.NotNil(t, ev)
	assert.Equal(t, "job.completed", ev.Type)
	assert.Equal(t, "j1", ev.Job.Id)
	assert.Equal(t, "emails", ev.Job.Queue)
	assert.Equal(t, "send-email", ev.Job.Type)
	assert.Equal(t, int32(5), ev.Job.Priority)
	assert.Equal(t, int32(2), ev.Job.Attempt)
}

func TestCoreEventToProto_QueueEvents_NoJobField(t *testing.T) {
	now := time.Now()

	ev := coreEventToProto(&core.QueuePaused{Queue: "default", Timestamp: now})
	require.NotNil(t, ev)
	assert.Nil(t, ev.Job)
	assert.Equal(t, "queue.paused", ev.Type)
}

func TestCoreEventToProto_UnknownEvent_ReturnsNil(t *testing.T) {
	ev := coreEventToProto(&core.CheckpointSaved{JobID: "j1", Timestamp: time.Now()})
	assert.Nil(t, ev)
}

func TestCoreEventToProto_CustomEvent_ReturnsNil(t *testing.T) {
	ev := coreEventToProto(&core.CustomEvent{JobID: "j1", Kind: "progress", Timestamp: time.Now()})
	assert.Nil(t, ev)
}

func TestParsePeriod(t *testing.T) {
	before := time.Now()

	tests := []struct {
		period       string
		expectedDiff time.Duration
	}{
		{"1h", 1 * time.Hour},
		{"24h", 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"", 1 * time.Hour},         // default
		{"invalid", 1 * time.Hour},  // default
	}

	for _, tt := range tests {
		since, until := parsePeriod(tt.period)
		diff := until.Sub(since)
		assert.InDelta(t, tt.expectedDiff.Seconds(), diff.Seconds(), 1.0, "period=%q", tt.period)
		assert.True(t, until.After(before) || until.Equal(before), "until should be now or later")
	}
}
