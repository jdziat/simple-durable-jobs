package core

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// eventMarker method coverage — each struct must satisfy the Event interface.
// Calling eventMarker() directly exercises the generated stub and registers
// the line in the coverage profile.
// ---------------------------------------------------------------------------

func TestJobStarted_EventMarker(t *testing.T) {
	e := &JobStarted{
		Job:       &Job{ID: "js"},
		Timestamp: time.Now(),
	}
	e.eventMarker() // exercise the method body

	// Verify interface satisfaction at compile time via assignment.
	var _ Event = e
	assert.NotNil(t, e)
}

func TestJobCompleted_EventMarker(t *testing.T) {
	e := &JobCompleted{
		Job:       &Job{ID: "jc"},
		Duration:  time.Second,
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.NotNil(t, e)
}

func TestJobFailed_EventMarker(t *testing.T) {
	e := &JobFailed{
		Job:       &Job{ID: "jf"},
		Error:     errors.New("oops"),
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.NotNil(t, e)
}

func TestJobRetrying_EventMarker(t *testing.T) {
	e := &JobRetrying{
		Job:       &Job{ID: "jr"},
		Attempt:   2,
		Error:     errors.New("transient"),
		NextRunAt: time.Now().Add(time.Minute),
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.NotNil(t, e)
}

func TestCheckpointSaved_EventMarker(t *testing.T) {
	e := &CheckpointSaved{
		JobID:     "cp-job",
		CallIndex: 1,
		CallType:  "fetch",
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.NotNil(t, e)
}

func TestJobPaused_EventMarker(t *testing.T) {
	e := &JobPaused{
		Job:       &Job{ID: "jp"},
		Mode:      PauseModeGraceful,
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "jp", e.Job.ID)
	assert.Equal(t, PauseModeGraceful, e.Mode)
}

func TestJobResumed_EventMarker(t *testing.T) {
	e := &JobResumed{
		Job:       &Job{ID: "jres"},
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "jres", e.Job.ID)
}

func TestQueuePaused_EventMarker(t *testing.T) {
	e := &QueuePaused{
		Queue:     "emails",
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "emails", e.Queue)
}

func TestQueueResumed_EventMarker(t *testing.T) {
	e := &QueueResumed{
		Queue:     "emails",
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "emails", e.Queue)
}

func TestWorkerPaused_EventMarker(t *testing.T) {
	e := &WorkerPaused{
		WorkerID:  "w-1",
		Mode:      PauseModeAggressive,
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "w-1", e.WorkerID)
	assert.Equal(t, PauseModeAggressive, e.Mode)
}

func TestWorkerResumed_EventMarker(t *testing.T) {
	e := &WorkerResumed{
		WorkerID:  "w-2",
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "w-2", e.WorkerID)
}

func TestCustomEvent_EventMarker(t *testing.T) {
	e := &CustomEvent{
		JobID:     "job-42",
		Kind:      "progress",
		Data:      map[string]any{"pct": 50},
		Timestamp: time.Now(),
	}
	e.eventMarker()

	var _ Event = e
	assert.Equal(t, "job-42", e.JobID)
	assert.Equal(t, "progress", e.Kind)
	assert.Equal(t, 50, e.Data["pct"])
}

// ---------------------------------------------------------------------------
// Interface satisfaction — all event types passed as Event values
// ---------------------------------------------------------------------------

func TestAllEvents_ImplementEventInterface(t *testing.T) {
	var events []Event
	events = append(events,
		&JobStarted{},
		&JobCompleted{},
		&JobFailed{},
		&JobRetrying{},
		&CheckpointSaved{},
		&JobPaused{},
		&JobResumed{},
		&QueuePaused{},
		&QueueResumed{},
		&WorkerPaused{},
		&WorkerResumed{},
		&CustomEvent{},
	)

	assert.Len(t, events, 12, "all event types should be in the list")
	for _, e := range events {
		assert.NotNil(t, e)
	}
}

// ---------------------------------------------------------------------------
// Existing interface conformance tests (retained for completeness)
// ---------------------------------------------------------------------------

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

func TestJobPausedEvent(t *testing.T) {
	job := &Job{ID: "test-123"}
	e := &JobPaused{
		Job:       job,
		Mode:      PauseModeGraceful,
		Timestamp: time.Now(),
	}

	// Verify it implements Event interface
	var _ Event = e
	assert.Equal(t, "test-123", e.Job.ID)
	assert.Equal(t, PauseModeGraceful, e.Mode)
}

func TestJobResumedEvent(t *testing.T) {
	job := &Job{ID: "test-123"}
	e := &JobResumed{
		Job:       job,
		Timestamp: time.Now(),
	}

	var _ Event = e
	assert.Equal(t, "test-123", e.Job.ID)
}

func TestQueuePausedEvent(t *testing.T) {
	e := &QueuePaused{
		Queue:     "emails",
		Timestamp: time.Now(),
	}

	var _ Event = e
	assert.Equal(t, "emails", e.Queue)
}

func TestQueueResumedEvent(t *testing.T) {
	e := &QueueResumed{
		Queue:     "emails",
		Timestamp: time.Now(),
	}

	var _ Event = e
	assert.Equal(t, "emails", e.Queue)
}
