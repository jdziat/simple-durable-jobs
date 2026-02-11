package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueState_Fields(t *testing.T) {
	now := time.Now()
	qs := QueueState{
		Queue:    "emails",
		Paused:   true,
		PausedAt: &now,
		PausedBy: "admin",
	}

	assert.Equal(t, "emails", qs.Queue)
	assert.True(t, qs.Paused)
	assert.NotNil(t, qs.PausedAt)
	assert.Equal(t, "admin", qs.PausedBy)
}
