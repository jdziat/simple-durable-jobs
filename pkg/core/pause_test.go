package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPauseModeConstants(t *testing.T) {
	assert.Equal(t, PauseMode("graceful"), PauseModeGraceful)
	assert.Equal(t, PauseMode("aggressive"), PauseModeAggressive)
}

func TestStatusPaused(t *testing.T) {
	assert.Equal(t, JobStatus("paused"), StatusPaused)
}
