// Package jobs — white-box tests for unexported pause option internals.
package jobs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPauseModeOpt_ApplyPause exercises the unexported applyPause method
// on pauseModeOpt, which is only reachable from within the package.
func TestPauseModeOpt_ApplyPause_Graceful(t *testing.T) {
	opt := WithPauseMode(PauseModeGraceful)
	opts := &pauseOptions{}
	opt.applyPause(opts)
	assert.Equal(t, PauseModeGraceful, opts.mode)
}

func TestPauseModeOpt_ApplyPause_Aggressive(t *testing.T) {
	opt := WithPauseMode(PauseModeAggressive)
	opts := &pauseOptions{}
	opt.applyPause(opts)
	assert.Equal(t, PauseModeAggressive, opts.mode)
}
