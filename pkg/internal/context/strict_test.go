package context

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// TestUnconsumedCallCheckpoints exercises the detector that powers Strict
// determinism: a recorded Call checkpoint whose index was never reached this
// run is "unconsumed"; phase checkpoints (index -1) are excluded.
func TestUnconsumedCallCheckpoints(t *testing.T) {
	mk := func(checkpoints []core.Checkpoint, callIndex int) *CallState {
		cs := &CallState{Checkpoints: map[CheckpointKey]*core.Checkpoint{}}
		for i := range checkpoints {
			cp := checkpoints[i]
			cs.Checkpoints[CheckpointKey{Index: cp.CallIndex, Type: cp.CallType}] = &cp
		}
		cs.CallIndex = callIndex
		return cs
	}

	t.Run("all consumed", func(t *testing.T) {
		cs := mk([]core.Checkpoint{
			{CallIndex: 0, CallType: "a"},
			{CallIndex: 1, CallType: "b"},
		}, 2)
		assert.Equal(t, 0, cs.UnconsumedCallCheckpoints())
	})

	t.Run("dropped trailing call", func(t *testing.T) {
		// Recorded 3 calls but the replay only made 2.
		cs := mk([]core.Checkpoint{
			{CallIndex: 0, CallType: "a"},
			{CallIndex: 1, CallType: "b"},
			{CallIndex: 2, CallType: "c"},
		}, 2)
		assert.Equal(t, 1, cs.UnconsumedCallCheckpoints())
	})

	t.Run("phase checkpoints are ignored", func(t *testing.T) {
		cs := mk([]core.Checkpoint{
			{CallIndex: 0, CallType: "a"},
			{CallIndex: -1, CallType: "phase:extract"},
			{CallIndex: -1, CallType: "phase:load"},
		}, 1)
		assert.Equal(t, 0, cs.UnconsumedCallCheckpoints(), "phase checkpoints (index -1) must not count")
	})

	t.Run("extra calls beyond checkpoints are fine", func(t *testing.T) {
		// Replay made more calls than were recorded — forward progress, not a
		// violation.
		cs := mk([]core.Checkpoint{
			{CallIndex: 0, CallType: "a"},
		}, 3)
		assert.Equal(t, 0, cs.UnconsumedCallCheckpoints())
	})
}
