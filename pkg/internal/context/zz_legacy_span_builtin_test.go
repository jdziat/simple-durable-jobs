package context

import (
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

func cpFor(idx int, callType string, spanEnd int) *core.Checkpoint {
	return &core.Checkpoint{CallIndex: idx, CallType: callType, SpanEnd: spanEnd}
}

func stateWith(cps ...*core.Checkpoint) *CallState {
	cs := &CallState{Checkpoints: make(map[CheckpointKey]*core.Checkpoint, len(cps))}
	for _, cp := range cps {
		cs.Checkpoints[CheckpointKey{Index: cp.CallIndex, Type: cp.CallType}] = cp
	}
	return cs
}

// HasLegacyCallSpans drives the per-run WARN that names a job as carrying
// pre-upgrade checkpoints. Built-in durable operations carry SpanEnd == 0 in
// every version including the current one, so counting them warned operators
// about healthy work — and UPGRADE.md directs them to Requeue what the detector
// lists, which clears checkpoints and re-executes completed durable work.
func TestHasLegacyCallSpans_IgnoresBuiltinDurableOperations(t *testing.T) {
	t.Run("built-in operations alone are never legacy", func(t *testing.T) {
		cs := stateWith(
			cpFor(0, core.CheckpointTypeFanOut, 0),
			cpFor(1, core.CheckpointTypeSignalPrefix+"approval", 0),
			cpFor(2, core.CheckpointTypeSignalTimeoutPrefix+"approval", 0),
		)
		require.False(t, cs.HasLegacyCallSpans(),
			"a healthy current-version workflow of built-in operations must not be warned about")
	})

	t.Run("one legacy Call padded by built-ins is not legacy", func(t *testing.T) {
		cs := stateWith(
			cpFor(0, core.CheckpointTypeFanOut, 0),
			cpFor(1, core.CheckpointTypeSignalPrefix+"go", 0),
			cpFor(2, "child", 0),
		)
		require.False(t, cs.HasLegacyCallSpans(),
			"the defect needs a later call to read an earlier call's slot, so a single legacy Call cannot be affected; built-ins must not pad the count past the threshold")
	})

	t.Run("two legacy Calls are still detected alongside built-ins", func(t *testing.T) {
		cs := stateWith(
			cpFor(0, core.CheckpointTypeFanOut, 0),
			cpFor(1, "child", 0),
			cpFor(2, "leaf", 0),
		)
		require.True(t, cs.HasLegacyCallSpans(),
			"narrowing the predicate must not buy a false negative on genuinely at-risk work")
	})

	t.Run("current-version Calls carrying a span are not legacy", func(t *testing.T) {
		cs := stateWith(cpFor(0, "child", 2), cpFor(1, "leaf", 2))
		require.False(t, cs.HasLegacyCallSpans())
	})

	t.Run("phase checkpoints are excluded", func(t *testing.T) {
		cs := stateWith(cpFor(-1, "phase-a", 0), cpFor(-1, "phase-b", 0))
		require.False(t, cs.HasLegacyCallSpans())
	})
}
