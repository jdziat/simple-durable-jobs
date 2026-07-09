package jobctx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadPhaseCheckpointErr_SurfacesDecodeFailure (PKT-14 / E5) proves the
// error-returning variant distinguishes a genuine absence from a checkpoint that
// EXISTS but cannot decode into T — which the legacy bool API silently treats as
// absent (re-running the phase).
func TestLoadPhaseCheckpointErr_SurfacesDecodeFailure(t *testing.T) {
	ctx := newTestVersionContext("job-1", nil, nil)
	require.NoError(t, SavePhaseCheckpoint(ctx, "phase1", "a string value"))

	// Legacy API: an undecodable checkpoint reads as absent (would re-run).
	_, ok := LoadPhaseCheckpoint[int](ctx, "phase1")
	require.False(t, ok, "legacy Load treats an undecodable checkpoint as absent")

	// Err variant: the checkpoint exists but does not decode into int → fail loud.
	_, ok, err := LoadPhaseCheckpointErr[int](ctx, "phase1")
	require.False(t, ok)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPhaseCheckpointDecode)

	// A genuine absence is (zero, false, nil) — no error.
	_, ok, err = LoadPhaseCheckpointErr[int](ctx, "never-saved")
	require.False(t, ok)
	require.NoError(t, err)

	// A decodable checkpoint returns (value, true, nil).
	got, ok, err := LoadPhaseCheckpointErr[string](ctx, "phase1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "a string value", got)
}
