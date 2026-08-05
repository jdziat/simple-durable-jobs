package jobctx

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
)

// callCheckpoint builds an indexed Call checkpoint of the kind an earlier
// execution leaves behind.
func callCheckpoint(jobID string, index int, callType string) core.Checkpoint {
	return core.Checkpoint{
		JobID:     core.UUID(jobID),
		CallIndex: index,
		CallType:  callType,
		Result:    []byte(`null`),
	}
}

// TestGetVersion_InFlightRunKeepsDefaultVersion is the unit-level guard for the
// documented Branch Pattern: a run whose checkpoints predate the marker must be
// pinned to DefaultVersion, not handed maxSupported.
//
// Handing it maxSupported is what made the `case jobs.DefaultVersion:` arm of
// the documented pattern unreachable and dead-lettered the run on a determinism
// violation at the first branched Call.
func TestGetVersion_InFlightRunKeepsDefaultVersion(t *testing.T) {
	t.Run("checkpoint at the cursor pins the run to DefaultVersion and records it", func(t *testing.T) {
		var saved *core.Checkpoint
		// An earlier run recorded Call "quote-shipping" at index 0 and no marker.
		ctx := newTestVersionContext("job-1", []core.Checkpoint{
			callCheckpoint("job-1", 0, "quote-shipping"),
		}, func(_ context.Context, cp *core.Checkpoint) error {
			saved = cp
			return nil
		})

		version, err := GetVersion(ctx, "shipping-v2", DefaultVersion, 1)
		require.NoError(t, err)
		assert.Equal(t, DefaultVersion, version, "an in-flight run must keep its originally recorded path")

		require.NotNil(t, saved, "the pinned version must be recorded like any other marker")
		var recorded int
		require.NoError(t, json.Unmarshal(saved.Result, &recorded))
		assert.Equal(t, DefaultVersion, recorded)
		assert.Equal(t, -1, saved.CallIndex)
	})

	t.Run("the recorded pin is what a later replay reads back", func(t *testing.T) {
		var saved *core.Checkpoint
		ctx := newTestVersionContext("job-1", []core.Checkpoint{
			callCheckpoint("job-1", 0, "quote-shipping"),
		}, func(_ context.Context, cp *core.Checkpoint) error {
			saved = cp
			return nil
		})
		_, err := GetVersion(ctx, "shipping-v2", DefaultVersion, 1)
		require.NoError(t, err)
		require.NotNil(t, saved)

		// Next attempt: the marker is now present, so the ordinary replay path
		// serves it — even after a later deploy raises maxSupported.
		replayCtx := newTestVersionContext("job-1", []core.Checkpoint{
			callCheckpoint("job-1", 0, "quote-shipping"),
			*saved,
		}, nil)
		replayed, err := GetVersion(replayCtx, "shipping-v2", DefaultVersion, 2)
		require.NoError(t, err)
		assert.Equal(t, DefaultVersion, replayed)
	})

	t.Run("a checkpoint BEHIND the cursor is this run's own replayed work, not evidence", func(t *testing.T) {
		// The handler already replayed Call "a" at index 0 before reaching
		// GetVersion, and the earlier run recorded nothing past it. Nothing can
		// collide, so this run takes the new branch.
		ctx := newTestVersionContext("job-1", []core.Checkpoint{
			callCheckpoint("job-1", 0, "a"),
		}, func(_ context.Context, _ *core.Checkpoint) error { return nil })
		cs := intctx.GetCallState(ctx)
		require.NotNil(t, cs)
		cs.CallIndex = 1 // Call "a" consumed index 0

		version, err := GetVersion(ctx, "later-change", DefaultVersion, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, version, "no unreplayed durable step remains, so the new branch is safe")
	})

	t.Run("a first execution with no checkpoints takes the new branch", func(t *testing.T) {
		ctx := newTestVersionContext("job-1", nil, func(_ context.Context, _ *core.Checkpoint) error { return nil })

		version, err := GetVersion(ctx, "shipping-v2", DefaultVersion, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, version)
	})

	t.Run("phase checkpoints are not replay evidence", func(t *testing.T) {
		// Phase checkpoints live at CallIndex -1. They say nothing about which
		// indexed durable steps a previous run reached, so they must not pin.
		ctx := newTestVersionContext("job-1", []core.Checkpoint{{
			JobID:     "job-1",
			CallIndex: -1,
			CallType:  "charged-card",
			Result:    []byte(`null`),
		}}, func(_ context.Context, _ *core.Checkpoint) error { return nil })

		version, err := GetVersion(ctx, "shipping-v2", DefaultVersion, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, version)
	})

	t.Run("second deploy: raising minSupported errors instead of taking a wrong branch", func(t *testing.T) {
		saves := 0
		ctx := newTestVersionContext("job-1", []core.Checkpoint{
			callCheckpoint("job-1", 0, "quote-shipping"),
		}, func(_ context.Context, _ *core.Checkpoint) error {
			saves++
			return nil
		})

		version, err := GetVersion(ctx, "shipping-v2", 1, 1)
		assert.Equal(t, 0, version)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrUnsupportedWorkflowVersion))
		assert.Zero(t, saves, "an out-of-range pin must not be recorded")
	})

	t.Run("evidence past a replay-jumped cursor still pins", func(t *testing.T) {
		// A span jump moved the cursor to 3; an earlier run recorded another
		// durable step at index 3 that this run has not reached.
		ctx := newTestVersionContext("job-1", []core.Checkpoint{
			callCheckpoint("job-1", 0, "outer"),
			callCheckpoint("job-1", 3, "quote-shipping"),
		}, func(_ context.Context, _ *core.Checkpoint) error { return nil })
		cs := intctx.GetCallState(ctx)
		require.NotNil(t, cs)
		cs.CallIndex = 3

		version, err := GetVersion(ctx, "shipping-v2", DefaultVersion, 1)
		require.NoError(t, err)
		assert.Equal(t, DefaultVersion, version)
	})
}

// TestHasUnreachedCallCheckpoints_Boundary pins the exact comparison the replay
// signal rests on: index == cursor is evidence (the earlier run occupied the
// slot this handler is about to use), index == cursor-1 is not.
func TestHasUnreachedCallCheckpoints_Boundary(t *testing.T) {
	build := func(cursor int, indices ...int) *intctx.CallState {
		cps := make([]core.Checkpoint, 0, len(indices))
		for _, i := range indices {
			cps = append(cps, callCheckpoint("job-1", i, "x"))
		}
		ctx := intctx.WithCallState(context.Background(), cps)
		cs := intctx.GetCallState(ctx)
		cs.CallIndex = cursor
		return cs
	}

	assert.False(t, build(0).HasUnreachedCallCheckpoints(), "no checkpoints at all")
	assert.True(t, build(0, 0).HasUnreachedCallCheckpoints(), "index == cursor is unreached")
	assert.False(t, build(1, 0).HasUnreachedCallCheckpoints(), "index == cursor-1 was consumed")
	assert.True(t, build(1, 0, 1).HasUnreachedCallCheckpoints(), "index == cursor is unreached")
	assert.False(t, build(4, 0, 1, 2, 3).HasUnreachedCallCheckpoints(), "all consumed")
}
