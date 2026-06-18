package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func TestBatchComplete_PerRowResults_OwnershipAndGC(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Three jobs owned by "w", one owned by "other" (must be excluded).
	a := runningJob(t, ctx, s, "w")
	b := runningJob(t, ctx, s, "w")
	c := runningJob(t, ctx, s, "w")
	other := runningJob(t, ctx, s, "other")

	// Give one of w's jobs a checkpoint to prove in-tx GC.
	require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{JobID: a.ID, CallIndex: 0, CallType: "x", Result: []byte(`"r"`)}))

	items := []BatchCompleteItem{
		{JobID: a.ID, Result: []byte(`{"job":"a"}`)},
		{JobID: b.ID, Result: []byte(`{"job":"b"}`)},
		{JobID: c.ID, Result: []byte(`{"job":"c"}`)},
		{JobID: other.ID, Result: []byte(`{"job":"other"}`)}, // not owned by w
	}
	committed, err := s.BatchComplete(ctx, "w", items, true)
	require.NoError(t, err)

	// Only w's three committed; the other-worker row excluded.
	require.ElementsMatch(t, []core.UUID{a.ID, b.ID, c.ID}, committed)

	// Each committed job is completed with its DISTINCT result.
	for id, want := range map[core.UUID]string{a.ID: `{"job":"a"}`, b.ID: `{"job":"b"}`, c.ID: `{"job":"c"}`} {
		got, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, core.StatusCompleted, got.Status, "job %s", id)
		assert.Equal(t, want, string(got.Result), "job %s result", id)
		assert.Empty(t, got.LockedBy, "job %s lock cleared", id)
		assert.NotNil(t, got.CompletedAt, "job %s completed_at", id)
	}

	// The not-owned job is untouched (still running, no result).
	og, err := s.GetJob(ctx, other.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusRunning, og.Status)
	assert.Empty(t, og.Result)

	// Checkpoint GC happened in-tx for the committed job.
	cps, err := s.GetCheckpoints(ctx, a.ID)
	require.NoError(t, err)
	assert.Empty(t, cps, "checkpoints GC'd in the batch tx")
}

func TestBatchComplete_Empty(t *testing.T) {
	s := newTestStorage(t)
	got, err := s.BatchComplete(context.Background(), "w", nil, false)
	require.NoError(t, err)
	assert.Empty(t, got)
}
