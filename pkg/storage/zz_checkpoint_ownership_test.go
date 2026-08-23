package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A checkpoint is part of an attempt's durable verdict. Once the lease moves,
// the stale attempt must not overwrite the new owner's result through the
// (job_id, call_index, call_type) upsert.
func TestSaveCheckpointOwnedRejectsAStaleWorkerWithoutOverwriting(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const owner = "worker-owner"
	const stale = "worker-stale"
	jobID := seedRunningJobOwnedBy(t, ctx, s, owner)

	ownerCP := &core.Checkpoint{
		JobID: jobID, CallIndex: 0, CallType: "charge",
		Result: []byte(`"payment-owner"`), SpanEnd: 1,
	}
	require.NoError(t, s.SaveCheckpointOwned(ctx, ownerCP, owner))

	staleCP := &core.Checkpoint{
		JobID: jobID, CallIndex: 0, CallType: "charge",
		Error: "duplicate charge", ErrorKind: core.CheckpointErrorKindNoRetry,
		SpanEnd: 1,
	}
	err := s.SaveCheckpointOwned(ctx, staleCP, stale)
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	checkpoints, err := s.GetCheckpoints(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	assert.JSONEq(t, `"payment-owner"`, string(checkpoints[0].Result))
	assert.Empty(t, checkpoints[0].Error,
		"a stale run's terminal error must not replace the owner's successful result")
}

// Re-saving is ordinary replay behaviour. The ownership fence must reject only
// stale workers, not turn the existing upsert into insert-only storage.
func TestSaveCheckpointOwnedLetsTheCurrentOwnerResaveInPlace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const owner = "worker-owner"
	jobID := seedRunningJobOwnedBy(t, ctx, s, owner)

	first := &core.Checkpoint{
		JobID: jobID, CallIndex: 0, CallType: "mint",
		Result: []byte(`"token-1"`), SpanEnd: 1,
	}
	second := &core.Checkpoint{
		JobID: jobID, CallIndex: 0, CallType: "mint",
		Result: []byte(`"token-2"`), SpanEnd: 2,
	}
	require.NoError(t, s.SaveCheckpointOwned(ctx, first, owner))
	require.NoError(t, s.SaveCheckpointOwned(ctx, second, owner))

	checkpoints, err := s.GetCheckpoints(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	assert.JSONEq(t, `"token-2"`, string(checkpoints[0].Result))
	assert.Equal(t, 2, checkpoints[0].SpanEnd)
}

func TestSaveCheckpointTxOwnedRejectsAStaleWorkerInsideCallerTransaction(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const owner = "worker-owner"
	jobID := seedRunningJobOwnedBy(t, ctx, s, owner)
	cp := &core.Checkpoint{JobID: jobID, CallIndex: -1, CallType: "phase", Result: []byte(`"done"`)}

	tx := s.DB().Begin()
	require.NoError(t, tx.Error)
	err := s.SaveCheckpointTxOwned(ctx, tx, cp, "worker-stale")
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	require.NoError(t, tx.Rollback().Error)

	checkpoints, err := s.GetCheckpoints(ctx, jobID)
	require.NoError(t, err)
	assert.Empty(t, checkpoints)
}
