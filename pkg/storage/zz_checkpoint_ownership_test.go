package storage

import (
	"context"
	"testing"
	"time"

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

func TestSaveCheckpointOwnedRejectsStaleDispatchFromSameWorker(t *testing.T) {
	base := context.Background()
	s := newTestStorage(t)
	const worker = "worker-a"
	jobID := seedRunningJobOwnedBy(t, base, s, worker)

	// This models a lease reclaim followed by redispatch to the same configured
	// worker. Worker identity is unchanged; the durable dispatch incarnation is
	// what must keep the old handler from overwriting the new handler's result.
	require.NoError(t, s.DB().Model(&core.Job{}).Where("id = ?", jobID).Update("dispatch_token", "run-2").Error)
	current := core.WithDispatchToken(base, "run-2")
	stale := core.WithDispatchToken(base, "run-1")
	require.NoError(t, s.SaveCheckpointOwned(current, &core.Checkpoint{JobID: jobID, CallIndex: 0, CallType: "call", Result: []byte(`"new"`)}, worker))
	require.ErrorIs(t, s.SaveCheckpointOwned(stale, &core.Checkpoint{JobID: jobID, CallIndex: 0, CallType: "call", Result: []byte(`"stale"`)}, worker), core.ErrJobNotOwned)

	checkpoints, err := s.GetCheckpoints(base, jobID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	assert.JSONEq(t, `"new"`, string(checkpoints[0].Result))
}

func TestLifecycleWritesRejectStaleSameWorkerDispatch(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	stale := core.WithDispatchToken(ctx, "stale")
	for name, write := range map[string]func(core.UUID) error{
		"complete":  func(id core.UUID) error { return s.Complete(stale, id, "worker-a") },
		"fail":      func(id core.UUID) error { return s.Fail(stale, id, "worker-a", "failed", nil) },
		"heartbeat": func(id core.UUID) error { return s.Heartbeat(stale, id, "worker-a") },
		"release":   func(id core.UUID) error { return s.Release(stale, id, "worker-a") },
		"waiting":   func(id core.UUID) error { return s.MarkWaitingWithDeadline(stale, id, "worker-a", time.Minute) },
	} {
		t.Run(name, func(t *testing.T) {
			id := seedRunningJobOwnedBy(t, ctx, s, "worker-a")
			require.NoError(t, s.DB().Model(&core.Job{}).Where("id = ?", id).Update("dispatch_token", "current").Error)
			require.ErrorIs(t, write(id), core.ErrJobNotOwned)
			got, err := s.GetJob(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, core.StatusRunning, got.Status)
		})
	}
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
