package jobctx

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Covers the SavePhaseCheckpointTx guard branches and the persisting happy path.
// The reserved-prefix branch is already covered in jobctx_test.go; here we pin
// the not-in-handler, no-tx-checkpoint, marshal-failure, and commit paths that
// kept this function at ~31%.

func TestSavePhaseCheckpointTx_NotInJobHandler(t *testing.T) {
	// A bare context (no job context) cannot checkpoint.
	err := SavePhaseCheckpointTx(context.Background(), nil, "phase", "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in a job handler")
}

func TestSavePhaseCheckpointTx_StorageWithoutTxCheckpointSupport(t *testing.T) {
	// JobContext with a nil Storage: the TxCheckpointer assertion fails, so the
	// call reports the backend cannot do transactional checkpoints — before any
	// payload marshaling.
	ctx := intctx.WithJobContext(context.Background(), &intctx.JobContext{Job: &core.Job{ID: "job-1"}})
	err := SavePhaseCheckpointTx(ctx, nil, "phase", "x")
	require.ErrorIs(t, err, core.ErrStorageNoTxCheckpoint)
}

func TestSavePhaseCheckpointTx_MarshalError(t *testing.T) {
	store := newVersionTestStorage(t) // real GormStorage implements TxCheckpointer
	ctx := intctx.WithJobContext(context.Background(), &intctx.JobContext{
		Job:     &core.Job{ID: "job-1"},
		Storage: store,
	})

	tx := store.DB().Begin()
	defer tx.Rollback()

	// A channel cannot be JSON-marshaled — fails after the TxCheckpointer check.
	err := SavePhaseCheckpointTx(ctx, tx, "phase", make(chan int))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal phase result")
}

func TestSavePhaseCheckpointTx_PersistsThroughCommittedTx(t *testing.T) {
	ctx := context.Background()
	store := newVersionTestStorage(t)

	// The checkpoint references a real job row (FK-safe on PG/MySQL).
	job := &core.Job{ID: core.NewID(), Type: "wf.run", Queue: "default"}
	require.NoError(t, store.Enqueue(ctx, job))

	jobCtx := intctx.WithJobContext(ctx, &intctx.JobContext{Job: job, Storage: store})

	tx := store.DB().Begin()
	require.NoError(t, SavePhaseCheckpointTx(jobCtx, tx, "compute-total", map[string]int{"total": 42}))
	require.NoError(t, tx.Commit().Error)

	// The phase checkpoint is recorded under the reserved phase CallIndex (-1).
	cps, err := store.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	var found bool
	for _, cp := range cps {
		if cp.CallType == "compute-total" && cp.CallIndex == -1 {
			found = true
			assert.JSONEq(t, `{"total":42}`, string(cp.Result))
		}
	}
	assert.True(t, found, "the committed phase checkpoint is persisted and queryable")
}
