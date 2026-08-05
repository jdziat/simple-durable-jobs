package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func TestGormStorage_SaveCheckpointTx_CommitAndRollbackVisibility(t *testing.T) {
	ctx := context.Background()
	s := newTxEnqueueTestStorage(t)

	t.Run("commit", func(t *testing.T) {
		seedTestJob(t, ctx, s, testUUID("job-tx-commit"), core.StatusRunning)
		seedTestJob(t, ctx, s, testUUID("job-plain-commit"), core.StatusRunning)
		resultBytes, err := json.Marshal(map[string]string{"status": "done"})
		require.NoError(t, err)
		cp := &core.Checkpoint{
			JobID:     testUUID("job-tx-commit"),
			CallIndex: -1,
			CallType:  "phase.commit",
			Result:    resultBytes,
		}

		tx := s.DB().Begin()
		require.NoError(t, tx.Error)
		require.NoError(t, s.SaveCheckpointTx(ctx, tx, cp))

		before, err := s.GetCheckpoints(ctx, cp.JobID)
		require.NoError(t, err)
		assert.Empty(t, before)

		require.NoError(t, tx.Commit().Error)

		after, err := s.GetCheckpoints(ctx, cp.JobID)
		require.NoError(t, err)
		require.Len(t, after, 1)
		assert.NotEmpty(t, after[0].ID)
		assert.Equal(t, cp.JobID, after[0].JobID)
		assert.Equal(t, -1, after[0].CallIndex)
		assert.Equal(t, "phase.commit", after[0].CallType)
		assert.JSONEq(t, string(resultBytes), string(after[0].Result))

		plain := &core.Checkpoint{
			JobID:     testUUID("job-plain-commit"),
			CallIndex: -1,
			CallType:  "phase.commit",
			Result:    resultBytes,
		}
		require.NoError(t, s.SaveCheckpoint(ctx, plain))
		plainRows, err := s.GetCheckpoints(ctx, plain.JobID)
		require.NoError(t, err)
		require.Len(t, plainRows, 1)
		assert.Equal(t, after[0].CallIndex, plainRows[0].CallIndex)
		assert.Equal(t, after[0].CallType, plainRows[0].CallType)
		assert.Equal(t, after[0].Result, plainRows[0].Result)
	})

	t.Run("rollback", func(t *testing.T) {
		seedTestJob(t, ctx, s, testUUID("job-tx-rollback"), core.StatusRunning)
		cp := &core.Checkpoint{
			JobID:     testUUID("job-tx-rollback"),
			CallIndex: -1,
			CallType:  "phase.rollback",
			Result:    []byte(`"rolled-back"`),
		}

		tx := s.DB().Begin()
		require.NoError(t, tx.Error)
		require.NoError(t, s.SaveCheckpointTx(ctx, tx, cp))
		require.NoError(t, tx.Rollback().Error)

		after, err := s.GetCheckpoints(ctx, cp.JobID)
		require.NoError(t, err)
		assert.Empty(t, after)
	})
}

// TestGormStorage_SaveCheckpointTx_UpsertsInPlace pins the OnConflict clause on
// the TRANSACTIONAL save. Re-saving the same phase or signal checkpoint is the
// ordinary case, not an edge one — a retried job re-runs the phase, and
// SavePhaseCheckpointTx / the signal paths mint a FRESH primary key each time —
// so the second write must resolve on (job_id, call_index, call_type) and update
// the existing row. Without the clause it is a unique-index violation; with a
// DoNothing/short DoUpdates it silently serves the stale first result back to
// replay.
func TestGormStorage_SaveCheckpointTx_UpsertsInPlace(t *testing.T) {
	ctx := context.Background()
	s := newTxEnqueueTestStorage(t)
	jobID := testUUID("job-tx-upsert")
	seedTestJob(t, ctx, s, jobID, core.StatusRunning)

	// A sibling on a different call index. The conflict target must not be so
	// broad that re-saving one checkpoint rewrites the job's others.
	sibling := &core.Checkpoint{
		JobID:     jobID,
		CallIndex: 0,
		CallType:  "call",
		Result:    []byte(`"sibling"`),
		SpanEnd:   1,
	}
	require.NoError(t, s.SaveCheckpoint(ctx, sibling))

	// Every conflict column starts non-zero and changes on the re-save, so no
	// assertion below can pass by comparing two zero values.
	first := &core.Checkpoint{
		JobID:           jobID,
		CallIndex:       -1,
		CallType:        "phase.upsert",
		Result:          []byte(`"first"`),
		Error:           "first error",
		ErrorKind:       "retry_after",
		ErrorCause:      "first cause",
		ErrorDelayNanos: 500,
		SpanEnd:         3,
	}
	tx := s.DB().Begin()
	require.NoError(t, tx.Error)
	require.NoError(t, s.SaveCheckpointTx(ctx, tx, first))
	require.NoError(t, tx.Commit().Error)

	second := &core.Checkpoint{
		JobID:           jobID,
		CallIndex:       -1,
		CallType:        "phase.upsert",
		Result:          []byte(`"second"`),
		Error:           "second error",
		ErrorKind:       "no_retry",
		ErrorCause:      "second cause",
		ErrorDelayNanos: 1500,
		SpanEnd:         7,
	}
	tx2 := s.DB().Begin()
	require.NoError(t, tx2.Error)
	require.NoError(t, s.SaveCheckpointTx(ctx, tx2, second),
		"re-save through the tx path must upsert, not violate idx_checkpoints_job_call")
	require.NoError(t, tx2.Commit().Error)
	require.NotEqual(t, first.ID, second.ID, "the re-save must carry a fresh primary key")

	rows, err := s.GetCheckpoints(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, rows, 2, "the re-save must update the phase row, not insert a duplicate")

	byType := map[string]core.Checkpoint{}
	for _, row := range rows {
		byType[row.CallType] = row
	}

	updated, ok := byType["phase.upsert"]
	require.True(t, ok)
	assert.Equal(t, first.ID, updated.ID, "the original row must be updated in place")
	assert.Equal(t, `"second"`, string(updated.Result))
	assert.Equal(t, "second error", updated.Error)
	assert.Equal(t, "no_retry", updated.ErrorKind)
	assert.Equal(t, "second cause", updated.ErrorCause)
	assert.EqualValues(t, 1500, updated.ErrorDelayNanos)
	assert.Equal(t, 7, updated.SpanEnd)

	untouched, ok := byType["call"]
	require.True(t, ok)
	assert.Equal(t, sibling.ID, untouched.ID)
	assert.Equal(t, `"sibling"`, string(untouched.Result))
	assert.Equal(t, 1, untouched.SpanEnd)
}
