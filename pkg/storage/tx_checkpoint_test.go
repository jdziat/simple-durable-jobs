package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
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
