package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/call"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/jobctx"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ownedCheckpointMock struct {
	*mockStorage
	ownedCalls atomic.Int32
	workerID   string
}

func (m *ownedCheckpointMock) SaveCheckpointOwned(_ context.Context, _ *core.Checkpoint, workerID string) error {
	m.workerID = workerID
	m.ownedCalls.Add(1)
	return nil
}

func TestExecuteHandlerRoutesCheckpointWritesThroughOwnershipFence(t *testing.T) {
	var legacyCalls atomic.Int32
	store := &ownedCheckpointMock{mockStorage: &mockStorage{
		saveCheckpointFunc: func(context.Context, *core.Checkpoint) error {
			legacyCalls.Add(1)
			return nil
		},
	}}
	q := queue.New(store)
	require.NoError(t, q.RegisterE("owned-checkpoint", func(ctx context.Context, _ struct{}) error {
		return jobctx.SavePhaseCheckpoint(ctx, "effect", "done")
	}))
	h, ok := q.GetHandler("owned-checkpoint")
	require.True(t, ok)

	w := NewWorker(q)
	job := &core.Job{ID: "owned-checkpoint-job", Type: "owned-checkpoint", Queue: "default", Args: []byte(`{}`)}
	_, err := w.executeHandler(context.Background(), job, h)
	require.NoError(t, err)

	assert.Equal(t, int32(1), store.ownedCalls.Load())
	assert.Equal(t, int32(0), legacyCalls.Load(),
		"GormStorage-like backends must never fall through to the unfenced v4 compatibility method")
	assert.Equal(t, w.config.WorkerID, store.workerID)
}

// This drives the production Call -> JobContext.SaveCheckpoint -> GormStorage
// path with the documented double-run interleave. Both executions load an empty
// checkpoint snapshot; the lease then moves, the owner writes first, and the
// stale execution tries to replace that verdict with either another result or a
// terminal error. Neither stale write may land.
func TestCallCheckpointFromAStaleExecutionCannotReplaceTheOwnersVerdict(t *testing.T) {
	for _, staleReturnsError := range []bool{false, true} {
		name := "stale result"
		if staleReturnsError {
			name = "stale terminal error"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db, store := newWorkerRetentionStore(t)
			q := queue.New(store)

			entered := make(chan struct{})
			releaseStale := make(chan struct{})
			var calls atomic.Int32
			require.NoError(t, q.RegisterE("activity", func(context.Context, struct{}) (string, error) {
				if calls.Add(1) == 1 {
					close(entered)
					<-releaseStale
					if staleReturnsError {
						return "", core.NoRetry(errors.New("duplicate side effect"))
					}
					return "stale-value", nil
				}
				return "owner-value", nil
			}))
			require.NoError(t, q.RegisterE("workflow", func(ctx context.Context, _ struct{}) (string, error) {
				return call.Call[string](ctx, "activity", struct{}{})
			}))
			h, ok := q.GetHandler("workflow")
			require.True(t, ok)

			job := &core.Job{Type: "workflow", Queue: "default", Args: []byte(`{}`), MaxRetries: 3}
			require.NoError(t, store.Enqueue(ctx, job))
			claimed, err := store.Dequeue(ctx, []string{"default"}, "stale-worker")
			require.NoError(t, err)
			require.NotNil(t, claimed)

			staleWorker := NewWorker(q)
			staleWorker.config.WorkerID = "stale-worker"
			ownerWorker := NewWorker(q)
			ownerWorker.config.WorkerID = "owner-worker"

			staleDone := make(chan error, 1)
			go func() {
				_, runErr := staleWorker.executeHandler(ctx, claimed, h)
				staleDone <- runErr
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("stale execution did not enter the activity")
			}

			// Drive the same public reclaim/claim path the stale-lock reaper uses.
			require.NoError(t, db.Model(&core.Job{}).Where("id = ?", job.ID).
				Update("locked_until", time.Now().Add(-time.Hour)).Error)
			released, err := store.ReleaseStaleLocks(ctx, 0)
			require.NoError(t, err)
			require.Contains(t, released, job.ID)
			ownerJob, err := store.Dequeue(ctx, []string{"default"}, "owner-worker")
			require.NoError(t, err)
			require.NotNil(t, ownerJob)

			ownerResult, err := ownerWorker.executeHandler(ctx, ownerJob, h)
			require.NoError(t, err)
			assert.JSONEq(t, `"owner-value"`, string(ownerResult))

			close(releaseStale)
			require.ErrorIs(t, <-staleDone, core.ErrJobNotOwned)

			checkpoints, err := store.GetCheckpoints(ctx, job.ID)
			require.NoError(t, err)
			require.Len(t, checkpoints, 1)
			assert.JSONEq(t, `"owner-value"`, string(checkpoints[0].Result))
			assert.Empty(t, checkpoints[0].Error)
		})
	}
}
