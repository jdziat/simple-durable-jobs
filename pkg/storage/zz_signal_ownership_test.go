package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedRunningJobOwnedBy creates a job in 'running' held by workerID.
func seedRunningJobOwnedBy(t *testing.T, ctx context.Context, s *GormStorage, workerID string) core.UUID {
	t.Helper()
	job := &core.Job{Type: "sig.wait", Queue: "default", Args: []byte(`{}`), MaxRetries: 3}
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", job.ID).
		Updates(map[string]any{"status": core.StatusRunning, "locked_by": workerID}).Error)
	return job.ID
}

// ConsumeSignalTxOwned must not report "you are not the owner" and "there is
// nothing pending" with the SAME value, because its caller acts on the difference
// and cannot recover it.
//
// The two returns were byte-identical — (nil, nil) — from these two branches:
//
//	if owned == 0 { return nil }                    // not the current owner
//	if errors.Is(err, gorm.ErrRecordNotFound) { ... return nil }  // nothing pending
//
// WaitForSignalTimeout reads that nil as "no signal arrived", and if its deadline
// has passed it writes a DURABLE 'timed out' verdict through an UNFENCED
// SaveCheckpoint upsert. So a run that has already lost its lease can commit the
// timeout verdict for a signal that is sitting in the table, in time, undelivered.
// Replay then reads that checkpoint as authoritative and the job completes down the
// wrong branch, with the signal still pending forever.
//
// The asymmetry is the tell: the SUSPEND path in this same file already fences,
// returning core.ErrJobNotOwned when its ownership-scoped UPDATE affects zero rows.
// The one branch that writes a terminal verdict was the one branch with no fence.
func TestConsumeSignalTxOwned_DistinguishesLostOwnershipFromNoSignal(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const owner = "worker-A"
	const usurper = "worker-B"
	const name = "approval"

	build := func(sig *core.Signal) (*core.Checkpoint, error) {
		return &core.Checkpoint{
			ID: core.NewID(), JobID: sig.JobID, CallIndex: 0,
			CallType: core.CheckpointTypeSignalTimeoutPrefix + name,
			Result:   json.RawMessage(`{"resolved":true}`),
		}, nil
	}

	t.Run("owner with a pending signal receives it", func(t *testing.T) {
		jobID := seedRunningJobOwnedBy(t, ctx, s, owner)
		require.NoError(t, s.SendSignal(ctx, jobID, name, json.RawMessage(`"APPROVED"`)))

		sig, err := s.ConsumeSignalTxOwned(ctx, jobID, owner, name, build)
		require.NoError(t, err)
		require.NotNil(t, sig, "the owner must receive a pending signal")
	})

	t.Run("owner with NO pending signal gets a nil signal and no error", func(t *testing.T) {
		jobID := seedRunningJobOwnedBy(t, ctx, s, owner)

		sig, err := s.ConsumeSignalTxOwned(ctx, jobID, owner, name, build)
		require.NoError(t, err, "'nothing pending' is not an error — the caller suspends or times out")
		require.Nil(t, sig)
	})

	// The defect, isolated to one variable. Same storage, same pending signal, same
	// call — only the workerID differs from the case above it.
	t.Run("NON-owner with a pending signal must not look like 'nothing pending'", func(t *testing.T) {
		jobID := seedRunningJobOwnedBy(t, ctx, s, owner)
		require.NoError(t, s.SendSignal(ctx, jobID, name, json.RawMessage(`"APPROVED"`)))

		// The lease moved on: a reaper released the row and another worker claimed
		// it. The original run is still executing and does not know.
		require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
			Where("id = ?", jobID).Update("locked_by", usurper).Error)

		sig, err := s.ConsumeSignalTxOwned(ctx, jobID, usurper+"-not-me", name, build)

		require.Nil(t, sig, "a non-owner must never consume the signal")
		assert.ErrorIs(t, err, core.ErrJobNotOwned,
			"a non-owner must be told so. Returning (nil, nil) here is indistinguishable "+
				"from 'nothing pending', and WaitForSignalTimeout turns that into a durable "+
				"'timed out' verdict written through an unfenced upsert — while the signal "+
				"is still pending, in time, and now undeliverable")

		// The signal must survive: it was never this run's to consume.
		pending, perr := s.PeekSignal(ctx, jobID, name)
		require.NoError(t, perr)
		assert.NotNil(t, pending,
			"the signal must remain pending for the run that actually owns the job")
	})
}
