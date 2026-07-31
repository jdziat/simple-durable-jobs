package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestPhaseCheckpoint_DuplicateNameInOneRunFailsLoud is the end-to-end
// regression for the phase-name identity defect. A phase checkpoint is keyed by
// {CallIndex: -1, CallType: phaseName} alone, so two DIFFERENT phases that share
// a name are ONE checkpoint: the second save upserted over the first, and from
// then on the first phase's LoadPhaseCheckpoint returned the SECOND phase's
// result while every replay skipped both as already-done.
func TestPhaseCheckpoint_DuplicateNameInOneRunFailsLoud(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)
	gormStore := store.(*jobs.GormStorage)

	var chargeRuns, receiptRuns atomic.Int32

	// The copy-paste defect: "send-receipt" was written but the phase name of the
	// block it was copied from was left behind.
	q.Register("dup-phase-name", func(ctx context.Context, _ struct{}) error {
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "settle"); !ok {
			chargeRuns.Add(1)
			if err := savePhaseInTx(ctx, gormStore, "settle", "charge"); err != nil {
				return err
			}
		}
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "settle"); !ok {
			receiptRuns.Add(1)
			if err := savePhaseInTx(ctx, gormStore, "settle", "receipt"); err != nil {
				return err
			}
		}
		return nil
	})

	// Retries are deliberately available: a retry replays the first phase's
	// checkpoint for BOTH phases and would complete the job with the receipt
	// silently skipped, so the refusal has to be terminal to be worth anything.
	jobID, err := q.Enqueue(ctx, "dup-phase-name", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)
	runWorkerUntilStatus(t, q, store, jobID, jobs.StatusFailed)

	failed, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Contains(t, failed.LastError, "settle",
		"the failure must name the phase that was saved twice")
	assert.Equal(t, 1, failed.Attempt, "the duplicate name is not retryable")
	assert.EqualValues(t, 1, chargeRuns.Load())
	assert.EqualValues(t, 1, receiptRuns.Load(),
		"the second phase's body still runs; it is its SAVE that is refused")

	// The crux: the first phase's checkpoint is intact, not carrying the second
	// phase's result under the first phase's name.
	assertPhaseCheckpoint(t, store, jobID, "settle", "charge")
}

// TestPhaseCheckpoint_DistinctNamesInOneRunStillComplete is the positive leg: a
// refusal that fired on every second phase would also make the test above pass,
// so pin that two properly named phases in one run both save and the job
// completes.
func TestPhaseCheckpoint_DistinctNamesInOneRunStillComplete(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)
	gormStore := store.(*jobs.GormStorage)

	q.Register("distinct-phase-names", func(ctx context.Context, _ struct{}) error {
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "charge-card"); !ok {
			if err := savePhaseInTx(ctx, gormStore, "charge-card", "charge"); err != nil {
				return err
			}
		}
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "send-receipt"); !ok {
			if err := savePhaseInTx(ctx, gormStore, "send-receipt", "receipt"); err != nil {
				return err
			}
		}
		return nil
	})

	jobID, err := q.Enqueue(ctx, "distinct-phase-names", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)
	runWorkerUntilDone(t, q, store, jobID)

	assertPhaseCheckpoint(t, store, jobID, "charge-card", "charge")
	assertPhaseCheckpoint(t, store, jobID, "send-receipt", "receipt")
}

// TestPhaseCheckpoint_SameNameAcrossRunsStillSaves is the replay leg: the phase
// name is claimed per RUN, not per job. A phase already recorded durably by an
// earlier attempt must still be saveable when the handler redoes it — a guard
// keyed off the replayed checkpoints instead of this run's writes would wedge
// every such job.
func TestPhaseCheckpoint_SameNameAcrossRunsStillSaves(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)

	var attempts atomic.Int32

	q.Register("resave-phase-across-runs", func(ctx context.Context, _ struct{}) error {
		attempt := attempts.Add(1)
		if err := jobs.SavePhaseCheckpoint(ctx, "settle", fmt.Sprintf("run-%d", attempt)); err != nil {
			return err
		}
		if attempt == 1 {
			return errors.New("force one replay")
		}
		return nil
	})

	jobID, err := q.Enqueue(ctx, "resave-phase-across-runs", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)
	runWorkerUntilDone(t, q, store, jobID)

	assert.EqualValues(t, 2, attempts.Load())
	assertPhaseCheckpoint(t, store, jobID, "settle", "run-2")
}

// savePhaseInTx commits a phase checkpoint through a caller-owned transaction,
// the atomic effect+checkpoint pattern from the transactional-checkpoints docs.
func savePhaseInTx(ctx context.Context, store *jobs.GormStorage, phaseName, result string) error {
	return store.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return jobs.SavePhaseCheckpointTx(ctx, tx, phaseName, result)
	})
}
