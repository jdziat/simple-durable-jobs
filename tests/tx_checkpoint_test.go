package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type txCheckpointEffect struct {
	ID    uint `gorm:"primaryKey"`
	JobID string
	Name  string
}

type noTxCheckpointStorage struct {
	jobs.Storage
}

func TestSavePhaseCheckpointTx_CommitSkipsOnReplay(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)
	gormStore := store.(*jobs.GormStorage)
	require.NoError(t, gormStore.DB().AutoMigrate(&txCheckpointEffect{}))

	var effectRuns atomic.Int32
	var replaySkipped atomic.Bool

	q.Register("tx-checkpoint-commit", func(ctx context.Context, _ struct{}) error {
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "business-effect"); ok {
			replaySkipped.Store(true)
			return nil
		}

		tx := gormStore.DB().WithContext(ctx).Begin()
		if tx.Error != nil {
			return tx.Error
		}
		defer tx.Rollback()

		effectRuns.Add(1)
		if err := tx.Create(&txCheckpointEffect{
			JobID: jobs.JobIDFromContext(ctx),
			Name:  "committed",
		}).Error; err != nil {
			return err
		}
		if err := jobs.SavePhaseCheckpointTx(ctx, tx, "business-effect", "done"); err != nil {
			return err
		}
		if err := tx.Commit().Error; err != nil {
			return err
		}
		return errors.New("force replay after atomic commit")
	})

	jobID, err := q.Enqueue(ctx, "tx-checkpoint-commit", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)
	runWorkerUntilDone(t, q, store, jobID)

	assert.True(t, replaySkipped.Load())
	assert.EqualValues(t, 1, effectRuns.Load())

	var count int64
	require.NoError(t, gormStore.DB().Model(&txCheckpointEffect{}).Where("job_id = ?", jobID).Count(&count).Error)
	assert.EqualValues(t, 1, count)
	assertPhaseCheckpoint(t, store, jobID, "business-effect", "done")
}

func TestSavePhaseCheckpointTx_RollbackReexecutesOnReplay(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)
	gormStore := store.(*jobs.GormStorage)
	require.NoError(t, gormStore.DB().AutoMigrate(&txCheckpointEffect{}))

	var attempts atomic.Int32
	var misses atomic.Int32

	q.Register("tx-checkpoint-rollback", func(ctx context.Context, _ struct{}) error {
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "business-effect"); !ok {
			misses.Add(1)
		}

		tx := gormStore.DB().WithContext(ctx).Begin()
		if tx.Error != nil {
			return tx.Error
		}
		defer tx.Rollback()

		if err := tx.Create(&txCheckpointEffect{
			JobID: jobs.JobIDFromContext(ctx),
			Name:  fmt.Sprintf("attempt-%d", attempts.Add(1)),
		}).Error; err != nil {
			return err
		}
		if err := jobs.SavePhaseCheckpointTx(ctx, tx, "business-effect", "done"); err != nil {
			return err
		}
		if attempts.Load() == 1 {
			return errors.New("simulate crash before commit")
		}
		return tx.Commit().Error
	})

	jobID, err := q.Enqueue(ctx, "tx-checkpoint-rollback", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)
	runWorkerUntilDone(t, q, store, jobID)

	assert.EqualValues(t, 2, attempts.Load())
	assert.EqualValues(t, 2, misses.Load(), "rolled-back checkpoint must not be visible on replay")

	var count int64
	require.NoError(t, gormStore.DB().Model(&txCheckpointEffect{}).Where("job_id = ?", jobID).Count(&count).Error)
	assert.EqualValues(t, 1, count)
	assertPhaseCheckpoint(t, store, jobID, "business-effect", "done")
}

func TestSavePhaseCheckpointTx_CapabilityAbsentReturnsSentinel(t *testing.T) {
	ctx := context.Background()
	baseStore := openIntegrationStorage(t)
	gormStore := baseStore.(*jobs.GormStorage)
	q := jobs.New(noTxCheckpointStorage{Storage: baseStore})

	var gotErr error
	q.Register("tx-checkpoint-no-capability", func(ctx context.Context, _ struct{}) error {
		tx := gormStore.DB().WithContext(ctx).Begin()
		if tx.Error != nil {
			return tx.Error
		}
		defer tx.Rollback()
		gotErr = jobs.SavePhaseCheckpointTx(ctx, tx, "business-effect", "done")
		return gotErr
	})

	jobID, err := q.Enqueue(ctx, "tx-checkpoint-no-capability", struct{}{}, jobs.Retries(0))
	require.NoError(t, err)
	runWorkerUntilStatus(t, q, baseStore, jobID, jobs.StatusFailed)
	require.ErrorIs(t, gotErr, jobs.ErrStorageNoTxCheckpoint)
}

func TestSavePhaseCheckpointTx_OutsideHandlerReturnsError(t *testing.T) {
	ctx := context.Background()
	_, store := openIntegrationQueue(t)
	gormStore := store.(*jobs.GormStorage)

	tx := gormStore.DB().WithContext(ctx).Begin()
	require.NoError(t, tx.Error)
	defer tx.Rollback()

	err := jobs.SavePhaseCheckpointTx(ctx, tx, "business-effect", "done")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in a job handler")
}

func TestSavePhaseCheckpointTx_CrashBeforeCommitHasExactlyOnceCommittedEffect(t *testing.T) {
	ctx := context.Background()
	q, store := openIntegrationQueue(t)
	gormStore := store.(*jobs.GormStorage)
	require.NoError(t, gormStore.DB().AutoMigrate(&txCheckpointEffect{}))

	var attempts atomic.Int32
	q.Register("tx-checkpoint-crash-window", func(ctx context.Context, _ struct{}) error {
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "external-effect"); ok {
			return nil
		}

		tx := gormStore.DB().WithContext(ctx).Begin()
		if tx.Error != nil {
			return tx.Error
		}
		defer tx.Rollback()

		if err := tx.Create(&txCheckpointEffect{
			JobID: jobs.JobIDFromContext(ctx),
			Name:  "external-effect",
		}).Error; err != nil {
			return err
		}
		if err := jobs.SavePhaseCheckpointTx(ctx, tx, "external-effect", "done"); err != nil {
			return err
		}
		if attempts.Add(1) == 1 {
			return errors.New("worker died before commit")
		}
		return tx.Commit().Error
	})

	jobID, err := q.Enqueue(ctx, "tx-checkpoint-crash-window", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)
	runWorkerUntilDone(t, q, store, jobID)

	assert.EqualValues(t, 2, attempts.Load())
	var count int64
	require.NoError(t, gormStore.DB().Model(&txCheckpointEffect{}).Where("job_id = ?", jobID).Count(&count).Error)
	assert.EqualValues(t, 1, count)
	assertPhaseCheckpoint(t, store, jobID, "external-effect", "done")
}

func runWorkerUntilDone(t *testing.T, q *jobs.Queue, store jobs.Storage, jobID string) {
	t.Helper()
	runWorkerUntilStatus(t, q, store, jobID, jobs.StatusCompleted)
}

func runWorkerUntilStatus(t *testing.T, q *jobs.Queue, store jobs.Storage, jobID string, status jobs.JobStatus) {
	t.Helper()
	workerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := q.NewWorker(jobs.WithPollInterval(20*time.Millisecond), jobs.WithStaleLockInterval(0))
	done := make(chan error, 1)
	go func() { done <- w.Start(workerCtx) }()

	waitForStatus(t, store, jobID, status, 10*time.Second)
	cancel()
	err := <-done
	if err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err)
	}
}

func assertPhaseCheckpoint(t *testing.T, store jobs.Storage, jobID, phaseName, want string) {
	t.Helper()
	checkpoints, err := store.GetCheckpoints(context.Background(), jobID)
	require.NoError(t, err)
	for _, cp := range checkpoints {
		if cp.CallIndex == -1 && cp.CallType == phaseName {
			assert.JSONEq(t, fmt.Sprintf("%q", want), string(cp.Result))
			return
		}
	}
	t.Fatalf("phase checkpoint %q not found", phaseName)
}
