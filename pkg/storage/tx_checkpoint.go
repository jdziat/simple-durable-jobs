package storage

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TxCheckpointer is the optional storage capability for persisting checkpoints
// through a caller-owned GORM transaction. Implementations must not commit or
// roll back the supplied transaction.
type TxCheckpointer interface {
	SaveCheckpointTx(ctx context.Context, tx *gorm.DB, cp *core.Checkpoint) error
}

// OwnedTxCheckpointer is the ownership-fenced form used by a running handler.
// The checkpoint write is accepted only while workerID still owns the running
// job, and the ownership check participates in the caller's transaction.
type OwnedTxCheckpointer interface {
	SaveCheckpointTxOwned(ctx context.Context, tx *gorm.DB, cp *core.Checkpoint, workerID string) error
}

var _ TxCheckpointer = (*GormStorage)(nil)
var _ OwnedTxCheckpointer = (*GormStorage)(nil)

// SaveCheckpointTx stores a checkpoint using the caller-supplied transaction
// handle.
func (s *GormStorage) SaveCheckpointTx(ctx context.Context, tx *gorm.DB, cp *core.Checkpoint) error {
	row, err := s.checkpointRowForSave(cp)
	if err != nil {
		return err
	}
	return s.saveCheckpointRow(ctx, tx, row)
}

// SaveCheckpointTxOwned stores a checkpoint through a caller-owned transaction
// only while workerID owns the running job. Locking the job row and writing the
// checkpoint on the same transaction closes the check-then-write lease race.
func (s *GormStorage) SaveCheckpointTxOwned(ctx context.Context, tx *gorm.DB, cp *core.Checkpoint, workerID string) error {
	row, err := s.checkpointRowForSave(cp)
	if err != nil {
		return err
	}
	if err := s.requireCheckpointOwner(tx.WithContext(ctx), cp.JobID, workerID); err != nil {
		return err
	}
	return s.saveCheckpointRow(ctx, tx, row)
}

func (s *GormStorage) checkpointRowForSave(cp *core.Checkpoint) (*core.Checkpoint, error) {
	if cp.ID == "" {
		cp.ID = core.NewID()
	}
	return s.encodedCheckpointForSave(cp)
}

func (s *GormStorage) saveCheckpointRow(ctx context.Context, db *gorm.DB, row *core.Checkpoint) error {
	return db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "job_id"}, {Name: "call_index"}, {Name: "call_type"}},
			DoUpdates: clause.AssignmentColumns(checkpointConflictColumns),
		}).
		Create(row).Error
}

func (s *GormStorage) requireCheckpointOwner(tx *gorm.DB, jobID core.UUID, workerID string) error {
	var job core.Job
	err := s.lockForUpdate(tx.Model(&core.Job{}).Select("id"), false).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Take(&job).Error
	if err == nil {
		return nil
	}
	if err == gorm.ErrRecordNotFound {
		return core.ErrJobNotOwned
	}
	return err
}
