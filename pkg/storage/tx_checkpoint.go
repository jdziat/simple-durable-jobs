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

var _ TxCheckpointer = (*GormStorage)(nil)

// SaveCheckpointTx stores a checkpoint using the caller-supplied transaction
// handle.
func (s *GormStorage) SaveCheckpointTx(ctx context.Context, tx *gorm.DB, cp *core.Checkpoint) error {
	if cp.ID == "" {
		cp.ID = core.NewID()
	}
	row, err := s.encodedCheckpointForSave(cp)
	if err != nil {
		return err
	}
	return tx.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "job_id"}, {Name: "call_index"}, {Name: "call_type"}},
			DoUpdates: clause.AssignmentColumns([]string{"result", "error", "error_kind", "error_cause", "error_delay_nanos"}),
		}).
		Create(row).Error
}
