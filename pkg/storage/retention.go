package storage

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

// DeleteTerminalJobsOlderThan deletes at most limit jobs in one terminal status
// whose terminal timestamp is older than age. It is an optional storage
// capability used by the worker through type assertion; core.Storage is
// intentionally unchanged.
func (s *GormStorage) DeleteTerminalJobsOlderThan(ctx context.Context, status core.JobStatus, age time.Duration, limit int) (int64, error) {
	if age <= 0 || limit <= 0 {
		return 0, nil
	}
	if status != core.StatusCompleted && status != core.StatusFailed && status != core.StatusCancelled {
		return 0, nil
	}

	var cutoff any
	if s.useDBClock() {
		cutoff = s.offsetExpr(-age)
	} else {
		cutoff = time.Now().Add(-age).UTC()
	}

	var deleted int64
	err := s.withSerializationRetry(ctx, func() error {
		deleted = 0
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var ids []string
			query := tx.Model(&core.Job{}).
				Where("status = ?", status).
				Where("completed_at IS NOT NULL").
				Where("completed_at < ?", cutoff).
				Order("completed_at ASC, id ASC").
				Limit(limit)
			if !s.isSQLite {
				query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
			}
			if err := query.Pluck("id", &ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			if err := tx.Where("job_id IN ?", ids).Delete(&core.Checkpoint{}).Error; err != nil {
				return err
			}
			if err := tx.Where("job_id IN ?", ids).Delete(&core.Signal{}).Error; err != nil {
				return err
			}
			result := tx.Where("id IN ?", ids).
				Where("status = ?", status).
				Where("completed_at IS NOT NULL").
				Where("completed_at < ?", cutoff).
				Delete(&core.Job{})
			deleted = result.RowsAffected
			return result.Error
		})
	})
	return deleted, err
}
