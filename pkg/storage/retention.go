package storage

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
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

	parentChildGuard := "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.parent_job_id = jobs.id AND c.status NOT IN ('completed','failed','cancelled'))"
	rootChildGuard := "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.root_job_id = jobs.id AND c.status NOT IN ('completed','failed','cancelled'))"
	if s.dialect() == dialectMySQL {
		parentChildGuard = "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.pending_parent_ref = jobs.id)"
		rootChildGuard = "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.pending_root_ref = jobs.id)"
	}

	var deleted int64
	err := s.withSerializationRetry(ctx, func() error {
		deleted = 0
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var ids []core.UUID
			query := tx.Model(&core.Job{}).
				Where("status = ?", status).
				Where("completed_at IS NOT NULL").
				Where("completed_at < ?", cutoff).
				Where(parentChildGuard).
				Where(rootChildGuard).
				Where("NOT EXISTS (SELECT 1 FROM fan_outs f WHERE f.parent_job_id = jobs.id AND f.status = 'pending')").
				Order("completed_at ASC, id ASC").
				Limit(limit)
			query = s.lockForUpdate(query, true)
			if err := query.Pluck("id", &ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			if err := tx.Where("parent_job_id IN ?", ids).Delete(&core.FanOut{}).Error; err != nil {
				return err
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
