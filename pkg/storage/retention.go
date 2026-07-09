package storage

import (
	"context"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func quotedTerminalJobStatuses() string {
	quoted := make([]string, 0, len(core.TerminalJobStatuses))
	for _, status := range core.TerminalJobStatuses {
		quoted = append(quoted, "'"+string(status)+"'")
	}
	return strings.Join(quoted, ",")
}

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

	terminalStatuses := quotedTerminalJobStatuses()
	parentChildGuard := "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.parent_job_id = jobs.id AND c.status NOT IN (" + terminalStatuses + "))"
	rootChildGuard := "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.root_job_id = jobs.id AND c.status NOT IN (" + terminalStatuses + "))"
	if s.dialect() == dialectMySQL {
		parentChildGuard = "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.pending_parent_ref = jobs.id)"
		rootChildGuard = "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.pending_root_ref = jobs.id)"
	}

	// Never GC a completed leaf sub-job whose owning fan-out's PARENT job is not
	// yet terminal. CollectResults (pkg/fanout) rebuilds its result slice from
	// surviving sub-job rows and is only guaranteed to have run once the parent
	// reaches a terminal status; deleting a completed child before then silently
	// turns succeeded work into ErrSubJobIncomplete with no top-level error.
	//
	// Guarding on the fan_out's own status is insufficient: the fan_out flips to
	// 'completed' the moment its last child finishes, which is BEFORE a stranded
	// (paused / backlogged / outage-delayed) parent resumes to collect. We must
	// therefore key on the parent job's terminal status, not the fan_out's. The
	// two-hop join walks jobs.fan_out_id -> fan_outs.id (PK) -> fan_outs
	// .parent_job_id -> jobs.id (PK); both hops are primary-key lookups evaluated
	// only for the already status/age-limited candidate rows, so no MySQL gencol
	// is required. It lives in the id-selection query only (never the DELETE), so
	// the MySQL "can't self-reference the delete target" rule is not triggered.
	fanOutParentGuard := "NOT EXISTS (SELECT 1 FROM fan_outs f JOIN jobs pp ON pp.id = f.parent_job_id " +
		"WHERE f.id = jobs.fan_out_id AND pp.status NOT IN (" + terminalStatuses + "))"

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
				Where(fanOutParentGuard).
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
			if err := tx.Where("job_id IN ?", ids).Delete(&core.UniqueLock{}).Error; err != nil {
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
