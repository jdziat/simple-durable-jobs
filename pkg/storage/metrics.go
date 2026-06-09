package storage

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

// QueuePendingCounts returns per-queue pending job counts for optional metrics
// instrumentation. It intentionally returns plain Go types so metrics packages
// do not depend on UI protobufs.
func (s *GormStorage) QueuePendingCounts(ctx context.Context) (map[string]int, error) {
	return s.queueCountsByStatus(ctx, core.StatusPending)
}

// QueueRunningCounts returns per-queue running job counts for optional metrics
// instrumentation. It intentionally returns plain Go types so metrics packages
// do not depend on UI protobufs.
func (s *GormStorage) QueueRunningCounts(ctx context.Context) (map[string]int, error) {
	return s.queueCountsByStatus(ctx, core.StatusRunning)
}

func (s *GormStorage) queueCountsByStatus(ctx context.Context, status core.JobStatus) (map[string]int, error) {
	type row struct {
		Queue string
		Count int
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, count(*) as count").
		Where("status = ?", status).
		Group("queue").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	counts := make(map[string]int, len(rows))
	for _, r := range rows {
		counts[r.Queue] = r.Count
	}
	return counts, nil
}
