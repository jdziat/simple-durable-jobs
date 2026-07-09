package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// QueueDeadLetterCounts returns per-queue dead-letter job counts for optional
// metrics instrumentation.
func (s *GormStorage) QueueDeadLetterCounts(ctx context.Context) (map[string]int, error) {
	if s.hotStats == nil { // zero-value storage: bypass the cache
		return s.queueDeadLetterCounts(ctx)
	}
	return s.hotStats.deadLetter.do(ctx, s.hotStatsTTLValue(), cloneStringIntMap, s.queueDeadLetterCounts)
}

func (s *GormStorage) queueDeadLetterCounts(ctx context.Context) (map[string]int, error) {
	type row struct {
		Queue string
		Count int
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, count(*) as count").
		Where("dead_lettered_at IS NOT NULL").
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

// QueueOldestPendingAt returns the oldest pending job creation timestamp by
// queue for optional metrics instrumentation.
func (s *GormStorage) QueueOldestPendingAt(ctx context.Context) (map[string]time.Time, error) {
	if s.hotStats == nil { // zero-value storage: bypass the cache
		return s.queueOldestPendingAt(ctx)
	}
	return s.hotStats.oldestPending.do(ctx, s.hotStatsTTLValue(), cloneStringTimeMap, s.queueOldestPendingAt)
}

func (s *GormStorage) queueOldestPendingAt(ctx context.Context) (map[string]time.Time, error) {
	type row struct {
		Queue           string
		OldestPendingAt sql.NullString
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, MIN(created_at) AS oldest_pending_at").
		Where("status = ?", core.StatusPending).
		Group("queue").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	oldestByQueue := make(map[string]time.Time, len(rows))
	for _, r := range rows {
		oldestPendingAt, ok := parseDBTimestamp(r.OldestPendingAt.String)
		if r.OldestPendingAt.Valid && ok {
			oldestByQueue[r.Queue] = oldestPendingAt
		}
	}
	return oldestByQueue, nil
}
