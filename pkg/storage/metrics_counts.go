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

// ConcurrencySlotCardinality returns the number of distinct concurrency slot
// names, counted via the permanent per-slot sentinel row (job_id = nil UUID) that
// admission creates once per slot name and never deletes. Because sentinels are
// never garbage-collected (the expiry sweep preserves them), this value is
// MONOTONIC — it counts distinct slot names ever acquired, the faithful
// unbounded-growth signal for the concurrency_slots table: a high-cardinality slot
// Key (e.g. per-user) grows it without bound. It is exposed only as a SINGLE scalar
// gauge (no per-slot label) so the metric itself cannot inherit the same
// cardinality explosion it is meant to detect.
func (s *GormStorage) ConcurrencySlotCardinality(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.WithContext(ctx).
		Model(&core.ConcurrencySlot{}).
		Where("job_id = ?", core.NilUUID).
		Count(&n).Error
	return n, err
}

// RateLimitWindowCardinality returns the total number of rate-limit window rows.
// Active limit names self-cap at a few windows (per-consume GC), so the total row
// count tracks how many distinct limit names (Name+Key) hold live or abandoned
// windows — the growth signal for a high-cardinality rate-limit Key. Exposed only
// as a single scalar gauge (no per-limit label).
func (s *GormStorage) RateLimitWindowCardinality(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.WithContext(ctx).
		Model(&core.RateLimitWindow{}).
		Count(&n).Error
	return n, err
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
