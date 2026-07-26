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

// QueueOldestPendingAt returns, per queue, the eligibility timestamp of the
// oldest pending job that is ALREADY DUE, for optional metrics instrumentation.
// Queues whose pending jobs are all scheduled for the future are omitted: they
// have nothing claimable, so they have no backlog age.
func (s *GormStorage) QueueOldestPendingAt(ctx context.Context) (map[string]time.Time, error) {
	if s.hotStats == nil { // zero-value storage: bypass the cache
		return s.queueOldestPendingAt(ctx)
	}
	return s.hotStats.oldestPending.do(ctx, s.hotStatsTTLValue(), cloneStringTimeMap, s.queueOldestPendingAt)
}

// queueOldestPendingAt is the single uncached loader behind every backlog-age
// surface: the jobs.backlog.oldest_age gauge, the dashboard's per-queue
// oldest_pending_at, and `sdj queues`' BACKLOG_AGE column. Two things here are
// load-bearing and both were wrong:
//
//  1. The age is anchored to the DEQUEUE'S OWN eligibility expression, not to
//     created_at. A job scheduled a month out is CREATED now and becomes
//     claimable LATER; ageing it from created_at reports backlog the queue could
//     not possibly have worked yet.
//  2. Not-yet-due rows are excluded outright. Without that, a queue holding only
//     future work reports a FUTURE timestamp — a negative age, which the metrics
//     collector silently drops and the dashboard renders as a live backlog.
//
// Together these produced the reported symptom: one scheduled job pinned
// backlog-age alerts at "hours old" forever, so operators muted the alert and
// lost the signal entirely. Reusing dequeueEligibleExpr rather than re-deriving
// the predicate is deliberate — the metric and the claim query can now only
// disagree if the shared helper changes, which changes both.
func (s *GormStorage) queueOldestPendingAt(ctx context.Context) (map[string]time.Time, error) {
	type row struct {
		Queue           string
		OldestPendingAt sql.NullString
	}

	// Same clock discipline as the dequeue: the DB server clock on
	// Postgres/MySQL so the metric cannot disagree with the claim query across
	// worker skew, and the caller's LOCAL wall clock on single-process SQLite.
	// Do NOT .UTC() the SQLite bind — SQLite stores timestamps as TEXT with
	// whatever offset they arrive with and created_at is written in local time,
	// so these comparisons are lexical and normalizing one side would mis-compare.
	var now any = time.Now()
	if s.useDBClock() {
		now = s.nowExpr()
	}

	eligExpr := s.dequeueEligibleExpr()
	var rows []row
	if err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, MIN("+eligExpr+") AS oldest_pending_at").
		Where("status = ?", core.StatusPending).
		Where(eligExpr+" <= ?", now).
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
