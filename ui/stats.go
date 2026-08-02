package ui

import (
	"context"
	"time"
)

// JobStat stores per-queue statistics bucketed by minute.
type JobStat struct {
	ID        uint      `gorm:"primaryKey"`
	Queue     string    `gorm:"uniqueIndex:idx_job_stats_queue_ts;size:255;not null"`
	Timestamp time.Time `gorm:"uniqueIndex:idx_job_stats_queue_ts;not null"`
	Pending   int64     `gorm:"default:0"`
	Running   int64     `gorm:"default:0"`
	Completed int64     `gorm:"default:0"`
	Failed    int64     `gorm:"default:0"`
	Retried   int64     `gorm:"default:0"`
}

// StatsStorage is the interface for stats persistence.
//
// Timestamps are written on the collector process's LOCAL clock face
// (StatsCollector.snapshot truncates time.Now() to the minute), so JobStat.Timestamp
// legitimately holds MORE THAN ONE offset — a single DST zone renders two of them
// across a fall-back. Both bounded methods below are therefore defined over
// INSTANTS, not over the rendered wall text: an implementation that compares the
// stored value lexically drops the fall-back hour from GetStatsHistory and, worse,
// makes PruneStats delete rows NEWER than its cutoff. See statsTimestampPredicate
// in stats_gorm.go for the SQLite-specific form and its measured cost.
type StatsStorage interface {
	MigrateStats(ctx context.Context) error
	UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error
	SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time, pending, running int64) error
	// GetStatsHistory returns every bucket whose instant is in [since, until],
	// inclusive on both ends and ordered by instant. A zero bound is unbounded.
	GetStatsHistory(ctx context.Context, queue string, since time.Time, until time.Time) ([]JobStat, error)
	// PruneStats deletes buckets STRICTLY older than before, and returns how many.
	PruneStats(ctx context.Context, before time.Time) (int64, error)
}
