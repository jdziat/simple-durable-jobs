package ui

import (
	"context"
	"time"
)

// JobStat stores per-queue statistics bucketed by minute.
type JobStat struct {
	ID        uint      `gorm:"primaryKey"`
	Queue     string    `gorm:"index:idx_job_stats_queue_ts;size:255;not null"`
	Timestamp time.Time `gorm:"index:idx_job_stats_queue_ts;not null"`
	Pending   int64     `gorm:"default:0"`
	Running   int64     `gorm:"default:0"`
	Completed int64     `gorm:"default:0"`
	Failed    int64     `gorm:"default:0"`
	Retried   int64     `gorm:"default:0"`
}

// StatsStorage is the interface for stats persistence.
type StatsStorage interface {
	MigrateStats(ctx context.Context) error
	UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error
	SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time, pending, running int64) error
	GetStatsHistory(ctx context.Context, queue string, since time.Time, until time.Time) ([]JobStat, error)
	PruneStats(ctx context.Context, before time.Time) (int64, error)
}
