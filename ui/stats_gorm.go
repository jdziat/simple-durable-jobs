package ui

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// gormStatsStorage implements StatsStorage using GORM.
type gormStatsStorage struct {
	db *gorm.DB
}

// NewGormStatsStorage creates a GORM-backed stats storage.
func NewGormStatsStorage(db *gorm.DB) StatsStorage {
	return &gormStatsStorage{db: db}
}

func (s *gormStatsStorage) MigrateStats(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&JobStat{})
}

func (s *gormStatsStorage) UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error {
	ts = ts.Truncate(time.Minute)

	var existing JobStat
	result := s.db.WithContext(ctx).
		Where("queue = ? AND timestamp = ?", queue, ts).
		First(&existing)

	if result.Error == gorm.ErrRecordNotFound {
		return s.db.WithContext(ctx).Create(&JobStat{
			Queue:     queue,
			Timestamp: ts,
			Completed: completed,
			Failed:    failed,
			Retried:   retried,
		}).Error
	}
	if result.Error != nil {
		return result.Error
	}

	return s.db.WithContext(ctx).Model(&existing).Updates(map[string]any{
		"completed": gorm.Expr("completed + ?", completed),
		"failed":    gorm.Expr("failed + ?", failed),
		"retried":   gorm.Expr("retried + ?", retried),
	}).Error
}

func (s *gormStatsStorage) SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time, pending, running int64) error {
	ts = ts.Truncate(time.Minute)

	var existing JobStat
	result := s.db.WithContext(ctx).
		Where("queue = ? AND timestamp = ?", queue, ts).
		First(&existing)

	if result.Error == gorm.ErrRecordNotFound {
		return s.db.WithContext(ctx).Create(&JobStat{
			Queue:     queue,
			Timestamp: ts,
			Pending:   pending,
			Running:   running,
		}).Error
	}
	if result.Error != nil {
		return result.Error
	}

	return s.db.WithContext(ctx).Model(&existing).Updates(map[string]any{
		"pending": pending,
		"running": running,
	}).Error
}

func (s *gormStatsStorage) GetStatsHistory(ctx context.Context, queue string, since time.Time, until time.Time) ([]JobStat, error) {
	var stats []JobStat
	q := s.db.WithContext(ctx).Order("timestamp ASC")

	if queue != "" {
		q = q.Where("queue = ?", queue)
	}
	if !since.IsZero() {
		q = q.Where("timestamp >= ?", since)
	}
	if !until.IsZero() {
		q = q.Where("timestamp <= ?", until)
	}

	return stats, q.Find(&stats).Error
}

func (s *gormStatsStorage) PruneStats(ctx context.Context, before time.Time) (int64, error) {
	result := s.db.WithContext(ctx).Where("timestamp < ?", before).Delete(&JobStat{})
	return result.RowsAffected, result.Error
}
