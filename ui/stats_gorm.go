package ui

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	if err := s.db.WithContext(ctx).AutoMigrate(&JobStat{}); err != nil {
		return err
	}
	return s.ensureTimestampIndex(ctx)
}

// jobStatsTimestampIndex indexes job_stats(timestamp).
//
// The table's only index is unique on (queue, timestamp), whose LEADING column
// is queue — so the retention prune (WHERE timestamp < ?) and the all-queues
// history read cannot use it and scanned the whole table, which grows by a row
// per queue per minute.
//
// pkg/storage carries the same index as versioned migration v38, which is the
// copy that holds the fleet lock and the one schema_migrations records. This
// copy exists for the case v38 structurally cannot cover: on a first-ever boot
// Migrate() runs before the dashboard is mounted, so job_stats does not exist
// yet, v38 correctly no-ops, and it is recorded as applied and never runs again.
//
// Racing creators are expected — this runs without the fleet lock — so a
// concurrent creator winning is not an error worth failing the mount over.
const jobStatsTimestampIndex = "idx_job_stats_timestamp"

func (s *gormStatsStorage) ensureTimestampIndex(ctx context.Context) error {
	m := s.db.Migrator()
	if m.HasIndex(&JobStat{}, jobStatsTimestampIndex) {
		return nil
	}
	err := s.db.WithContext(ctx).Exec(
		"CREATE INDEX " + jobStatsTimestampIndex + " ON job_stats (timestamp)",
	).Error
	if err != nil && m.HasIndex(&JobStat{}, jobStatsTimestampIndex) {
		return nil // lost the race to a peer; the index is there, which is all we wanted
	}
	return err
}

func (s *gormStatsStorage) UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error {
	ts = ts.Truncate(time.Minute)

	for attempt := 0; attempt < 3; attempt++ {
		result := s.db.WithContext(ctx).Model(&JobStat{}).
			Where("queue = ? AND timestamp = ?", queue, ts).
			Updates(map[string]any{
				"completed": gorm.Expr("completed + ?", completed),
				"failed":    gorm.Expr("failed + ?", failed),
				"retried":   gorm.Expr("retried + ?", retried),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			return nil
		}

		insert := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&JobStat{
			Queue:     queue,
			Timestamp: ts,
			Completed: completed,
			Failed:    failed,
			Retried:   retried,
		})
		if insert.Error != nil {
			return insert.Error
		}
		if insert.RowsAffected == 1 {
			return nil
		}
	}

	return fmt.Errorf("upsert stat counters did not converge for queue %q at %s", queue, ts.Format(time.RFC3339))
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
