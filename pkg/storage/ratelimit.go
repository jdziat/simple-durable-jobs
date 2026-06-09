package storage

import (
	"context"
	"math"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

// TryConsumeRate attempts to consume one unit from a named fixed-window rate
// limit. It is an optional storage capability used by the worker through type
// assertion; core.Storage is intentionally unchanged.
func (s *GormStorage) TryConsumeRate(ctx context.Context, limitName string, perSecond float64, window time.Duration, now time.Time) (bool, error) {
	if limitName == "" || perSecond <= 0 || window <= 0 {
		return false, nil
	}
	ceiling := int(math.Ceil(perSecond * window.Seconds()))
	if ceiling <= 0 {
		return false, nil
	}

	var allowed bool
	err := s.withSerializationRetry(ctx, func() error {
		allowed = false
		effectiveNow := now
		if effectiveNow.IsZero() {
			var err error
			effectiveNow, err = s.rateLimitNow(ctx, now)
			if err != nil {
				return err
			}
		}
		windowStart := effectiveNow.Truncate(window)

		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := s.deleteExpiredRateLimitWindows(tx, windowStart.Add(-2*window)); err != nil {
				return err
			}

			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&core.RateLimitWindow{
				LimitName:   limitName,
				WindowStart: windowStart,
				Count:       0,
			}).Error; err != nil {
				return err
			}

			query := tx.Where("limit_name = ? AND window_start = ?", limitName, windowStart)
			if !s.isSQLite {
				query = query.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			var row core.RateLimitWindow
			if err := query.First(&row).Error; err != nil {
				return err
			}
			if row.Count >= ceiling {
				return nil
			}

			result := tx.Model(&core.RateLimitWindow{}).
				Where("limit_name = ? AND window_start = ? AND count < ?", limitName, windowStart, ceiling).
				Updates(map[string]any{
					"count":      gorm.Expr("count + 1"),
					"updated_at": effectiveNow,
				})
			if result.Error != nil {
				return result.Error
			}
			allowed = result.RowsAffected == 1
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return allowed, nil
}

func (s *GormStorage) deleteExpiredRateLimitWindows(tx *gorm.DB, cutoff time.Time) error {
	return tx.Where("window_start < ?", cutoff).Delete(&core.RateLimitWindow{}).Error
}

func (s *GormStorage) rateLimitNow(ctx context.Context, fallback time.Time) (time.Time, error) {
	if !s.useDBClock() {
		if fallback.IsZero() {
			return time.Now(), nil
		}
		return fallback, nil
	}
	var now time.Time
	sql := "SELECT NOW()"
	if s.dialect() == dialectMySQL {
		sql = "SELECT NOW(6)"
	}
	if err := s.db.WithContext(ctx).Raw(sql).Scan(&now).Error; err != nil {
		return time.Time{}, err
	}
	return now, nil
}
