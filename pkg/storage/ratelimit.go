package storage

import (
	"context"
	"math"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
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
			if err := s.deleteExpiredRateLimitWindows(tx, limitName, windowStart.Add(-2*window)); err != nil {
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
			query = s.lockForUpdate(query, false)
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

// deleteExpiredRateLimitWindows GCs only THIS limit's expired windows. Scoping to
// limit_name is both correct — a short-window limit's per-call GC must not delete a
// coexisting long-window limit's still-live counter (teardown g4) — and efficient:
// the (limit_name, window_start) primary key serves "limit_name = ? AND
// window_start < ?" as an index range, where the previous bare "window_start < ?"
// could not use the limit_name-leading PK and scanned the whole table each call.
func (s *GormStorage) deleteExpiredRateLimitWindows(tx *gorm.DB, limitName string, cutoff time.Time) error {
	return tx.Where("limit_name = ? AND window_start < ?", limitName, cutoff).Delete(&core.RateLimitWindow{}).Error
}

// ReleaseRate returns one previously-consumed unit to a fleet rate limit's current
// window. The worker calls it to refund limits it already consumed when a LATER
// fleet limit in the same admission denies the job (teardown g4): without the
// refund, every multi-limit bounce permanently drains the earlier limits' windows
// for a job that never runs, silently starving them. Guarded count > 0 so a refund
// that races a window rollover can never drive a counter negative. It is an
// optional storage capability used by the worker through type assertion.
func (s *GormStorage) ReleaseRate(ctx context.Context, limitName string, window time.Duration) error {
	if limitName == "" || window <= 0 {
		return nil
	}
	return s.withSerializationRetry(ctx, func() error {
		now, err := s.rateLimitNow(ctx, time.Time{})
		if err != nil {
			return err
		}
		windowStart := now.Truncate(window)
		return s.db.WithContext(ctx).Model(&core.RateLimitWindow{}).
			Where("limit_name = ? AND window_start = ? AND count > 0", limitName, windowStart).
			Update("count", gorm.Expr("count - 1")).Error
	})
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
