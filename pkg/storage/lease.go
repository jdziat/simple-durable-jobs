package storage

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// TryAcquireRecoveryLease attempts to take (or renew) the named maintenance
// lease for owner, valid for ttl. It returns true if owner now holds the lease.
//
// The lease elects a single worker to run a fleet-wide periodic scan (the
// fan-out recovery poll) so N workers don't all run the same correlated
// subquery every tick. Semantics:
//
//   - A non-owner acquires only when the current lease has expired (the holder
//     died or stopped renewing). After a holder dies the lease fails over within
//     one ttl.
//   - The current owner always renews (the "OR owner = ?" clause), so the holder
//     keeps the lease across ticks instead of having to wait for its own lease to
//     expire before it can scan again.
//
// ExpiresAt is stamped from the DB clock on multi-worker backends so lease
// arbitration is immune to per-worker clock skew, exactly like the job lock.
// On SQLite (single process) the wall clock is used.
//
// This is exposed as an optional storage capability (discovered via type
// assertion), not a core.Storage method, so custom backends that don't
// implement it simply fall back to every worker polling.
func (s *GormStorage) TryAcquireRecoveryLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	var acquired bool
	err := s.withSerializationRetry(ctx, func() error {
		acquired = false
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Ensure a row exists, seeded as already-expired so the first
			// caller's conditional UPDATE below can take it.
			seed := core.Lease{Name: name, Owner: "", ExpiresAt: time.Unix(0, 0).UTC()}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
				return err
			}

			var nowVal, expiresVal any
			if s.useDBClock() {
				nowVal = s.nowExpr()
				expiresVal = s.offsetExpr(ttl)
			} else {
				now := time.Now()
				nowVal = now
				expiresVal = now.Add(ttl)
			}

			res := tx.Model(&core.Lease{}).
				Where("name = ? AND (expires_at < ? OR owner = ?)", name, nowVal, owner).
				Updates(map[string]any{
					"owner":      owner,
					"expires_at": expiresVal,
				})
			if res.Error != nil {
				return res.Error
			}
			acquired = res.RowsAffected == 1
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return acquired, nil
}
