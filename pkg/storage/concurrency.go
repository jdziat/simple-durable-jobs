package storage

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

// TryAcquireConcurrencySlot attempts to acquire or renew one leased slot for
// slotName on behalf of jobID. It is an optional storage capability used by the
// worker through type assertion; core.Storage is intentionally unchanged.
func (s *GormStorage) TryAcquireConcurrencySlot(ctx context.Context, slotName, jobID, workerID string, limit int, ttl time.Duration) (bool, error) {
	if limit <= 0 {
		return false, nil
	}
	var acquired bool
	err := s.withSerializationRetry(ctx, func() error {
		acquired = false
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var nowVal, expiresVal any
			if s.useDBClock() {
				nowVal = s.nowExpr()
				expiresVal = s.offsetExpr(ttl)
			} else {
				now := time.Now()
				nowVal = now
				expiresVal = now.Add(ttl)
			}

			// Serialize admission per slotName. Row locks on the currently-live
			// slot rows alone do not protect the "last slot" gap when two jobs
			// race to insert distinct (slot_name, job_id) rows. A permanent,
			// expired sentinel row gives every contender for the same slotName a
			// concrete row to lock before it counts and inserts.
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&core.ConcurrencySlot{
				SlotName:  slotName,
				JobID:     "",
				WorkerID:  "",
				ExpiresAt: time.Unix(0, 0).UTC(),
			}).Error; err != nil {
				return err
			}
			sentinel := tx.Where("slot_name = ? AND job_id = ?", slotName, "")
			if !s.isSQLite {
				sentinel = sentinel.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			var guard core.ConcurrencySlot
			if err := sentinel.First(&guard).Error; err != nil {
				return err
			}

			// Idempotent renewal: an already-held row never consumes another
			// slot, even if it is close to expiry.
			renew := tx.Model(&core.ConcurrencySlot{}).
				Where("slot_name = ? AND job_id = ?", slotName, jobID).
				Updates(map[string]any{
					"worker_id":  workerID,
					"expires_at": expiresVal,
				})
			if renew.Error != nil {
				return renew.Error
			}
			if renew.RowsAffected == 1 {
				acquired = true
				return nil
			}

			live := tx.Model(&core.ConcurrencySlot{}).
				Where("slot_name = ? AND expires_at >= ?", slotName, nowVal).
				Where("job_id <> ?", "")
			if !s.isSQLite {
				live = live.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
			}
			var liveSlots []core.ConcurrencySlot
			if err := live.Find(&liveSlots).Error; err != nil {
				return err
			}
			if len(liveSlots) >= limit {
				return nil
			}

			create := tx.Model(&core.ConcurrencySlot{}).
				Clauses(clause.OnConflict{DoNothing: true}).
				Create(map[string]any{
					"slot_name":  slotName,
					"job_id":     jobID,
					"worker_id":  workerID,
					"expires_at": expiresVal,
				})
			if create.Error != nil {
				return create.Error
			}
			acquired = create.RowsAffected == 1
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return acquired, nil
}

// ReleaseConcurrencySlot releases a slot held for jobID. It is idempotent:
// missing rows are already released.
func (s *GormStorage) ReleaseConcurrencySlot(ctx context.Context, slotName, jobID string) error {
	return s.db.WithContext(ctx).
		Where("slot_name = ? AND job_id = ?", slotName, jobID).
		Delete(&core.ConcurrencySlot{}).Error
}
