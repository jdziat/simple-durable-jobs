package storage

import (
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// explicitZeroRetryIDs records which jobs asked for zero retries. It MUST be called
// BEFORE the insert.
//
// core.Job.MaxRetries declares `default:3`, and GORM substitutes a field's declared
// default whenever the value is zero — then writes the substituted value BACK into
// the struct. So after Create the caller's job says MaxRetries=3 and the original
// intent is gone: a check placed after the insert reads state that has already been
// overwritten and never fires.
//
// The tag cannot simply be dropped. AutoMigrate would see a changed column
// definition and REBUILD the SQLite jobs table, destroying the indexes created by
// versioned migrations — measured at 14 before the upgrade and 4 after.
func explicitZeroRetryIDs(jobs ...*core.Job) []core.UUID {
	var ids []core.UUID
	for _, j := range jobs {
		if j != nil && j.MaxRetries == 0 {
			ids = append(ids, j.ID)
		}
	}
	return ids
}

// applyExplicitZeroRetries writes the intended 0 back, in the caller's transaction,
// so the row is never visible carrying a value its author did not ask for. It is a
// no-op unless some job actually requested zero retries.
func applyExplicitZeroRetries(tx *gorm.DB, ids []core.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	return tx.Model(&core.Job{}).Where("id IN ?", ids).
		UpdateColumn("max_retries", 0).Error
}
