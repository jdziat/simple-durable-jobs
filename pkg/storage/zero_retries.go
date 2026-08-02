package storage

import (
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// explicitZeroRetryIDs records which jobs asked for zero retries. It MUST be
// called BEFORE the insert, and on a struct GORM has never been handed.
//
// core.Job.MaxRetries declares `default:3`, and GORM substitutes a field's declared
// default whenever the value is zero — then writes the substituted value BACK into
// the struct. So after Create the caller's job says MaxRetries=3 and the original
// intent is gone: a check placed after the insert reads state that has already been
// overwritten and never fires.
//
// "Before the insert" is not sufficient on its own, and the second half of the
// contract is why. GORM performs that substitution while BUILDING the statement
// and leaves it in place when the statement then FAILS and the transaction rolls
// back. Every enqueue path is retried on transient serialization failures, so a
// capture that reads the CALLER's struct is "before the insert" on attempt 2 and
// AFTER the one on attempt 1 — it re-reads 3, the corrective UPDATE never arms,
// and the row commits max_retries=3 with a nil error. What makes this safe is
// that encodedJobForCreate now returns a fresh copy on every call, on every
// codec, so GORM never touches the struct this reads. See its godoc, and
// TestExplicitZeroRetriesSurvivesASerializationRetryOnEveryEnqueuePath.
//
// The tag cannot simply be dropped. AutoMigrate would see a changed column
// definition and REBUILD the SQLite jobs table, destroying the indexes created by
// versioned migrations — measured at 14 before the upgrade and 4 after.
//
// WHY THE FLAG AND NOT `MaxRetries == 0`
//
// The value alone does not carry the intent, and treating it as if it did is a
// silent breaking change for a documented API. core.Storage is exported, and
//
//	store.Enqueue(ctx, &core.Job{Type: "charge", Queue: "default", Args: args})
//
// leaves MaxRetries at 0 because the caller never mentioned retries — not because
// they asked for none. Keying off the value corrects that row to 0 too, so a job
// that survived a transient failure on every shipped release now dead-letters on
// the first one, with nothing in the API surface changed to warn anyone.
//
// The distinction has to come from the layer that knows: queue.Options already
// tracked it as retriesSet, and core.Job.MaxRetriesSet is how it reaches here.
// A caller that constructs a core.Job by hand and genuinely wants zero sets it
// explicitly; a caller that says nothing keeps the column default, exactly as
// before. Both halves are pinned, over the same eight enqueue paths, by
// TestExplicitZeroRetriesSurvivesEveryEnqueuePath and
// TestOmittedMaxRetriesKeepsTheColumnDefaultOnEveryEnqueuePath.
func explicitZeroRetryIDs(jobs ...*core.Job) []core.UUID {
	var ids []core.UUID
	for _, j := range jobs {
		if j != nil && j.MaxRetriesSet && j.MaxRetries == 0 {
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
