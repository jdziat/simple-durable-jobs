package storage

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

var _ core.UniqueLockEnqueuer = (*GormStorage)(nil)
var _ core.UniqueLockSweeper = (*GormStorage)(nil)

// EnqueueWithUniqueLock atomically enqueues job under a time-bounded unique
// lock. When a live lock already exists, it returns that original job ID and
// does not insert job.
func (s *GormStorage) EnqueueWithUniqueLock(ctx context.Context, job *core.Job, scopeHash string, ttl time.Duration) (core.UUID, error) {
	if scopeHash == "" || ttl <= 0 {
		return core.NilUUID, core.ErrStorageNoUniqueLocks
	}
	fillEnqueueDefaults(job)

	var jobID core.UUID
	err := s.withSerializationRetry(ctx, func() error {
		jobID = core.NilUUID
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			id, err := s.enqueueWithUniqueLockDB(ctx, tx, job, scopeHash, ttl)
			if err != nil {
				return err
			}
			jobID = id
			return nil
		})
	})
	if err != nil {
		return core.NilUUID, err
	}
	return jobID, nil
}

func (s *GormStorage) enqueueWithUniqueLockDB(ctx context.Context, db *gorm.DB, job *core.Job, scopeHash string, ttl time.Duration) (core.UUID, error) {
	acquired, existingID, err := s.tryAcquireUniqueLock(ctx, db, scopeHash, job.ID, ttl)
	if err != nil {
		return core.NilUUID, err
	}
	if !acquired {
		// A live lock is held by existingID. If the referenced job is still
		// making progress (pending/running/waiting/paused) or already succeeded
		// (completed) the lock must continue to dedup. Only a terminally dead
		// reference — failed or cancelled — means the deduped work will never
		// run; in that case we steal the window so the re-enqueue admits fresh
		// work. A MISSING row is deliberately not a steal trigger; see
		// stealTerminalUniqueLock.
		stolen, stolenID, err := s.stealTerminalUniqueLock(ctx, db, job, scopeHash, existingID, ttl)
		if err != nil {
			return core.NilUUID, err
		}
		if stolen {
			return stolenID, nil
		}
		return existingID, nil
	}

	return s.createUniqueLockedJob(ctx, db, job, scopeHash)
}

// createUniqueLockedJob inserts the job row (with the dq_ready restore path)
// once a unique lock has been won, returning the new job's ID.
func (s *GormStorage) createUniqueLockedJob(ctx context.Context, db *gorm.DB, job *core.Job, scopeHash string) (core.UUID, error) {
	row, err := s.encodedJobForCreate(job)
	if err != nil {
		return core.NilUUID, err
	}
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
	// A job may carry BOTH a windowed key (IdempotencyKey/UniqueFor, which is the
	// lock we just won) and an active-only key (queue.Unique), which is enforced by
	// the partial unique index. Winning the lock says nothing about the index, so a
	// bare Create surfaced a raw driver constraint error — a 1062/23505/UNIQUE
	// string the caller cannot match on — where the documented contract for the
	// same collision on the ordinary enqueue path is core.ErrDuplicateJob.
	//
	// Use the same OnConflict + RowsAffected mapping the enqueue backstop uses,
	// rather than sniffing driver error text, which differs per dialect.
	//
	// The lock we hold would then reference a job that was not inserted. Usually
	// the whole enqueue transaction rolls back on the returned error and the lock
	// goes with it — but on the EnqueueWithUniqueLockTx path the caller owns the
	// transaction and may commit anyway, which would strand a window guarding work
	// that never ran. Release it explicitly instead of relying on a later reader
	// inferring "job row missing, therefore stealable": that inference is exactly
	// what let a retention-deleted completed job's live window be stolen and the
	// guarded work run twice (see stealTerminalUniqueLock).
	//
	// The predicate names BOTH the scope we just won and our freshly-minted job id.
	// scope_hash is the primary key, so it identifies exactly one window, and the
	// job_id term ensures we only release it while WE still hold it — a concurrent
	// enqueue that already re-won the scope is never clobbered.
	//
	// Keying on job_id ALONE would be wrong, and a skeptic caught it here: nothing
	// forces a job id to be referenced by only one scope (fillEnqueueDefaults mints
	// an id only when the caller left it empty, so a caller-supplied id can appear
	// under two windows). A job_id-only delete would then destroy an unrelated LIVE
	// window and reintroduce the double charge this whole change exists to prevent,
	// from a new direction.
	zeroRetryIDs := explicitZeroRetryIDs(row)
	result := db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error == nil && result.RowsAffected > 0 {
		if err := applyExplicitZeroRetries(db.WithContext(ctx), zeroRetryIDs); err != nil {
			return core.UUID(""), err
		}
	}
	if result.Error != nil {
		return core.NilUUID, result.Error
	}
	if result.RowsAffected == 0 {
		if err := db.WithContext(ctx).
			Where("scope_hash = ? AND job_id = ?", scopeHash, job.ID).
			Delete(&core.UniqueLock{}).Error; err != nil {
			return core.NilUUID, err
		}
		return core.NilUUID, core.ErrDuplicateJob
	}
	if err := restoreDQReadyFalse(db.WithContext(ctx), dqReadyFalseIDs, dqReadyFalseRefs); err != nil {
		return core.NilUUID, err
	}
	return job.ID, nil
}

// stealTerminalUniqueLock inspects the job referenced by a live lock. If that
// job is terminally dead — failed or cancelled — it attempts a concurrency-safe
// steal of the window for job and, on winning, inserts the new job. The steal is
// a single conditional UPDATE gated on the referenced job id (WHERE scope_hash =
// ? AND job_id = existingID): at most one racer's UPDATE can match, so exactly
// one re-enqueue wins the election; the losers re-read the lock and dedup against
// whatever the winner installed (no loop).
//
// A MISSING job row is NOT a steal trigger. It used to be, on the reasoning that
// a vanished row meant the deduped work never ran — but the commonest way for a
// row to vanish is that it COMPLETED and was garbage-collected, so the inference
// inverted the guarantee it was protecting: a 90-day IdempotencyKey stopped
// deduplicating the moment the (much shorter) retention window deleted the job,
// and the replayed request charged the card a second time. A window is now
// released only by an explicit act of whoever removes the job — DeleteJob,
// PurgeJobs and Requeue's subtree replay all delete the lock with the row, and
// automatic retention refuses to delete a job a live window still references
// (retention.go) — or by the window's own expires_at lapsing, after which the
// ordinary acquire path takes over. A dangling live lock therefore fails CLOSED
// (keeps deduplicating for the remainder of the TTL the operator asked for)
// rather than open.
//
// Returns (false, NilUUID, nil) when the reference is live or missing and the
// caller must keep deduping against existingID.
func (s *GormStorage) stealTerminalUniqueLock(ctx context.Context, db *gorm.DB, job *core.Job, scopeHash string, existingID core.UUID, ttl time.Duration) (bool, core.UUID, error) {
	if existingID == core.NilUUID {
		return false, core.NilUUID, nil
	}

	var status core.JobStatus
	err := db.WithContext(ctx).Model(&core.Job{}).
		Select("status").
		Where("id = ?", existingID).
		Scan(&status).Error
	if err != nil {
		return false, core.NilUUID, err
	}
	// Only failed/cancelled steals. pending/running/waiting/paused, completed, and
	// a missing row all keep deduping — see the doc comment for why "missing" is
	// deliberately not a steal trigger.
	if status != core.StatusFailed && status != core.StatusCancelled {
		return false, core.NilUUID, nil
	}

	var nowVal, expiresVal any
	if s.useDBClock() {
		nowVal = s.nowExpr()
		expiresVal = s.offsetExpr(ttl)
	} else {
		now := time.Now().UTC()
		nowVal = now
		expiresVal = now.Add(ttl)
	}

	// Conditional election: only the racer whose WHERE still sees job_id =
	// existingID wins. RowsAffected == 1 means we won; 0 means a concurrent
	// enqueue already rewrote the lock, so re-read and dedup against its winner.
	result := db.WithContext(ctx).Exec(
		"UPDATE unique_locks SET job_id = ?, expires_at = ?, created_at = ? WHERE scope_hash = ? AND job_id = ?",
		job.ID, expiresVal, nowVal, scopeHash, existingID,
	)
	if result.Error != nil {
		return false, core.NilUUID, result.Error
	}
	if result.RowsAffected == 1 {
		newID, err := s.createUniqueLockedJob(ctx, db, job, scopeHash)
		if err != nil {
			return false, core.NilUUID, err
		}
		return true, newID, nil
	}

	var lock core.UniqueLock
	if err := db.WithContext(ctx).First(&lock, "scope_hash = ?", scopeHash).Error; err != nil {
		return false, core.NilUUID, err
	}
	return true, lock.JobID, nil
}

func (s *GormStorage) tryAcquireUniqueLock(ctx context.Context, db *gorm.DB, scopeHash string, jobID core.UUID, ttl time.Duration) (bool, core.UUID, error) {
	var nowVal, expiresVal any
	if s.useDBClock() {
		nowVal = s.nowExpr()
		expiresVal = s.offsetExpr(ttl)
	} else {
		now := time.Now().UTC()
		nowVal = now
		expiresVal = now.Add(ttl)
	}

	var result *gorm.DB
	switch s.dialect() {
	case dialectMySQL:
		result = db.WithContext(ctx).Exec(`
			INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
			VALUES (?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				job_id = IF(expires_at <= ?, VALUES(job_id), job_id),
				created_at = IF(expires_at <= ?, VALUES(created_at), created_at),
				expires_at = IF(expires_at <= ?, VALUES(expires_at), expires_at)
		`, scopeHash, jobID, expiresVal, nowVal, nowVal, nowVal, nowVal)
	case dialectPostgres:
		result = db.WithContext(ctx).Exec(`
			INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (scope_hash) DO UPDATE SET
				job_id = EXCLUDED.job_id,
				expires_at = EXCLUDED.expires_at,
				created_at = EXCLUDED.created_at
			WHERE unique_locks.expires_at <= ?
		`, scopeHash, jobID, expiresVal, nowVal, nowVal)
	default:
		result = db.WithContext(ctx).Exec(`
			INSERT INTO unique_locks (scope_hash, job_id, expires_at, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (scope_hash) DO UPDATE SET
				job_id = excluded.job_id,
				expires_at = excluded.expires_at,
				created_at = excluded.created_at
			WHERE unique_locks.expires_at <= ?
		`, scopeHash, jobID, expiresVal, nowVal, nowVal)
	}
	if result.Error != nil {
		return false, core.NilUUID, result.Error
	}
	if result.RowsAffected > 0 {
		if s.dialect() == dialectMySQL {
			var lock core.UniqueLock
			if err := db.WithContext(ctx).First(&lock, "scope_hash = ?", scopeHash).Error; err != nil {
				return false, core.NilUUID, err
			}
			if lock.JobID != jobID {
				return false, lock.JobID, nil
			}
		}
		return true, core.NilUUID, nil
	}

	var lock core.UniqueLock
	err := db.WithContext(ctx).First(&lock, "scope_hash = ?", scopeHash).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, core.NilUUID, gorm.ErrRecordNotFound
	}
	if err != nil {
		return false, core.NilUUID, err
	}
	return false, lock.JobID, nil
}

// DeleteExpiredUniqueLocks deletes expired windowed enqueue deduplication locks.
func (s *GormStorage) DeleteExpiredUniqueLocks(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}

	var nowVal any
	if s.useDBClock() {
		nowVal = s.nowExpr()
	} else {
		nowVal = time.Now().UTC()
	}

	var deleted int64
	err := s.withSerializationRetry(ctx, func() error {
		deleted = 0
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var hashes []string
			query := tx.Model(&core.UniqueLock{}).
				Where("expires_at <= ?", nowVal).
				Order("expires_at ASC, scope_hash ASC").
				Limit(limit)
			query = s.lockForUpdate(query, true)
			if err := query.Pluck("scope_hash", &hashes).Error; err != nil {
				return err
			}
			if len(hashes) == 0 {
				return nil
			}
			// Bound the literal IN-list independently of the caller's batch size,
			// as the retention sweep and PurgeJobs do: an oversized list exceeds the
			// driver's bind-parameter ceiling (SQLite ~32k, Postgres 65535) and makes
			// EVERY pass fail, so the sweep would silently stop collecting expired
			// windows and unique_locks would grow forever.
			for _, chunk := range chunkIDs(hashes, retentionDeleteChunkSize) {
				result := tx.Where("scope_hash IN ?", chunk).
					Where("expires_at <= ?", nowVal).
					Delete(&core.UniqueLock{})
				if result.Error != nil {
					return result.Error
				}
				deleted += result.RowsAffected
			}
			return nil
		})
	})
	return deleted, err
}
