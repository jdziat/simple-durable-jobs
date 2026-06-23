package storage

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

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
		// making progress (pending/running/waiting/paused/completed) the lock
		// must continue to dedup. But a terminally dead reference — failed,
		// cancelled, or a row that no longer exists (deleted by retention) —
		// means the deduped work will never run; in that case we steal the
		// window so the re-enqueue admits fresh work.
		stolen, stolenID, err := s.stealTerminalUniqueLock(ctx, db, job, scopeHash, existingID, ttl)
		if err != nil {
			return core.NilUUID, err
		}
		if stolen {
			return stolenID, nil
		}
		return existingID, nil
	}

	return s.createUniqueLockedJob(ctx, db, job)
}

// createUniqueLockedJob inserts the job row (with the dq_ready restore path)
// once a unique lock has been won, returning the new job's ID.
func (s *GormStorage) createUniqueLockedJob(ctx context.Context, db *gorm.DB, job *core.Job) (core.UUID, error) {
	row, err := s.encodedJobForCreate(job)
	if err != nil {
		return core.NilUUID, err
	}
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
	if err := db.WithContext(ctx).Create(row).Error; err != nil {
		return core.NilUUID, err
	}
	if err := restoreDQReadyFalse(db.WithContext(ctx), dqReadyFalseIDs, dqReadyFalseRefs); err != nil {
		return core.NilUUID, err
	}
	return job.ID, nil
}

// stealTerminalUniqueLock inspects the job referenced by a live lock. If that
// job is missing (deleted) or terminally dead (failed/cancelled), it attempts a
// concurrency-safe steal of the window for job and, on winning, inserts the new
// job. The steal is a single conditional UPDATE gated on the referenced job id
// (WHERE scope_hash = ? AND job_id = existingID): at most one racer's UPDATE can
// match, so exactly one re-enqueue wins the election; the losers re-read the
// lock and dedup against whatever the winner installed (no loop).
//
// Returns (false, NilUUID, nil) when the reference is live and the caller must
// keep deduping against existingID.
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
	missing := status == ""
	// Only failed/cancelled/missing steals; pending/running/waiting/paused and
	// completed all keep deduping.
	if !missing && status != core.StatusFailed && status != core.StatusCancelled {
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
		newID, err := s.createUniqueLockedJob(ctx, db, job)
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
			result := tx.Where("scope_hash IN ?", hashes).
				Where("expires_at <= ?", nowVal).
				Delete(&core.UniqueLock{})
			deleted = result.RowsAffected
			return result.Error
		})
	})
	return deleted, err
}
