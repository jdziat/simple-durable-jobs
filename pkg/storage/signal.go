package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// SendSignal persists a named signal for a job. It is buffered: a signal sent
// before the handler waits for it is not lost.
//
// On Postgres/MySQL the fk_signals_job foreign key rejects a signal for a
// missing job. SQLite runs with foreign_keys=OFF, so the same insert would leave
// a permanently-orphaned pending signal (e.g. when the job was retention-deleted
// between a caller's existence check and this write). On SQLite the job's
// existence is therefore re-checked inside the write transaction — where the
// serialized writer makes check-then-insert atomic — returning core.ErrJobNotFound.
func (s *GormStorage) SendSignal(ctx context.Context, jobID core.UUID, name string, payload []byte) error {
	encoded, err := s.encodePayload("signal payload", string(jobID)+"/"+name, payload)
	if err != nil {
		return err
	}
	sig := &core.Signal{
		ID:      core.NewID(),
		JobID:   jobID,
		Name:    name,
		Payload: encoded,
	}
	if !s.isSQLite {
		return s.db.WithContext(ctx).Create(sig).Error
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&core.Job{}).Where("id = ?", jobID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("%w: %s", core.ErrJobNotFound, jobID)
		}
		return tx.Create(sig).Error
	})
}

// PeekSignal returns the oldest pending (unconsumed) signal of name for the job
// WITHOUT consuming it, or nil if none. Used by CheckSignal.
func (s *GormStorage) PeekSignal(ctx context.Context, jobID core.UUID, name string) (*core.Signal, error) {
	var sig core.Signal
	err := s.db.WithContext(ctx).
		Where("job_id = ? AND name = ? AND consumed_at IS NULL", jobID, name).
		Order("created_at ASC").
		First(&sig).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := s.decodeSignalPayload(&sig); err != nil {
		return nil, err
	}
	return &sig, nil
}

func (s *GormStorage) pendingSignalsLocked(tx *gorm.DB, jobID core.UUID, name string) *gorm.DB {
	return s.lockForUpdate(tx.Where("job_id = ? AND name = ? AND consumed_at IS NULL", jobID, name).Order("created_at ASC"), true)
}

// ConsumeSignal atomically takes the oldest pending signal of name for the job
// (marking it consumed), or returns nil if none are pending. Concurrent
// consumers receive disjoint signals (FOR UPDATE SKIP LOCKED on Postgres/MySQL;
// SQLite's serialized writer provides equivalent protection).
func (s *GormStorage) ConsumeSignal(ctx context.Context, jobID core.UUID, name string) (*core.Signal, error) {
	var out *core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := s.pendingSignalsLocked(tx, jobID, name)
			var sig core.Signal
			err := q.First(&sig).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // nothing pending
			}
			if err != nil {
				return err
			}
			// Write consumed_at on the DB clock (nowWriteValue) so it shares a
			// clock with the DB-clock retention cutoff in
			// DeleteConsumedSignalsOlderThan; now is the wall-clock approximation
			// returned in the struct (no caller reads it for equality).
			now := time.Now()
			res := tx.Model(&core.Signal{}).
				Where("id = ? AND consumed_at IS NULL", sig.ID).
				Update("consumed_at", s.nowWriteValue())
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				return nil // raced with another consumer; treat as none this round
			}
			sig.ConsumedAt = &now
			if err := s.decodeSignalPayload(&sig); err != nil {
				return err
			}
			out = &sig
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DrainSignals atomically consumes ALL currently-pending signals of name for the
// job, in arrival (FIFO) order, returning them. Empty slice if none.
func (s *GormStorage) DrainSignals(ctx context.Context, jobID core.UUID, name string) ([]*core.Signal, error) {
	var out []*core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := s.pendingSignalsLocked(tx, jobID, name)
			var sigs []*core.Signal
			if err := q.Find(&sigs).Error; err != nil {
				return err
			}
			if len(sigs) == 0 {
				return nil
			}
			// consumed_at on the DB clock (see ConsumeSignal); now is the
			// wall-clock approximation surfaced in the returned structs.
			now := time.Now()
			ids := make([]core.UUID, len(sigs))
			for i, sg := range sigs {
				ids[i] = sg.ID
				sg.ConsumedAt = &now
			}
			if err := tx.Model(&core.Signal{}).
				Where("id IN ?", ids).
				Update("consumed_at", s.nowWriteValue()).Error; err != nil {
				return err
			}
			if err := s.decodeSignalPayloads(sigs); err != nil {
				return err
			}
			out = sigs
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ConsumeSignalTx atomically takes the oldest pending signal of name for the job
// (marking it consumed) AND persists a replay checkpoint built from that signal,
// in a SINGLE transaction. The two writes commit or roll back together, closing
// the torn-write window that the separate consume-then-checkpoint sequence left:
// a crash either rolls back both (signal stays pending → replay re-consumes
// cleanly) or commits both (replay reads the checkpoint without re-consuming).
//
// buildCheckpoint receives the decoded, consumed signal and returns the
// fully-formed *core.Checkpoint to persist (arbitrary Result bytes — the caller
// controls the payload shape: a raw signal payload, a JSON-wrapped timeout
// object, etc.). Returning (nil, nil) skips the checkpoint write; returning an
// error rolls back the consume. When no signal is pending the closure is never
// invoked and (nil, nil) is returned (the caller suspends without a checkpoint).
//
// Concurrent consumers receive disjoint signals (FOR UPDATE SKIP LOCKED on
// Postgres/MySQL; SQLite's serialized writer provides equivalent protection).
// withSerializationRetry wraps the whole consume+checkpoint so a 40001/1213
// retry re-runs both atomically.
func (s *GormStorage) ConsumeSignalTx(ctx context.Context, jobID core.UUID, name string, buildCheckpoint func(sig *core.Signal) (*core.Checkpoint, error)) (*core.Signal, error) {
	var out *core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := s.pendingSignalsLocked(tx, jobID, name)
			var sig core.Signal
			err := q.First(&sig).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // nothing pending — caller suspends, NO checkpoint written
			}
			if err != nil {
				return err
			}
			// consumed_at on the DB clock (see ConsumeSignal); now is the
			// wall-clock approximation surfaced in the returned struct.
			now := time.Now()
			res := tx.Model(&core.Signal{}).
				Where("id = ? AND consumed_at IS NULL", sig.ID).
				Update("consumed_at", s.nowWriteValue())
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				return nil // raced with another consumer; treat as none this round
			}
			sig.ConsumedAt = &now
			if err := s.decodeSignalPayload(&sig); err != nil {
				return err
			}
			cp, err := buildCheckpoint(&sig)
			if err != nil {
				return err
			}
			if cp != nil {
				if err := s.SaveCheckpointTx(ctx, tx, cp); err != nil {
					return err
				}
			}
			out = &sig
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DrainSignalsTx atomically consumes ALL currently-pending signals of name for
// the job, in arrival (FIFO) order, AND persists a single replay checkpoint
// built from the whole batch, in ONE transaction. Unlike ConsumeSignalTx the
// checkpoint closure is ALWAYS invoked — even when zero signals are pending —
// because DrainSignals must record an empty-result checkpoint so replay of an
// empty drain is deterministic. The consume-all and the checkpoint commit or
// roll back together.
func (s *GormStorage) DrainSignalsTx(ctx context.Context, jobID core.UUID, name string, buildCheckpoint func(sigs []*core.Signal) (*core.Checkpoint, error)) ([]*core.Signal, error) {
	var out []*core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := s.pendingSignalsLocked(tx, jobID, name)
			var sigs []*core.Signal
			if err := q.Find(&sigs).Error; err != nil {
				return err
			}
			if len(sigs) > 0 {
				// consumed_at on the DB clock (see ConsumeSignal); now is the
				// wall-clock approximation surfaced in the returned structs.
				now := time.Now()
				ids := make([]core.UUID, len(sigs))
				for i, sg := range sigs {
					ids[i] = sg.ID
					sg.ConsumedAt = &now
				}
				if err := tx.Model(&core.Signal{}).
					Where("id IN ?", ids).
					Update("consumed_at", s.nowWriteValue()).Error; err != nil {
					return err
				}
				if err := s.decodeSignalPayloads(sigs); err != nil {
					return err
				}
			}
			cp, err := buildCheckpoint(sigs)
			if err != nil {
				return err
			}
			if cp != nil {
				if err := s.SaveCheckpointTx(ctx, tx, cp); err != nil {
					return err
				}
			}
			out = sigs
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetPendingSignalName returns the oldest pending signal name for jobID. It is
// an optional capability used by the worker to distinguish signal-driven wakes
// from expired durable-timer wakes before emitting JobResumedBySignal.
func (s *GormStorage) GetPendingSignalName(ctx context.Context, jobID core.UUID) (string, bool, error) {
	var sig core.Signal
	err := s.db.WithContext(ctx).
		Select("name").
		Where("job_id = ? AND consumed_at IS NULL", jobID).
		Order("created_at ASC").
		First(&sig).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return sig.Name, true, nil
}

// DeleteConsumedSignalsOlderThan deletes at most limit consumed signal rows
// whose consumed_at timestamp is older than age. Pending/unconsumed signals are
// durable workflow state and are never deleted by this retention capability.
func (s *GormStorage) DeleteConsumedSignalsOlderThan(ctx context.Context, age time.Duration, limit int) (int64, error) {
	if age <= 0 || limit <= 0 {
		return 0, nil
	}
	var cutoff any
	if s.useDBClock() {
		cutoff = s.offsetExpr(-age)
	} else {
		cutoff = time.Now().Add(-age).UTC()
	}

	var deleted int64
	err := s.withSerializationRetry(ctx, func() error {
		deleted = 0
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var ids []core.UUID
			query := tx.Model(&core.Signal{}).
				Where("consumed_at IS NOT NULL").
				Where("consumed_at < ?", cutoff).
				Order("consumed_at ASC, id ASC").
				Limit(limit)
			query = s.lockForUpdate(query, true)
			if err := query.Pluck("id", &ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			result := tx.Where("id IN ?", ids).
				Where("consumed_at IS NOT NULL").
				Where("consumed_at < ?", cutoff).
				Delete(&core.Signal{})
			deleted = result.RowsAffected
			return result.Error
		})
	})
	return deleted, err
}

// MarkWaitingWithDeadline moves an owned running job into StatusWaiting and parks
// run_at at (now + d) as the wake deadline, so the signal-resume poll wakes it to
// time out if no signal arrives first. Like MarkWaiting but with the wake
// deadline.
//
// run_at is computed on the DATABASE clock (offsetExpr) on multi-worker backends:
// it is written by one worker and compared against NOW() by the poll (possibly on
// another worker), so anchoring both to the single DB clock removes the wall-clock
// skew that would otherwise make the timeout fire early or late. SQLite is
// single-clock, so it uses the caller's time.
func (s *GormStorage) MarkWaitingWithDeadline(ctx context.Context, jobID core.UUID, workerID string, d time.Duration) error {
	rowsAffected, err := s.markWaitingWithDeadlineTx(s.db.WithContext(ctx), jobID, workerID, d)
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// markWaitingWithDeadlineTx performs the StatusRunning->StatusWaiting transition
// with a wake deadline on the caller-supplied handle (a *gorm.DB that may be a
// transaction). It returns the rows affected so the caller can map 0 ->
// core.ErrJobNotOwned and, inside a transaction, roll back any sibling writes
// (e.g. a just-written checkpoint) when ownership was lost. run_at is computed on
// the DB clock (offsetExpr) on multi-worker backends and the caller's clock on
// SQLite, exactly as MarkWaitingWithDeadline documents.
func (s *GormStorage) markWaitingWithDeadlineTx(tx *gorm.DB, jobID core.UUID, workerID string, d time.Duration) (int64, error) {
	var runAt any
	if s.useDBClock() {
		runAt = s.offsetExpr(d)
	} else {
		runAt = time.Now().Add(d)
	}
	result := tx.
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(map[string]any{
			"status":       core.StatusWaiting,
			"locked_by":    "",
			"locked_until": nil,
			"run_at":       runAt,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// SaveCheckpointAndMarkWaiting atomically persists a replay checkpoint (when
// cp != nil) AND advances an owned running job into StatusWaiting with a wake
// deadline at (now + d), in ONE transaction. The combined write closes the
// torn-write window that the old separate writeCheckpoint -> MarkWaitingWithDeadline
// pair left open: a crash between the two committed the checkpoint but left the
// job 'running', so only the stale-lock reaper recovered it and the timer fired
// late by up to the lock TTL.
//
// When ownership has been lost (the row is not running+owned-by-workerID) the
// status update affects 0 rows; this method then returns core.ErrJobNotOwned and
// the transaction rolls back, so the checkpoint is NOT persisted either —
// checkpoint and status stay consistent (never a checkpoint without the matching
// waiting status).
//
// A nil cp means "the checkpoint was already persisted on a prior replay; just
// mark waiting atomically" — no checkpoint write is attempted. The whole tx runs
// under withSerializationRetry so a 40001/1213 retry re-runs both writes together.
func (s *GormStorage) SaveCheckpointAndMarkWaiting(ctx context.Context, cp *core.Checkpoint, jobID core.UUID, workerID string, d time.Duration) error {
	return s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if cp != nil {
				if err := s.SaveCheckpointTx(ctx, tx, cp); err != nil {
					return err
				}
			}
			rowsAffected, err := s.markWaitingWithDeadlineTx(tx, jobID, workerID, d)
			if err != nil {
				return err
			}
			if rowsAffected == 0 {
				// Ownership lost: return the sentinel so the checkpoint write
				// (if any) rolls back with the failed status transition.
				return core.ErrJobNotOwned
			}
			return nil
		})
	})
}

// GetSignalWaitingJobsToResume finds waiting jobs that should be resumed for a
// signal: those with at least one pending signal (it arrived) OR whose run_at
// wake deadline has passed (a WaitForSignalTimeout that should now time out).
// The run_at comparison uses the DB clock on multi-worker backends.
//
// This is the signal analogue of GetStalledFanOutParents and closes the same
// deliver-vs-suspend race: a signal delivered in the window between the handler
// deciding to wait and MarkWaiting committing would otherwise leave the job
// waiting forever with the event-driven resume already missed.
func (s *GormStorage) GetSignalWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	return s.GetSignalWaitingJobsToResumeAfter(ctx, core.NilUUID, maxResumeBatch)
}

// GetSignalWaitingJobsToResumeAfter is the ordered, keyset-paged form of
// GetSignalWaitingJobsToResume. The worker uses it to scan past durable timers
// that have buffered user signals but should not be resumed before run_at.
func (s *GormStorage) GetSignalWaitingJobsToResumeAfter(ctx context.Context, afterJobID core.UUID, limit int) ([]*core.Job, error) {
	if limit <= 0 {
		limit = maxResumeBatch
	}
	var nowVal any
	if s.useDBClock() {
		nowVal = s.nowExpr()
	} else {
		nowVal = time.Now()
	}
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("status = ?", core.StatusWaiting).
		Where("id > ?", afterJobID).
		Where(
			s.db.Where("EXISTS (?)",
				s.db.Model(&core.Signal{}).
					Select("1").
					Where("signals.job_id = jobs.id AND signals.consumed_at IS NULL"),
			).Or("run_at IS NOT NULL AND run_at <= ?", nowVal),
		).
		// Exclude parents still waiting on a pending fan-out: resuming one before
		// its sub-jobs finish would just replay the handler and re-suspend on the
		// incomplete FanOut. Such a parent's signals stay buffered and are
		// consumed once the fan-out completes and the handler reaches its wait.
		// Mirrors the fan-out exclusion in GetWaitingJobsToResume.
		Where("NOT EXISTS (?)",
			s.db.Model(&core.FanOut{}).
				Select("1").
				Where("fan_outs.parent_job_id = jobs.id AND fan_outs.status = ?", core.FanOutPending),
		).
		Order("id ASC").
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
