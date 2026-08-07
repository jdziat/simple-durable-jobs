package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
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
// signalFIFOOrder is the ORDER BY implementing the documented FIFO delivery
// contract, written so it is correct against rows stored on EITHER clock face.
//
// created_at is now written in UTC, but an upgraded database still holds rows
// written by older releases wearing the sender's local offset — and during a
// rolling deploy both faces are being written concurrently. A bare
// `created_at ASC` is a LEXICAL compare on SQLite, so those rows sort by their
// digits rather than their instants: measured at TZ=Asia/Kolkata, a signal sent
// 30ms EARLIER by an older binary was delivered SECOND.
//
// julianday() is computed from the parsed instant and is face-independent.
//
// WHY THIS IS AFFORDABLE HERE, WHERE THE UI LIST REJECTED THE SAME FIX.
// gorm_ui.go documents a measured 554x regression from julianday-ordering the
// dead-letter list, because that query walks an index over the whole table in
// order. This one does not: the WHERE narrows to the PENDING signals of a single
// (job_id, name) before anything is sorted. EXPLAIN QUERY PLAN confirms
// idx_signals_pending still serves the lookup on both forms; julianday adds only
// "USE TEMP B-TREE FOR ORDER BY" over that handful of rows.
func (s *GormStorage) signalFIFOOrder() string {
	if !s.isSQLite {
		return "created_at ASC"
	}
	return "julianday(created_at) ASC"
}

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
		// Set EXPLICITLY on one clock face rather than left to GORM's
		// autoCreateTime, which stamps a bare time.Now() wearing the SENDING
		// process's local offset.
		//
		// created_at is not decoration here: it is the ORDER BY that implements the
		// documented FIFO delivery contract (core.Signal's godoc), and on SQLite it
		// is TEXT compared LEXICALLY. Two senders on different offsets — a UTC
		// container and a local-TZ host, the ordinary shape in a mixed deployment —
		// therefore invert permanently, and a single host inverts across a DST
		// fall-back. Measured: signals sent 30ms apart delivered newest-first.
		//
		// The sibling column is the tell: consumed_at already goes through
		// nowWriteValue(). This one was left on the GORM default.
		//
		// UTC() rather than nowWriteValue(): the value must be comparable with rows
		// written by OTHER processes, and on DB-clock backends Create cannot take a
		// SQL expression here without a raw insert. UTC is the one face every
		// sender agrees on without a round trip.
		CreatedAt: time.Now().UTC(),
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
		Order(s.signalFIFOOrder()).
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
	return s.lockForUpdate(tx.Where("job_id = ? AND name = ? AND consumed_at IS NULL", jobID, name).Order(s.signalFIFOOrder()), true)
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
			// CHUNKED: `ids` is every pending signal of this name, which is
			// user-data-unbounded — nothing caps how many signals may buffer for a
			// job. Unchunked, a backlog past the driver's bind-parameter ceiling
			// (SQLite 32766, Postgres/MySQL 65535) fails the statement outright, and
			// because DrainSignals is the ONLY bulk consume the backlog can never be
			// cleared: the handler errors, retries, fails identically and dead-letters.
			// Measured: the threshold is ~32765, and single-signal ConsumeSignal on
			// the same backlog still works, which isolates the IN-list as the cause.
			for _, chunk := range chunkIDs(ids, retentionDeleteChunkSize) {
				if err := tx.Model(&core.Signal{}).
					Where("id IN ?", chunk).
					Update("consumed_at", s.nowWriteValue()).Error; err != nil {
					return err
				}
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
// ConsumeSignalTx atomically consumes the oldest pending signal of name and
// persists the replay checkpoint built from it, in ONE transaction. It performs
// NO ownership check; the handler path uses ConsumeSignalTxOwned. Retained with
// its original signature for v4 API compatibility.
func (s *GormStorage) ConsumeSignalTx(ctx context.Context, jobID core.UUID, name string, buildCheckpoint func(sig *core.Signal) (*core.Checkpoint, error)) (*core.Signal, error) {
	return s.consumeSignalTx(ctx, jobID, "", false, name, buildCheckpoint)
}

// ConsumeSignalTxOwned is ConsumeSignalTx gated on job ownership (locked_by =
// workerID AND status = running). Under the documented at-least-once double-run
// edge (a stale lock is reaped and a second worker replays concurrently), a
// NON-OWNER execution must not consume the delivered signal — doing so sets
// consumed_at and makes the real owner find nothing pending and take the timeout
// branch, silently losing the signal (the consumed row cannot be re-delivered).
// A non-owner consumes nothing and suspends; the owner delivers. This is the
// method the signal handler path uses.
func (s *GormStorage) ConsumeSignalTxOwned(ctx context.Context, jobID core.UUID, workerID string, name string, buildCheckpoint func(sig *core.Signal) (*core.Checkpoint, error)) (*core.Signal, error) {
	return s.consumeSignalTx(ctx, jobID, workerID, true, name, buildCheckpoint)
}

func (s *GormStorage) consumeSignalTx(ctx context.Context, jobID core.UUID, workerID string, gate bool, name string, buildCheckpoint func(sig *core.Signal) (*core.Checkpoint, error)) (*core.Signal, error) {
	var out *core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if gate {
				var owned int64
				if err := tx.Model(&core.Job{}).
					Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
					Count(&owned).Error; err != nil {
					return err
				}
				if owned == 0 {
					// ErrJobNotOwned, NOT nil. A bare nil here is byte-identical to
					// the "nothing pending" return below, and the caller acts on the
					// difference: WaitForSignalTimeout reads nil as "no signal
					// arrived" and, once its deadline has passed, commits a DURABLE
					// 'timed out' verdict through an unfenced SaveCheckpoint upsert.
					// A run that had already lost its lease could therefore decide
					// the timeout for a signal sitting in the table, in time and
					// undelivered — and replay treats that checkpoint as
					// authoritative, so the job completes down the wrong branch.
					//
					// This method's own godoc says a non-owner "suspends"; it could
					// not, because it was never told. The suspend path in this file
					// already fences this way.
					return core.ErrJobNotOwned
				}
			}
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
	return s.drainSignalsTx(ctx, jobID, "", false, name, buildCheckpoint)
}

// DrainSignalsTxOwned is DrainSignalsTx gated on job ownership (see
// ConsumeSignalTxOwned) so a non-owner double-run execution cannot drain
// delivered signals out from under the real owner. Used by the signal handler
// path.
func (s *GormStorage) DrainSignalsTxOwned(ctx context.Context, jobID core.UUID, workerID string, name string, buildCheckpoint func(sigs []*core.Signal) (*core.Checkpoint, error)) ([]*core.Signal, error) {
	return s.drainSignalsTx(ctx, jobID, workerID, true, name, buildCheckpoint)
}

func (s *GormStorage) drainSignalsTx(ctx context.Context, jobID core.UUID, workerID string, gate bool, name string, buildCheckpoint func(sigs []*core.Signal) (*core.Checkpoint, error)) ([]*core.Signal, error) {
	var out []*core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if gate {
				var owned int64
				if err := tx.Model(&core.Job{}).
					Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
					Count(&owned).Error; err != nil {
					return err
				}
				if owned == 0 {
					// Same fence, same reason as consumeSignalTx above: a non-owner
					// must be told it is a non-owner, not handed the value that means
					// "there was nothing here".
					return core.ErrJobNotOwned
				}
			}
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
				// Chunked for the same bind-parameter ceiling as the sibling above;
				// this is the handler-facing path (jobs.DrainSignals ->
				// DrainSignalsTxOwned), so it is the one a user actually hits.
				for _, chunk := range chunkIDs(ids, retentionDeleteChunkSize) {
					if err := tx.Model(&core.Signal{}).
						Where("id IN ?", chunk).
						Update("consumed_at", s.nowWriteValue()).Error; err != nil {
						return err
					}
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
		Order(s.signalFIFOOrder()).
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
			// Bound the literal IN-list, as DeleteTerminalJobsOlderThan and
			// DeleteExpiredUniqueLocks do. This is the third sweep driven by
			// RetentionBatchSize; the option is clamped, but this method is
			// EXPORTED, so a direct caller passing a large limit would otherwise
			// hit the driver's bind-parameter ceiling (SQLite ~32k, Postgres
			// 65535) and get deleted=0 on every pass, forever.
			for _, chunk := range chunkIDs(ids, retentionDeleteChunkSize) {
				result := tx.Where("id IN ?", chunk).
					Where("consumed_at IS NOT NULL").
					Where("consumed_at < ?", cutoff).
					Delete(&core.Signal{})
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
	return s.markWaitingWithDeadlineForSignalTx(tx, jobID, workerID, d, "")
}

// markWaitingWithDeadlineForSignalTx is markWaitingWithDeadlineTx that also
// records which signal name may wake the job. An empty name means "not recorded"
// and leaves the resume poll permissive for this job.
func (s *GormStorage) markWaitingWithDeadlineForSignalTx(tx *gorm.DB, jobID core.UUID, workerID string, d time.Duration, signalName string) (int64, error) {
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
			"status":              core.StatusWaiting,
			"locked_by":           "",
			"locked_until":        nil,
			"run_at":              runAt,
			"waiting_signal_name": signalName,
			"updated_at":          time.Now(),
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
	return s.SaveCheckpointAndMarkWaitingForSignal(ctx, cp, jobID, workerID, d, "")
}

// SaveCheckpointAndMarkWaitingForSignal is SaveCheckpointAndMarkWaiting that also
// records, in the SAME transaction, which signal name may wake the job. Recording
// it atomically with the status transition matters: a job that reached waiting
// without its name recorded would fall back to the permissive resume and could be
// re-dispatched and fully replayed on every poll tick by a signal it will never
// consume.
//
// Pass signal.SleepCheckpointType for a durable sleep, which no signal should
// wake. An empty name records nothing and keeps the permissive behaviour.
func (s *GormStorage) SaveCheckpointAndMarkWaitingForSignal(ctx context.Context, cp *core.Checkpoint, jobID core.UUID, workerID string, d time.Duration, signalName string) error {
	return s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if cp != nil {
				if err := s.SaveCheckpointTx(ctx, tx, cp); err != nil {
					return err
				}
			}
			rowsAffected, err := s.markWaitingWithDeadlineForSignalTx(tx, jobID, workerID, d, signalName)
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
					Where("signals.job_id = jobs.id AND signals.consumed_at IS NULL").
					// Correlate on the name the job actually suspended on. Without
					// this, a pending signal the handler will never consume wakes
					// the job on EVERY tick forever: it re-dispatches, replays,
					// re-suspends, and the surplus signal is still pending for the
					// next tick. jobs.waiting_signal_name = '' means "not
					// recorded" (fan-out suspends, or a core.Storage that does not
					// implement SignalWaitMarker) and keeps the permissive
					// behaviour so such a wait still resumes.
					Where("(jobs.waiting_signal_name = '' OR signals.name = jobs.waiting_signal_name)"),
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
