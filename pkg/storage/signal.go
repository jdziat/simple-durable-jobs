package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// SendSignal persists a named signal for a job. It is buffered: a signal sent
// before the handler waits for it is not lost.
func (s *GormStorage) SendSignal(ctx context.Context, jobID, name string, payload []byte) error {
	encoded, err := s.encodePayload("signal payload", jobID+"/"+name, payload)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Create(&core.Signal{
		ID:      uuid.New().String(),
		JobID:   jobID,
		Name:    name,
		Payload: encoded,
	}).Error
}

// PeekSignal returns the oldest pending (unconsumed) signal of name for the job
// WITHOUT consuming it, or nil if none. Used by CheckSignal.
func (s *GormStorage) PeekSignal(ctx context.Context, jobID, name string) (*core.Signal, error) {
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

// ConsumeSignal atomically takes the oldest pending signal of name for the job
// (marking it consumed), or returns nil if none are pending. Concurrent
// consumers receive disjoint signals (FOR UPDATE SKIP LOCKED on Postgres/MySQL;
// SQLite's serialized writer provides equivalent protection).
func (s *GormStorage) ConsumeSignal(ctx context.Context, jobID, name string) (*core.Signal, error) {
	var out *core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := tx.Where("job_id = ? AND name = ? AND consumed_at IS NULL", jobID, name).
				Order("created_at ASC")
			if !s.isSQLite {
				q = q.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
			}
			var sig core.Signal
			err := q.First(&sig).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // nothing pending
			}
			if err != nil {
				return err
			}
			now := time.Now()
			res := tx.Model(&core.Signal{}).
				Where("id = ? AND consumed_at IS NULL", sig.ID).
				Update("consumed_at", now)
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
func (s *GormStorage) DrainSignals(ctx context.Context, jobID, name string) ([]*core.Signal, error) {
	var out []*core.Signal
	err := s.withSerializationRetry(ctx, func() error {
		out = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := tx.Where("job_id = ? AND name = ? AND consumed_at IS NULL", jobID, name).
				Order("created_at ASC")
			if !s.isSQLite {
				q = q.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
			}
			var sigs []*core.Signal
			if err := q.Find(&sigs).Error; err != nil {
				return err
			}
			if len(sigs) == 0 {
				return nil
			}
			now := time.Now()
			ids := make([]string, len(sigs))
			for i, sg := range sigs {
				ids[i] = sg.ID
				sg.ConsumedAt = &now
			}
			if err := tx.Model(&core.Signal{}).
				Where("id IN ?", ids).
				Update("consumed_at", now).Error; err != nil {
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

// SuspendJobWithDeadline moves an owned running job into StatusWaiting and parks
// run_at at (now + d) as the wake deadline, so the signal-resume poll wakes it to
// time out if no signal arrives first. Like SuspendJob but with the wake
// deadline.
//
// run_at is computed on the DATABASE clock (offsetExpr) on multi-worker backends:
// it is written by one worker and compared against NOW() by the poll (possibly on
// another worker), so anchoring both to the single DB clock removes the wall-clock
// skew that would otherwise make the timeout fire early or late. SQLite is
// single-clock, so it uses the caller's time.
func (s *GormStorage) SuspendJobWithDeadline(ctx context.Context, jobID, workerID string, d time.Duration) error {
	var runAt any
	if s.useDBClock() {
		runAt = s.offsetExpr(d)
	} else {
		runAt = time.Now().Add(d)
	}
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"status":       core.StatusWaiting,
			"locked_by":    "",
			"locked_until": nil,
			"run_at":       runAt,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// GetSignalWaitingJobsToResume finds waiting jobs that should be resumed for a
// signal: those with at least one pending signal (it arrived) OR whose run_at
// wake deadline has passed (a WaitForSignalTimeout that should now time out).
// The run_at comparison uses the DB clock on multi-worker backends.
//
// This is the signal analogue of GetStalledFanOutParents and closes the same
// deliver-vs-suspend race: a signal delivered in the window between the handler
// deciding to wait and SuspendJob committing would otherwise leave the job
// waiting forever with the event-driven resume already missed.
func (s *GormStorage) GetSignalWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	return s.GetSignalWaitingJobsToResumeAfter(ctx, "", maxResumeBatch)
}

// GetSignalWaitingJobsToResumeAfter is the ordered, keyset-paged form of
// GetSignalWaitingJobsToResume. The worker uses it to scan past durable timers
// that have buffered user signals but should not be resumed before run_at.
func (s *GormStorage) GetSignalWaitingJobsToResumeAfter(ctx context.Context, afterJobID string, limit int) ([]*core.Job, error) {
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
