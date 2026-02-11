// Package storage provides storage implementations for the jobs package.
package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// GormStorage implements Storage using GORM.
type GormStorage struct {
	db       *gorm.DB
	isSQLite bool
}

// NewGormStorage creates a new GORM-backed storage.
func NewGormStorage(db *gorm.DB) *GormStorage {
	// Detect SQLite by checking the dialect name
	isSQLite := false
	if db != nil {
		dialector := db.Dialector.Name()
		isSQLite = strings.Contains(strings.ToLower(dialector), "sqlite")
	}
	return &GormStorage{db: db, isSQLite: isSQLite}
}

// IsSQLite returns true if the storage is using SQLite.
// SQLite doesn't support FOR UPDATE SKIP LOCKED and uses a fallback strategy.
func (s *GormStorage) IsSQLite() bool {
	return s.isSQLite
}

// DB returns the underlying GORM database connection.
// This is useful for running custom queries or accessing the database directly.
func (s *GormStorage) DB() *gorm.DB {
	return s.db
}

// Migrate creates the necessary tables.
func (s *GormStorage) Migrate(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{})
}

// Enqueue adds a job to the queue.
func (s *GormStorage) Enqueue(ctx context.Context, job *core.Job) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	if job.Status == "" {
		job.Status = core.StatusPending
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
	return s.db.WithContext(ctx).Create(job).Error
}

// EnqueueUnique adds a job only if no job with the same unique key exists in pending/running state.
// Uses SELECT FOR UPDATE to prevent TOCTOU race conditions in concurrent scenarios.
// For SQLite, relies on serializable transaction isolation (SQLite's default).
func (s *GormStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	if job.Status == "" {
		job.Status = core.StatusPending
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
	job.UniqueKey = uniqueKey

	// Use transaction with row-level locking to prevent race conditions
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Where("unique_key = ?", uniqueKey).
			Where("status IN ?", []core.JobStatus{core.StatusPending, core.StatusRunning})

		// SQLite doesn't support FOR UPDATE, but its serializable transactions
		// provide equivalent protection for single-process scenarios
		if !s.isSQLite {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}

		var existing core.Job
		err := query.First(&existing).Error

		if err == nil {
			// Found existing job with same unique key
			return core.ErrDuplicateJob
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			// Unexpected error
			return err
		}

		// No existing job found, safe to create
		return tx.Create(job).Error
	})
}

// Dequeue fetches and locks the next available job.
// Uses FOR UPDATE SKIP LOCKED to prevent multiple workers from selecting the same job.
// For SQLite, uses optimistic locking with atomic update (suitable for dev/testing).
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	var job core.Job
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute) // Extended to support long-running scans

	// SQLite uses optimistic locking - no row-level locks available
	if s.isSQLite {
		return s.dequeueSQLite(ctx, queues, workerID, now, lockUntil)
	}

	// PostgreSQL/MySQL: Use FOR UPDATE SKIP LOCKED for proper distributed locking
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// FOR UPDATE SKIP LOCKED ensures:
		// 1. The selected row is locked for this transaction
		// 2. Other workers skip locked rows instead of waiting
		// This prevents duplicate job execution in distributed scenarios
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("queue IN ?", queues).
			Where("status = ?", core.StatusPending).
			Where("(run_at IS NULL OR run_at <= ?)", now).
			Where("(locked_until IS NULL OR locked_until < ?)", now).
			Order("priority DESC, created_at ASC").
			First(&job)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		job.Status = core.StatusRunning
		job.LockedBy = workerID
		job.LockedUntil = &lockUntil
		job.StartedAt = &now
		job.Attempt++

		return tx.Save(&job).Error
	})

	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, nil
	}
	return &job, nil
}

// dequeueSQLite uses optimistic locking for SQLite (dev/testing only).
// This is NOT safe for multiple concurrent workers - use PostgreSQL for production.
func (s *GormStorage) dequeueSQLite(ctx context.Context, queues []string, workerID string, now time.Time, lockUntil time.Time) (*core.Job, error) {
	var job core.Job

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Find a candidate job
		result := tx.
			Where("queue IN ?", queues).
			Where("status = ?", core.StatusPending).
			Where("(run_at IS NULL OR run_at <= ?)", now).
			Where("(locked_until IS NULL OR locked_until < ?)", now).
			Order("priority DESC, created_at ASC").
			First(&job)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		// Optimistic lock: Update only if still pending (atomic check-and-set)
		// If another worker grabbed it, RowsAffected will be 0
		updateResult := tx.Model(&core.Job{}).
			Where("id = ?", job.ID).
			Where("status = ?", core.StatusPending). // Must still be pending
			Updates(map[string]any{
				"status":       core.StatusRunning,
				"locked_by":    workerID,
				"locked_until": lockUntil,
				"started_at":   now,
				"attempt":      job.Attempt + 1,
			})

		if updateResult.Error != nil {
			return updateResult.Error
		}

		if updateResult.RowsAffected == 0 {
			// Another worker got it first - reset job so caller sees no job found
			job = core.Job{}
			return nil
		}

		// Update the job struct with the new values
		job.Status = core.StatusRunning
		job.LockedBy = workerID
		job.LockedUntil = &lockUntil
		job.StartedAt = &now
		job.Attempt++

		return nil
	})

	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, nil
	}
	return &job, nil
}

// Complete marks a job as successfully completed.
// Validates that the worker owns the job before completing.
func (s *GormStorage) Complete(ctx context.Context, jobID string, workerID string) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"status":       core.StatusCompleted,
			"completed_at": now,
			"locked_by":    "",
			"locked_until": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// Fail marks a job as failed, optionally scheduling a retry.
// Validates that the worker owns the job before failing.
// Error messages are sanitized before storage.
func (s *GormStorage) Fail(ctx context.Context, jobID string, workerID string, errMsg string, retryAt *time.Time) error {
	// Sanitize error message to prevent sensitive data leakage
	sanitizedErr := security.SanitizeErrorMessage(errMsg)

	updates := map[string]any{
		"last_error":   sanitizedErr,
		"locked_by":    "",
		"locked_until": nil,
	}

	if retryAt != nil {
		updates["status"] = core.StatusPending
		updates["run_at"] = retryAt
	} else {
		updates["status"] = core.StatusFailed
		now := time.Now()
		updates["completed_at"] = now
	}

	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(updates)

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// SaveCheckpoint stores a checkpoint for a durable call.
func (s *GormStorage) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	if cp.ID == "" {
		cp.ID = uuid.New().String()
	}
	return s.db.WithContext(ctx).Create(cp).Error
}

// GetCheckpoints retrieves all checkpoints for a job.
func (s *GormStorage) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
	var checkpoints []core.Checkpoint
	err := s.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Order("call_index ASC").
		Find(&checkpoints).Error
	return checkpoints, err
}

// DeleteCheckpoints removes all checkpoints for a job.
func (s *GormStorage) DeleteCheckpoints(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Delete(&core.Checkpoint{}).Error
}

// GetDueJobs returns jobs ready to run.
func (s *GormStorage) GetDueJobs(ctx context.Context, queues []string, limit int) ([]*core.Job, error) {
	var jobList []*core.Job
	now := time.Now()

	err := s.db.WithContext(ctx).
		Where("queue IN ?", queues).
		Where("status = ?", core.StatusPending).
		Where("(run_at IS NULL OR run_at <= ?)", now).
		Where("(locked_until IS NULL OR locked_until < ?)", now).
		Order("priority DESC, created_at ASC").
		Limit(limit).
		Find(&jobList).Error

	return jobList, err
}

// Heartbeat extends the lock on a running job.
// Returns ErrJobNotOwned if the job is no longer owned by this worker.
func (s *GormStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute)
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"locked_until":      lockUntil,
			"last_heartbeat_at": now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// ReleaseStaleLocks releases locks on jobs that haven't had a heartbeat.
func (s *GormStorage) ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error) {
	cutoff := time.Now().Add(-staleDuration)
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("status = ?", core.StatusRunning).
		Where("locked_until < ?", cutoff).
		Updates(map[string]any{
			"status":       core.StatusPending,
			"locked_by":    nil,
			"locked_until": nil,
		})
	return result.RowsAffected, result.Error
}

// GetJob retrieves a job by ID.
func (s *GormStorage) GetJob(ctx context.Context, jobID string) (*core.Job, error) {
	var job core.Job
	err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &job, err
}

// GetJobsByStatus retrieves jobs by status.
func (s *GormStorage) GetJobsByStatus(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	var jobList []*core.Job
	err := s.db.WithContext(ctx).
		Where("status = ?", status).
		Limit(limit).
		Find(&jobList).Error
	return jobList, err
}

// --- Fan-out operations ---

// CreateFanOut creates a new fan-out record.
func (s *GormStorage) CreateFanOut(ctx context.Context, fanOut *core.FanOut) error {
	if fanOut.ID == "" {
		fanOut.ID = uuid.New().String()
	}
	return s.db.WithContext(ctx).Create(fanOut).Error
}

// GetFanOut retrieves a fan-out by ID.
func (s *GormStorage) GetFanOut(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).First(&fanOut, "id = ?", fanOutID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &fanOut, err
}

// IncrementFanOutCompleted atomically increments the completed count.
func (s *GormStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"UPDATE fan_outs SET completed_count = completed_count + 1, updated_at = ? WHERE id = ?",
			time.Now(), fanOutID,
		).Error; err != nil {
			return err
		}
		return tx.First(&fanOut, "id = ?", fanOutID).Error
	})
	return &fanOut, err
}

// IncrementFanOutFailed atomically increments the failed count.
func (s *GormStorage) IncrementFanOutFailed(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"UPDATE fan_outs SET failed_count = failed_count + 1, updated_at = ? WHERE id = ?",
			time.Now(), fanOutID,
		).Error; err != nil {
			return err
		}
		return tx.First(&fanOut, "id = ?", fanOutID).Error
	})
	return &fanOut, err
}

// UpdateFanOutStatus atomically updates the status of a fan-out from pending.
// Returns (true, nil) if the status was updated, (false, nil) if already completed
// by another worker, or (false, error) on database error.
// This prevents race conditions where multiple workers try to complete the same fan-out.
func (s *GormStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
	result := s.db.WithContext(ctx).
		Model(&core.FanOut{}).
		Where("id = ? AND status = ?", fanOutID, core.FanOutPending).
		Update("status", status)
	if result.Error != nil {
		return false, result.Error
	}
	// RowsAffected == 0 means another worker already completed this fan-out
	return result.RowsAffected > 0, nil
}

// GetFanOutsByParent retrieves all fan-outs for a parent job.
func (s *GormStorage) GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*core.FanOut, error) {
	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id = ?", parentJobID).
		Order("created_at ASC").
		Find(&fanOuts).Error
	return fanOuts, err
}

// --- Sub-job operations ---

// EnqueueBatch inserts multiple jobs in a single operation.
func (s *GormStorage) EnqueueBatch(ctx context.Context, jobs []*core.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	for _, job := range jobs {
		if job.ID == "" {
			job.ID = uuid.New().String()
		}
		if job.Status == "" {
			job.Status = core.StatusPending
		}
		if job.Queue == "" {
			job.Queue = "default"
		}
	}
	return s.db.WithContext(ctx).Create(jobs).Error
}

// GetSubJobs retrieves all sub-jobs for a fan-out.
func (s *GormStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id = ?", fanOutID).
		Order("fan_out_index ASC").
		Find(&jobs).Error
	return jobs, err
}

// GetSubJobResults retrieves completed/failed sub-jobs for result collection.
func (s *GormStorage) GetSubJobResults(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusCompleted, core.StatusFailed}).
		Order("fan_out_index ASC").
		Find(&jobs).Error
	return jobs, err
}

// CancelSubJobs cancels pending/running sub-jobs for a fan-out.
func (s *GormStorage) CancelSubJobs(ctx context.Context, fanOutID string) (int64, error) {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusPending, core.StatusRunning}).
		Updates(map[string]any{
			"status":     core.StatusCancelled,
			"updated_at": time.Now(),
		})
	return result.RowsAffected, result.Error
}

// --- Waiting job operations ---

// SuspendJob suspends a job to waiting status.
// Returns ErrJobNotOwned if the job is no longer owned by this worker.
func (s *GormStorage) SuspendJob(ctx context.Context, jobID string, workerID string) error {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"status":       core.StatusWaiting,
			"locked_by":    "",
			"locked_until": nil,
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

// ResumeJob resumes a waiting job to pending status.
// Returns (true, nil) if resumed, (false, nil) if job was not in waiting status.
func (s *GormStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusWaiting).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// GetWaitingJobsToResume finds waiting jobs whose fan-out is complete.
func (s *GormStorage) GetWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	var jobs []*core.Job
	// Find waiting jobs where their fan-out is complete
	err := s.db.WithContext(ctx).Raw(`
		SELECT j.* FROM jobs j
		INNER JOIN fan_outs f ON j.id = f.parent_job_id
		WHERE j.status = ?
		AND f.status IN (?, ?)
	`, core.StatusWaiting, core.FanOutCompleted, core.FanOutFailed).Scan(&jobs).Error
	return jobs, err
}

// SaveJobResult stores the serialized result for a job.
func (s *GormStorage) SaveJobResult(ctx context.Context, jobID string, workerID string, result []byte) error {
	return s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Update("result", result).Error
}

// --- Job pause operations ---

// PauseJob pauses a job, preventing it from being picked up.
// Only pending, running, and waiting jobs can be paused.
func (s *GormStorage) PauseJob(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job core.Job
		if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("job not found: %s", jobID)
			}
			return err
		}

		// Check if already paused
		if job.Status == core.StatusPaused {
			return core.ErrJobAlreadyPaused
		}

		// Only allow pausing pending, running, or waiting jobs
		switch job.Status {
		case core.StatusPending, core.StatusRunning, core.StatusWaiting:
			// OK to pause
		default:
			return core.ErrCannotPauseStatus
		}

		return tx.Model(&core.Job{}).
			Where("id = ?", jobID).
			Updates(map[string]any{
				"status":       core.StatusPaused,
				"locked_by":    "",
				"locked_until": nil,
				"updated_at":   time.Now(),
			}).Error
	})
}

// UnpauseJob resumes a paused job back to pending status.
func (s *GormStorage) UnpauseJob(ctx context.Context, jobID string) error {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusPaused).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"updated_at": time.Now(),
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotPaused
	}
	return nil
}

// GetPausedJobs returns all paused jobs in a queue.
func (s *GormStorage) GetPausedJobs(ctx context.Context, queue string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("queue = ? AND status = ?", queue, core.StatusPaused).
		Order("created_at ASC").
		Find(&jobs).Error
	return jobs, err
}

// IsJobPaused checks if a job is paused.
func (s *GormStorage) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
	var job core.Job
	err := s.db.WithContext(ctx).
		Select("status").
		First(&job, "id = ?", jobID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return job.Status == core.StatusPaused, nil
}

// --- Queue pause operations ---

// PauseQueue marks a queue as paused.
func (s *GormStorage) PauseQueue(ctx context.Context, queue string) error {
	now := time.Now()

	var existing core.QueueState
	err := s.db.WithContext(ctx).First(&existing, "queue = ?", queue).Error

	if err == nil {
		if existing.Paused {
			return core.ErrQueueAlreadyPaused
		}
		return s.db.WithContext(ctx).
			Model(&core.QueueState{}).
			Where("queue = ?", queue).
			Updates(map[string]any{
				"paused":    true,
				"paused_at": now,
			}).Error
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	return s.db.WithContext(ctx).Create(&core.QueueState{
		Queue:    queue,
		Paused:   true,
		PausedAt: &now,
	}).Error
}

// UnpauseQueue unpauses a queue.
func (s *GormStorage) UnpauseQueue(ctx context.Context, queue string) error {
	result := s.db.WithContext(ctx).
		Model(&core.QueueState{}).
		Where("queue = ? AND paused = ?", queue, true).
		Updates(map[string]any{
			"paused":    false,
			"paused_at": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrQueueNotPaused
	}
	return nil
}

// GetPausedQueues returns all paused queue names.
func (s *GormStorage) GetPausedQueues(ctx context.Context) ([]string, error) {
	var states []core.QueueState
	err := s.db.WithContext(ctx).
		Where("paused = ?", true).
		Find(&states).Error
	if err != nil {
		return nil, err
	}

	queues := make([]string, len(states))
	for i, state := range states {
		queues[i] = state.Queue
	}
	return queues, nil
}

// IsQueuePaused checks if a queue is paused.
func (s *GormStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	var state core.QueueState
	err := s.db.WithContext(ctx).First(&state, "queue = ?", queue).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return state.Paused, nil
}

// RefreshQueueStates returns a map of queue names to their paused state.
func (s *GormStorage) RefreshQueueStates(ctx context.Context) (map[string]bool, error) {
	var states []core.QueueState
	err := s.db.WithContext(ctx).Find(&states).Error
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool)
	for _, state := range states {
		result[state.Queue] = state.Paused
	}
	return result, nil
}
