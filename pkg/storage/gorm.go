// Package storage provides storage implementations for the jobs package.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// GormStorage implements Storage using GORM.
type GormStorage struct {
	db *gorm.DB
}

// NewGormStorage creates a new GORM-backed storage.
func NewGormStorage(db *gorm.DB) *GormStorage {
	return &GormStorage{db: db}
}

// Migrate creates the necessary tables.
func (s *GormStorage) Migrate(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{})
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

	// Check for existing job with same unique key that's pending or running
	var count int64
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("unique_key = ?", uniqueKey).
		Where("status IN ?", []core.JobStatus{core.StatusPending, core.StatusRunning}).
		Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return core.ErrDuplicateJob
	}

	return s.db.WithContext(ctx).Create(job).Error
}

// Dequeue fetches and locks the next available job.
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	var job core.Job
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute) // Extended to support long-running scans

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
func (s *GormStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute)
	return s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"locked_until":      lockUntil,
			"last_heartbeat_at": now,
		}).Error
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

// UpdateFanOutStatus updates the status of a fan-out.
func (s *GormStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) error {
	return s.db.WithContext(ctx).
		Model(&core.FanOut{}).
		Where("id = ?", fanOutID).
		Update("status", status).Error
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
func (s *GormStorage) SuspendJob(ctx context.Context, jobID string, workerID string) error {
	return s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"status":       core.StatusWaiting,
			"locked_by":    "",
			"locked_until": nil,
			"updated_at":   time.Now(),
		}).Error
}

// ResumeJob resumes a waiting job to pending status.
func (s *GormStorage) ResumeJob(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusWaiting).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"updated_at": time.Now(),
		}).Error
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
