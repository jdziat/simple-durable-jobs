package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs"
	"gorm.io/gorm"
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
	return s.db.WithContext(ctx).AutoMigrate(&jobs.Job{}, &jobs.Checkpoint{})
}

// Enqueue adds a job to the queue.
func (s *GormStorage) Enqueue(ctx context.Context, job *jobs.Job) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	if job.Status == "" {
		job.Status = jobs.StatusPending
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
	return s.db.WithContext(ctx).Create(job).Error
}

// Dequeue fetches and locks the next available job.
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*jobs.Job, error) {
	var job jobs.Job
	now := time.Now()
	lockUntil := now.Add(5 * time.Minute)

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.
			Where("queue IN ?", queues).
			Where("status = ?", jobs.StatusPending).
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

		job.Status = jobs.StatusRunning
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
func (s *GormStorage) Complete(ctx context.Context, jobID string, result []byte) error {
	now := time.Now()
	return s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("id = ?", jobID).
		Updates(map[string]interface{}{
			"status":       jobs.StatusCompleted,
			"completed_at": now,
			"locked_by":    nil,
			"locked_until": nil,
		}).Error
}

// Fail marks a job as failed, optionally scheduling a retry.
func (s *GormStorage) Fail(ctx context.Context, jobID string, errMsg string, retryAt *time.Time) error {
	updates := map[string]interface{}{
		"last_error":   errMsg,
		"locked_by":    nil,
		"locked_until": nil,
	}

	if retryAt != nil {
		updates["status"] = jobs.StatusPending
		updates["run_at"] = retryAt
	} else {
		updates["status"] = jobs.StatusFailed
		now := time.Now()
		updates["completed_at"] = now
	}

	return s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("id = ?", jobID).
		Updates(updates).Error
}

// SaveCheckpoint stores a checkpoint for a durable call.
func (s *GormStorage) SaveCheckpoint(ctx context.Context, cp *jobs.Checkpoint) error {
	if cp.ID == "" {
		cp.ID = uuid.New().String()
	}
	return s.db.WithContext(ctx).Create(cp).Error
}

// GetCheckpoints retrieves all checkpoints for a job.
func (s *GormStorage) GetCheckpoints(ctx context.Context, jobID string) ([]jobs.Checkpoint, error) {
	var checkpoints []jobs.Checkpoint
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
		Delete(&jobs.Checkpoint{}).Error
}

// GetDueJobs returns jobs ready to run.
func (s *GormStorage) GetDueJobs(ctx context.Context, queues []string, limit int) ([]*jobs.Job, error) {
	var jobList []*jobs.Job
	now := time.Now()

	err := s.db.WithContext(ctx).
		Where("queue IN ?", queues).
		Where("status = ?", jobs.StatusPending).
		Where("(run_at IS NULL OR run_at <= ?)", now).
		Where("(locked_until IS NULL OR locked_until < ?)", now).
		Order("priority DESC, created_at ASC").
		Limit(limit).
		Find(&jobList).Error

	return jobList, err
}

// Heartbeat extends the lock on a running job.
func (s *GormStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	lockUntil := time.Now().Add(5 * time.Minute)
	return s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Update("locked_until", lockUntil).Error
}

// ReleaseStaleLocks releases locks on jobs that haven't had a heartbeat.
func (s *GormStorage) ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error) {
	cutoff := time.Now().Add(-staleDuration)
	result := s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("status = ?", jobs.StatusRunning).
		Where("locked_until < ?", cutoff).
		Updates(map[string]interface{}{
			"status":       jobs.StatusPending,
			"locked_by":    nil,
			"locked_until": nil,
		})
	return result.RowsAffected, result.Error
}

// GetJob retrieves a job by ID.
func (s *GormStorage) GetJob(ctx context.Context, jobID string) (*jobs.Job, error) {
	var job jobs.Job
	err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &job, err
}

// GetJobsByStatus retrieves jobs by status.
func (s *GormStorage) GetJobsByStatus(ctx context.Context, status jobs.JobStatus, limit int) ([]*jobs.Job, error) {
	var jobList []*jobs.Job
	err := s.db.WithContext(ctx).
		Where("status = ?", status).
		Limit(limit).
		Find(&jobList).Error
	return jobList, err
}
