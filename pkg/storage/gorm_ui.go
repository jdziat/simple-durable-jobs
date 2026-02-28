package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
)

// GetQueueStats returns per-queue job counts grouped by status.
func (s *GormStorage) GetQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	type row struct {
		Queue  string
		Status string
		Count  int64
	}
	var rows []row
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, status, count(*) as count").
		Group("queue, status").
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	statsMap := make(map[string]*jobsv1.QueueStats)
	for _, r := range rows {
		qs, ok := statsMap[r.Queue]
		if !ok {
			qs = &jobsv1.QueueStats{Name: r.Queue}
			statsMap[r.Queue] = qs
		}
		switch core.JobStatus(r.Status) {
		case core.StatusPending:
			qs.Pending += r.Count
		case core.StatusRunning:
			qs.Running += r.Count
		case core.StatusCompleted:
			qs.Completed += r.Count
		case core.StatusFailed:
			qs.Failed += r.Count
		}
	}

	result := make([]*jobsv1.QueueStats, 0, len(statsMap))
	for _, qs := range statsMap {
		result = append(result, qs)
	}
	return result, nil
}

// SearchJobs returns jobs matching the filter with pagination and total count.
func (s *GormStorage) SearchJobs(ctx context.Context, filter core.JobFilter) ([]*core.Job, int64, error) {
	q := s.db.WithContext(ctx).Model(&core.Job{})

	if filter.Status != "" {
		q = q.Where("status = ?", filter.Status)
	}
	if filter.Queue != "" {
		q = q.Where("queue = ?", filter.Queue)
	}
	if filter.Type != "" {
		q = q.Where("type = ?", filter.Type)
	}
	if filter.Search != "" {
		search := "%" + filter.Search + "%"
		q = q.Where("id LIKE ? OR CAST(args AS TEXT) LIKE ?", search, search)
	}
	if !filter.Since.IsZero() {
		q = q.Where("created_at >= ?", filter.Since)
	}
	if !filter.Until.IsZero() {
		q = q.Where("created_at <= ?", filter.Until)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var jobs []*core.Job
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	err := q.Order("created_at DESC").
		Offset(filter.Offset).
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}

// RetryJob resets a failed or cancelled job back to pending for re-execution.
func (s *GormStorage) RetryJob(ctx context.Context, jobID string) (*core.Job, error) {
	var job core.Job
	err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error
	if err != nil {
		return nil, err
	}

	if job.Status != core.StatusFailed && job.Status != core.StatusCancelled {
		return nil, fmt.Errorf("jobs: cannot retry job with status %q", job.Status)
	}

	now := time.Now()
	err = s.db.WithContext(ctx).Model(&job).Updates(map[string]any{
		"status":       core.StatusPending,
		"attempt":      0,
		"last_error":   "",
		"locked_by":    "",
		"locked_until": nil,
		"started_at":   nil,
		"completed_at": nil,
		"updated_at":   now,
	}).Error
	if err != nil {
		return nil, err
	}

	job.Status = core.StatusPending
	job.Attempt = 0
	job.LastError = ""
	job.LockedBy = ""
	job.LockedUntil = nil
	job.StartedAt = nil
	job.CompletedAt = nil
	job.UpdatedAt = now
	return &job, nil
}

// DeleteJob permanently removes a job from the database.
func (s *GormStorage) DeleteJob(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Delete checkpoints first
		if err := tx.Where("job_id = ?", jobID).Delete(&core.Checkpoint{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", jobID).Delete(&core.Job{}).Error
	})
}

// PurgeJobs deletes all jobs in a queue matching the given status.
func (s *GormStorage) PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error) {
	q := s.db.WithContext(ctx).Where("status = ?", status)
	if queue != "" {
		q = q.Where("queue = ?", queue)
	}
	result := q.Delete(&core.Job{})
	return result.RowsAffected, result.Error
}

// GetWorkflowRoots returns root workflow jobs (jobs with children but no parent).
func (s *GormStorage) GetWorkflowRoots(ctx context.Context, status string, limit, offset int) ([]*core.Job, int64, error) {
	q := s.db.WithContext(ctx).Model(&core.Job{}).
		Where("parent_job_id IS NULL").
		Where("id IN (SELECT DISTINCT parent_job_id FROM jobs WHERE parent_job_id IS NOT NULL)")

	if status != "" {
		statuses := strings.Split(status, ",")
		q = q.Where("status IN ?", statuses)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if limit <= 0 {
		limit = 50
	}

	var jobs []*core.Job
	err := q.Order("created_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}
