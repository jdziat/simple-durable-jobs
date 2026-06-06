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

const (
	maxUISearchLength = 256
	maxUIQueryLimit   = 200
)

// GetQueueStats returns per-queue job counts grouped by status.
func (s *GormStorage) GetQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	return s.GetQueueDepthStats(ctx)
}

// GetQueueDepthStats returns accurate per-queue depth counts using aggregate
// queries instead of fetching job rows.
func (s *GormStorage) GetQueueDepthStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
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
		case core.StatusPaused:
			qs.Paused += r.Count
		}
	}

	// Check which queues are paused
	pausedQueues, _ := s.GetPausedQueues(ctx)
	pausedSet := make(map[string]struct{}, len(pausedQueues))
	for _, q := range pausedQueues {
		pausedSet[q] = struct{}{}
	}

	result := make([]*jobsv1.QueueStats, 0, len(statsMap))
	for _, qs := range statsMap {
		if _, ok := pausedSet[qs.Name]; ok {
			qs.IsPaused = true
		}
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
		searchTerm := filter.Search
		if len(searchTerm) > maxUISearchLength {
			searchTerm = searchTerm[:maxUISearchLength]
		}
		search := "%" + escapeLikePattern(searchTerm) + "%"
		argsExpr := "CAST(args AS TEXT)"
		if strings.Contains(strings.ToLower(s.db.Name()), "mysql") {
			argsExpr = "CONVERT(args USING utf8mb4)"
		}
		q = q.Where("id LIKE ? ESCAPE '\\' OR "+argsExpr+" LIKE ? ESCAPE '\\'", search, search)
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
	limit, offset := clampUIPagination(filter.Limit, filter.Offset)
	err := q.Order("created_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, 0, err
	}

	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

func clampUIPagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 50
	} else if limit > maxUIQueryLimit {
		limit = maxUIQueryLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func escapeLikePattern(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
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
		"status":             core.StatusPending,
		"attempt":            0,
		"last_error":         "",
		"dead_lettered_at":   nil,
		"dead_letter_reason": "",
		"locked_by":          "",
		"locked_until":       nil,
		"started_at":         nil,
		"completed_at":       nil,
		"updated_at":         now,
	}).Error
	if err != nil {
		return nil, err
	}

	job.Status = core.StatusPending
	job.Attempt = 0
	job.LastError = ""
	job.DeadLetteredAt = nil
	job.DeadLetterReason = ""
	job.LockedBy = ""
	job.LockedUntil = nil
	job.StartedAt = nil
	job.CompletedAt = nil
	job.UpdatedAt = now
	if err := s.decodeJobPayloads(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// DeleteJob permanently removes a job from the database.
func (s *GormStorage) DeleteJob(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Delete checkpoints and any buffered signals first
		if err := tx.Where("job_id = ?", jobID).Delete(&core.Checkpoint{}).Error; err != nil {
			return err
		}
		if err := tx.Where("job_id = ?", jobID).Delete(&core.Signal{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", jobID).Delete(&core.Job{}).Error
	})
}

// PurgeJobs deletes all jobs in a queue matching the given status.
func (s *GormStorage) PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error) {
	var deleted int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		matchingJobs := tx.Model(&core.Job{}).Select("id").Where("status = ?", status)
		deleteJobs := tx.Where("status = ?", status)
		if queue != "" {
			matchingJobs = matchingJobs.Where("queue = ?", queue)
			deleteJobs = deleteJobs.Where("queue = ?", queue)
		}

		if err := tx.Where("job_id IN (?)", matchingJobs).Delete(&core.Checkpoint{}).Error; err != nil {
			return err
		}

		result := deleteJobs.Delete(&core.Job{})
		deleted = result.RowsAffected
		return result.Error
	})
	return deleted, err
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

	limit, offset = clampUIPagination(limit, offset)

	var jobs []*core.Job
	err := q.Order("created_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, 0, err
	}

	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

// GetFanOutsByParents retrieves fan-outs for multiple parent jobs in one query.
func (s *GormStorage) GetFanOutsByParents(ctx context.Context, parentJobIDs []string) ([]*core.FanOut, error) {
	if len(parentJobIDs) == 0 {
		return nil, nil
	}

	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id IN ?", parentJobIDs).
		Order("parent_job_id ASC, created_at ASC").
		Find(&fanOuts).Error
	return fanOuts, err
}

// GetSubJobsByFanOuts retrieves sub-jobs for multiple fan-outs in one query.
func (s *GormStorage) GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []string) ([]*core.Job, error) {
	if len(fanOutIDs) == 0 {
		return nil, nil
	}

	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id IN ?", fanOutIDs).
		Order("fan_out_id ASC, fan_out_index ASC").
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
