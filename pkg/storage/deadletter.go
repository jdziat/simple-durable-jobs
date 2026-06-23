package storage

import (
	"context"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

const (
	defaultDeadLetterLimit = 50
	maxDeadLetterLimit     = 1000
)

// ListDeadLettered returns jobs with explicit DLQ metadata, ordered newest
// dead-letter first. This is an optional storage capability; core.Storage stays
// unchanged.
func (s *GormStorage) ListDeadLettered(ctx context.Context, filter core.DeadLetterFilter) ([]*core.Job, error) {
	q := s.deadLetterQuery(ctx, filter)

	var jobs []*core.Job
	limit, offset := clampDeadLetterPagination(filter.Limit, filter.Offset)
	// Default to most-recently-dead first; honor an explicit (whitelisted) sort
	// when the dashboard requests one so the dead-letter view's sortable headers
	// aren't a no-op.
	order := "dead_lettered_at DESC, id DESC"
	if filter.SortKey != "" {
		order = jobSortOrder(core.JobFilter{SortKey: filter.SortKey, SortDir: filter.SortDir})
	}
	if err := q.Order(order).
		Offset(offset).
		Limit(limit).
		Find(&jobs).Error; err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// CountDeadLettered returns the number of jobs with explicit DLQ metadata for
// the supplied filter. This is an optional storage capability.
func (s *GormStorage) CountDeadLettered(ctx context.Context, filter core.DeadLetterFilter) (int64, error) {
	var total int64
	if err := s.deadLetterQuery(ctx, filter).Count(&total).Error; err != nil {
		return 0, err
	}
	return total, nil
}

func (s *GormStorage) deadLetterQuery(ctx context.Context, filter core.DeadLetterFilter) *gorm.DB {
	q := s.db.WithContext(ctx).Model(&core.Job{}).
		Where("dead_lettered_at IS NOT NULL")
	if filter.Queue != "" {
		q = q.Where("queue = ?", filter.Queue)
	}
	if filter.Type != "" {
		q = q.Where("type = ?", filter.Type)
	}
	if filter.Tenant != "" {
		q = q.Where("tenant = ?", filter.Tenant)
	}
	q = applyMetaContains(s, q, filter.MetaContains)
	q = applyJobSearch(s, q, filter.Search)
	return q
}

func clampDeadLetterPagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = defaultDeadLetterLimit
	} else if limit > maxDeadLetterLimit {
		limit = maxDeadLetterLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
