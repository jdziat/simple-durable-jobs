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

// deadLetterOrderColumn is the default DLQ sort column, and the reason
// Storage.Fail writes it through nowWriteValue rather than a bare time.Now().
//
// # WHY THE ORDER BY IS BARE AND THE WRITE SIDE CARRIES THE FIX
//
// On SQLite a timestamp is TEXT carrying its own offset, so ORDER BY over it is a
// LEXICAL compare — an ordering of WALL FACES, not of instants. The window
// predicate solves that per row with julianday() (see timeBoundPredicate), but an
// ORDER BY cannot borrow that trick: wrapping the column makes it a computed
// value and SQLite loses the index it was walking in order.
//
// Measured on the real migrated schema, 200k rows over ~139 days, 8 queues, a
// tenth dead-lettered, ANALYZEd, LIMIT 50, mean of 5 runs after 2 warm-ups:
//
//	                                bare ORDER BY   julianday() ORDER BY
//	DLQ, no queue filter                   295us              161.787ms
//	jobs list, queue = ?                   279us              154.606ms
//	jobs list, status = ?                  531us              258.767ms
//	jobs list, no filter                55.513ms               68.864ms
//
//	plan  DLQ            bare: SEARCH … idx_jobs_dead_lettered_at
//	                             | USE TEMP B-TREE FOR RIGHT PART OF ORDER BY
//	                     jd:   SEARCH … idx_jobs_dead_lettered_at
//	                             | USE TEMP B-TREE FOR ORDER BY   <- whole sort
//	plan  queue = ?      bare: SEARCH … idx_jobs_queue_created
//	                             | USE TEMP B-TREE FOR RIGHT PART OF ORDER BY
//	                     jd:   SEARCH … idx_jobs_queue_created
//	                             | USE TEMP B-TREE FOR ORDER BY   <- whole sort
//
// "RIGHT PART OF ORDER BY" is the index supplying the timestamp order and only
// the id tiebreak needing a sort; the normalized form sorts everything. 548x on
// the DLQ and 554x on the paged jobs list is not a trade worth making for a
// triage view, so the ORDER BY stays bare and the WRITE side supplies the single
// clock face instead — which is what run_at already does (normalizeRunAtZone),
// and what completed_at, written in the same terminal UPDATE, already did.
//
// # WHAT THIS DOES AND DOES NOT COVER
//
// Every row written by this version stores UTC on SQLite (the DB clock on
// Postgres/MySQL), so the bare compare is instant-correct across a mixed-zone
// fleet AND across a DST fall-back, both of which previously inverted it.
//
// Rows dead-lettered by an EARLIER version keep the local face they were written
// on and can still sort by wall face against each other and against new rows —
// by up to the writer's offset. They are not rewritten: a migration that rewrote
// stored timestamp text was tried in this repo and corrupted ordering on every
// SQLite database. Legacy rows drain with retention, and the window that selects
// them was already instant-correct.
//
// A caller-supplied SortKey routes to jobSortOrder, whose created_at terms are
// still face-ordered — see the residual documented there.
const deadLetterOrderColumn = "dead_lettered_at"

// deadLetterDefaultOrder is the ORDER BY ListDeadLettered uses when the caller
// asks for no explicit sort. It is a function rather than a literal at the call
// site so the plan guard (TestR29_JobsListOrderKeepsTheIndex) explains the SAME
// string production emits — asserting a plan for a clause a test built itself
// proves nothing about the shipped query.
func deadLetterDefaultOrder() string {
	return deadLetterOrderColumn + " DESC, id DESC"
}

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
	order := deadLetterDefaultOrder()
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
		Where(deadLetterOrderColumn + " IS NOT NULL")
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
	// The window bounds dead_lettered_at, the column this view is ordered by and
	// the one "what died in the last hour" is asking about. SearchJobs bounds
	// created_at; both go through the same instant-correct helper, which is what
	// has to be shared here — not the column. See core.DeadLetterFilter.
	q = applyTimeWindow(s, q, deadLetterOrderColumn, filter.DeadLetteredSince, filter.DeadLetteredUntil)
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
