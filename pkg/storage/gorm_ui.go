package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v3/ui/gen/jobs/v1"
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

	type pendingRow struct {
		Queue           string
		OldestPendingAt sql.NullString
	}
	var pendingRows []pendingRow
	if err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, MIN(created_at) AS oldest_pending_at").
		Where("status = ?", core.StatusPending).
		Group("queue").
		Find(&pendingRows).Error; err != nil {
		return nil, err
	}
	for _, r := range pendingRows {
		qs, ok := statsMap[r.Queue]
		if !ok {
			qs = &jobsv1.QueueStats{Name: r.Queue}
			statsMap[r.Queue] = qs
		}
		oldestPendingAt, ok := parseDBTimestamp(r.OldestPendingAt.String)
		if r.OldestPendingAt.Valid && ok {
			qs.OldestPendingAt = timestamppb.New(oldestPendingAt)
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

// parseDBTimestamp accepts the timestamp strings returned when aggregate
// expressions are scanned through sql.NullString: pgx/MySQL convertAssign use
// T-separated RFC3339, while SQLite returns a space-separated value with offset.
func parseDBTimestamp(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
	} {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

// CountActiveWorkers returns distinct workers currently holding running jobs.
func (s *GormStorage) CountActiveWorkers(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("status = ? AND locked_by <> ?", core.StatusRunning, "").
		Select("COUNT(DISTINCT locked_by)").
		Count(&count).Error
	return count, err
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
	if filter.Tenant != "" {
		q = q.Where("tenant = ?", filter.Tenant)
	}
	q = applyMetaContains(s, q, filter.MetaContains)
	q = applyJobSearch(s, q, filter.Search)
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

func applyMetaContains(s *GormStorage, q *gorm.DB, m *core.MetadataMap) *gorm.DB {
	if m == nil || len(*m) == 0 {
		return q
	}
	switch s.dialect() {
	case dialectPostgres:
		jsonBytes, err := json.Marshal(*m)
		if err != nil {
			_ = q.AddError(fmt.Errorf("marshal metadata contains: %w", err))
			return q
		}
		return q.Where("(NULLIF(metadata, '')::jsonb) @> ?::jsonb", string(jsonBytes))
	case dialectMySQL:
		declared := s.indexedMetadataKeyList()
		indexed := make(map[string]struct{}, len(declared))
		for _, key := range declared {
			if indexedMetadataKeyPattern.MatchString(key) {
				indexed[key] = struct{}{}
			}
		}
		// Route a declared key to its STORED generated column (meta_<key>, indexed)
		// when the filter value fits the VARCHAR(255) width; longer values would be
		// truncated in the column and could false-match, so they fall back to the
		// untruncated JSON_CONTAINS. This relies on metadata values being JSON
		// strings: core.Job.Metadata is map[string]string serialized via
		// serializer:json, so the gencol (JSON_VALUE ... RETURNING CHAR) and
		// JSON_CONTAINS agree. A non-string JSON value could only be injected via
		// raw out-of-band SQL, which is outside the library's contract.
		fallback := make(map[string]string, len(*m))
		for key, value := range *m {
			if _, ok := indexed[key]; ok && len(value) <= 255 {
				q = q.Where(mysqlMetadataGenColumn(key)+" = ?", value)
				continue
			}
			fallback[key] = value
		}
		if len(fallback) == 0 {
			return q
		}
		// JSON_CONTAINS is canonical containment (matches PG @>, unlike
		// substring LIKE which can false-match). MySQL has no general
		// JSON-containment index (PG GIN has no MySQL equivalent —
		// multi-valued indexes are per-array-path only) so this remains a
		// correct full scan, acceptable because metadata search is a
		// dashboard/admin cold path not the dequeue hot path. The metadata <>
		// empty-string guard prevents JSON_CONTAINS erroring on empty rows.
		jsonBytes, err := json.Marshal(fallback)
		if err != nil {
			_ = q.AddError(fmt.Errorf("marshal metadata contains: %w", err))
			return q
		}
		return q.Where("metadata IS NOT NULL AND metadata <> '' AND JSON_CONTAINS(metadata, CAST(? AS JSON))", string(jsonBytes))
	default:
		for key, value := range *m {
			pattern := `%` + escapeLikePattern(metadataPairFragment(key, value)) + `%`
			q = q.Where(metadataTextExpression(s)+" LIKE ? ESCAPE ?", pattern, `\`)
		}
		return q
	}
}

func mysqlMetadataGenColumn(key string) string {
	return "meta_" + key
}

func mysqlMetadataGenColumnDefinition(key string) string {
	column := mysqlMetadataGenColumn(key)
	return column + " VARCHAR(255) COLLATE utf8mb4_bin " +
		"GENERATED ALWAYS AS (JSON_VALUE(metadata, '$." + key + "' RETURNING CHAR(255) NULL ON ERROR)) STORED"
}

func metadataTextExpression(s *GormStorage) string {
	switch s.dialect() {
	case dialectPostgres:
		return "metadata::text"
	default:
		return "CAST(metadata AS CHAR)"
	}
}

// applyJobSearch applies the dashboard/dead-letter search term to a jobs query.
// Job IDs are stored as binary uuid columns (v3), so a substring LIKE against id
// no longer works on Postgres/MySQL and is meaningless for random UUIDv7s.
// Instead an exact id match is added when the term parses as a UUID, alongside a
// substring search over the args text (which still supports free-text lookups).
func applyJobSearch(s *GormStorage, q *gorm.DB, rawSearch string) *gorm.DB {
	if rawSearch == "" {
		return q
	}
	searchTerm := rawSearch
	if len(searchTerm) > maxUISearchLength {
		searchTerm = searchTerm[:maxUISearchLength]
	}
	argsLike := "%" + escapeLikePattern(searchTerm) + "%"
	if id, err := core.ParseUUID(strings.TrimSpace(searchTerm)); err == nil && id != core.NilUUID {
		return q.Where("id = ? OR "+argsTextExpression(s)+" LIKE ? ESCAPE ?", id, argsLike, `\`)
	}
	return q.Where(argsTextExpression(s)+" LIKE ? ESCAPE ?", argsLike, `\`)
}

func argsTextExpression(s *GormStorage) string {
	switch s.dialect() {
	case dialectPostgres:
		return "convert_from(args,'UTF8')"
	case dialectMySQL:
		return "CONVERT(args USING utf8mb4)"
	default:
		return "CAST(args AS TEXT)"
	}
}

func metadataPairFragment(key, value string) string {
	pair, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return fmt.Sprintf(`"%s":"%s"`, key, value)
	}
	return strings.TrimSuffix(strings.TrimPrefix(string(pair), "{"), "}")
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
func (s *GormStorage) RetryJob(ctx context.Context, jobID core.UUID) (*core.Job, error) {
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
func (s *GormStorage) DeleteJob(ctx context.Context, jobID core.UUID) error {
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
func (s *GormStorage) GetFanOutsByParents(ctx context.Context, parentJobIDs []core.UUID) ([]*core.FanOut, error) {
	if len(parentJobIDs) == 0 {
		return nil, nil
	}

	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id IN ?", parentJobIDs).
		Order("parent_job_id ASC, created_at ASC").
		Find(&fanOuts).Error
	if err != nil {
		return nil, err
	}
	if err := overlayLiveFanOutCountsBatch(s.db.WithContext(ctx), fanOuts); err != nil {
		return nil, err
	}
	return fanOuts, nil
}

// GetSubJobsByFanOuts retrieves sub-jobs for multiple fan-outs in one query.
func (s *GormStorage) GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []core.UUID) ([]*core.Job, error) {
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
