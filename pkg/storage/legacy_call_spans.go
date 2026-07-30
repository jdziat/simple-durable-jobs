package storage

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// LegacyCallSpanJob identifies a job whose Call checkpoints predate span
// tracking and therefore may be affected by the pre-v4.6 nested-call replay
// defect.
type LegacyCallSpanJob struct {
	JobID           core.UUID `gorm:"column:job_id"`
	JobType         string    `gorm:"column:job_type"`
	Status          string    `gorm:"column:status"`
	CallCheckpoints int       `gorm:"column:call_checkpoints"`
}

// FindLegacyCallSpanJobs returns non-terminal jobs carrying more than one Call
// checkpoint written before span tracking existed (span_end = 0).
//
// # WHY THIS EXISTS
//
// Before v4.6, Call indices came from a single flat counter shared with nested
// durable operations. On replay an outer Call was served from its checkpoint
// without re-invoking its handler, so indices consumed by anything nested inside
// it were never re-consumed and every later call read a checkpoint one or more
// slots too low — completing the workflow with another call's result, or raising
// a spurious determinism violation.
//
// The fix records the index span each call consumed, but it can only do so for
// checkpoints written after the upgrade. A checkpoint already on disk has
// span_end = 0 and deliberately degrades to the old behaviour, so work that was
// in flight across the upgrade stays exactly as (in)correct as it was. This
// query is how an operator finds that work. Requeue is the only path that clears
// checkpoints (see GormStorage.Requeue), so requeueing a listed job is the
// repair.
//
// The result is a conservative OVER-approximation: nothing persisted records
// whether a legacy call actually nested, so a purely flat workflow with two or
// more calls is also listed. Flagging safe work is cheap; missing corrupted work
// is not.
//
// Phase checkpoints (call_index < 0) are excluded — they are not Call
// checkpoints and never participate in index assignment.
//
// Built-in durable operations are excluded too, and that exclusion is
// load-bearing rather than tidiness. Only Call() records a SpanEnd, so a fan-out
// or signal-wait checkpoint carries span_end = 0 in EVERY version including this
// one. Without the exclusion this listed healthy current-version workflows as
// pre-upgrade suspects — and since the documented repair for a listed job is a
// checkpoint-clearing Requeue, acting on one discarded completed work and
// re-ran its side effects. The exclusion is built from the core.CheckpointType*
// constants the producers themselves write, so the two cannot drift.
//
// Note this is necessarily a POST-upgrade query: checkpoints.span_end is added by
// the v4.6 migration, so it cannot be run against a pre-upgrade database, which
// is exactly why current-version rows are guaranteed to be present when it runs.
func (s *GormStorage) FindLegacyCallSpanJobs(ctx context.Context, limit int) ([]LegacyCallSpanJob, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []LegacyCallSpanJob
	err := s.db.WithContext(ctx).Raw(`
		SELECT j.id AS job_id,
		       j.type AS job_type,
		       j.status AS status,
		       count(c.id) AS call_checkpoints
		FROM jobs j
		INNER JOIN checkpoints c ON c.job_id = j.id
		WHERE c.call_index >= 0
		  AND c.span_end = 0
		  AND c.call_type <> ?
		  AND c.call_type NOT LIKE ?
		  AND c.call_type NOT LIKE ?
		  AND j.status NOT IN (?, ?, ?)
		GROUP BY j.id, j.type, j.status
		HAVING count(c.id) > 1
		ORDER BY count(c.id) DESC
		LIMIT ?`,
		core.CheckpointTypeFanOut,
		core.CheckpointTypeSignalPrefix+"%",
		core.CheckpointTypeSignalTimeoutPrefix+"%",
		core.StatusCompleted, core.StatusCancelled, core.StatusFailed,
		limit,
	).Scan(&out).Error
	if err != nil {
		return nil, err
	}
	return out, nil
}
