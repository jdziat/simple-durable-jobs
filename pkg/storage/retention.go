package storage

import (
	"context"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// retentionDeleteChunkSize bounds the literal IN-list of every DELETE the
// retention sweep issues, independently of the caller's batch size, so a large
// RetentionBatchSize can never exceed the driver's bind-parameter ceiling
// (SQLite ~32k, Postgres 65535). It matches PurgeJobs' purgeBatchSize.
const retentionDeleteChunkSize = 1000

// retentionTerminalIndexPredicate is the WHERE clause of the partial
// idx_jobs_retention_terminal, and the term DeleteTerminalJobsOlderThan repeats
// so the planner can see the index applies. Both read this one constant because
// a partial index and the query it was built for are the same decision written
// twice, and this repo has already shipped an index whose predicate no query
// matched.
//
// It is a hardcoded literal, NOT derived from core.TerminalJobStatuses, on
// purpose: it is the text of a shipped, versioned migration. Adding a fourth
// terminal status must not silently change what an already-applied migration
// says the index covers.
const retentionTerminalIndexPredicate = "status IN ('completed','failed','cancelled')"

// chunkIDs splits ids into consecutive slices of at most size elements, so a
// literal SQL IN-list built from one chunk stays within the driver's
// bind-parameter ceiling. The returned slices alias ids; callers must not retain
// them past the statement.
func chunkIDs[T any](ids []T, size int) [][]T {
	if size <= 0 || len(ids) <= size {
		return [][]T{ids}
	}
	chunks := make([][]T, 0, (len(ids)+size-1)/size)
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[start:end])
	}
	return chunks
}

func quotedTerminalJobStatuses() string {
	quoted := make([]string, 0, len(core.TerminalJobStatuses))
	for _, status := range core.TerminalJobStatuses {
		quoted = append(quoted, "'"+string(status)+"'")
	}
	return strings.Join(quoted, ",")
}

// DeleteTerminalJobsOlderThan deletes at most limit jobs in one terminal status
// whose terminal timestamp is older than age. It is an optional storage
// capability used by the worker through type assertion; core.Storage is
// intentionally unchanged.
func (s *GormStorage) DeleteTerminalJobsOlderThan(ctx context.Context, status core.JobStatus, age time.Duration, limit int) (int64, error) {
	if age <= 0 || limit <= 0 {
		return 0, nil
	}
	if status != core.StatusCompleted && status != core.StatusFailed && status != core.StatusCancelled {
		return 0, nil
	}

	var cutoff any
	if s.useDBClock() {
		cutoff = s.offsetExpr(-age)
	} else {
		cutoff = time.Now().Add(-age).UTC()
	}

	terminalStatuses := quotedTerminalJobStatuses()
	parentChildGuard := "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.parent_job_id = jobs.id AND c.status NOT IN (" + terminalStatuses + "))"
	rootChildGuard := "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.root_job_id = jobs.id AND c.status NOT IN (" + terminalStatuses + "))"
	if s.dialect() == dialectMySQL {
		parentChildGuard = "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.pending_parent_ref = jobs.id)"
		rootChildGuard = "NOT EXISTS (SELECT 1 FROM jobs c WHERE c.pending_root_ref = jobs.id)"
	}

	// Never GC a completed leaf sub-job whose owning fan-out's PARENT job is not
	// yet terminal. CollectResults (pkg/fanout) rebuilds its result slice from
	// surviving sub-job rows and is only guaranteed to have run once the parent
	// reaches a terminal status; deleting a completed child before then silently
	// turns succeeded work into ErrSubJobIncomplete with no top-level error.
	//
	// Guarding on the fan_out's own status is insufficient: the fan_out flips to
	// 'completed' the moment its last child finishes, which is BEFORE a stranded
	// (paused / backlogged / outage-delayed) parent resumes to collect. We must
	// therefore key on the parent job's terminal status, not the fan_out's. The
	// two-hop join walks jobs.fan_out_id -> fan_outs.id (PK) -> fan_outs
	// .parent_job_id -> jobs.id (PK); both hops are primary-key lookups evaluated
	// only for the already status/age-limited candidate rows, so no MySQL gencol
	// is required. It lives in the id-selection query only (never the DELETE), so
	// the MySQL "can't self-reference the delete target" rule is not triggered.
	fanOutParentGuard := "NOT EXISTS (SELECT 1 FROM fan_outs f JOIN jobs pp ON pp.id = f.parent_job_id " +
		"WHERE f.id = jobs.fan_out_id AND pp.status NOT IN (" + terminalStatuses + "))"

	// Never GC a job that a STILL-LIVE windowed dedup lock (IdempotencyKey /
	// UniqueFor) references. Those windows are documented to keep deduplicating
	// until their own expires_at, independently of how fast the job finished, and
	// an operator sets a 90-day idempotency TTL precisely so a replayed request
	// cannot charge a card twice. Retention windows are much shorter (the stock
	// completed window is 30 days; jobs.DefaultRetention() lowers it to 7), so
	// without this guard the sweep ended every idempotency window at the retention
	// horizon and the replay ran the guarded work a second time.
	//
	// Deleting only the job row and keeping the lock is NOT sufficient either: a
	// live lock whose referenced job row is gone is a broken invariant, and it
	// leaves Enqueue returning a dedup id that resolves to nothing. Releasing a
	// window is an explicit act of whoever deletes the job (DeleteJob / PurgeJobs /
	// Requeue's subtree replay all delete the lock with the row); automatic
	// retention must simply wait.
	//
	// The pin is bounded by the lock's OWN expires_at, not held forever: the moment
	// the window lapses this guard stops matching and the next pass collects the job
	// and its lock together. Worst case a job row outlives its retention window by
	// the TTL the operator chose. unique_locks.job_id is indexed
	// (idx_unique_locks_job_id, migration v34), so this is an index probe per
	// already status/age-limited candidate row. It lives in the id-SELECT only,
	// never in the DELETE, so MySQL's 1093 self-reference rule is not triggered.
	var lockNow any
	var liveUniqueLockGuard string
	var lockBinds []any
	if s.useDBClock() {
		lockNow = s.nowExpr()
		liveUniqueLockGuard = "NOT EXISTS (SELECT 1 FROM unique_locks ul WHERE ul.job_id = jobs.id AND ul.expires_at > ?)"
		lockBinds = []any{lockNow}
	} else {
		// SQLite stores timestamps as offset-suffixed TEXT and compares them
		// LEXICALLY, so `expires_at > ?` is only meaningful while the stored row
		// and the bound value carry the SAME trailing offset. Production writes
		// expires_at UTC-faced, but a row written on any other face — a legacy
		// row, a different tool, a direct Create — compares as garbage, and the
		// failure is silent and one-directional: the guard stops matching, the
		// job is collected, and a live idempotency window is destroyed. That is
		// precisely the double charge this guard exists to prevent, so it must
		// not depend on the writer's clock face.
		//
		// julianday() (not strftime, which is itself face-dependent here) parses
		// each value to a number, giving the same result on every face. Measured:
		// with the bare comparison, a live 1h window written local-faced failed to
		// pin its job on SQLite while pinning correctly on Postgres and MySQL.
		// Pinned by TestRetentionPinIsFaceIndependent.
		lockNow = time.Now().UTC()
		liveUniqueLockGuard = "NOT EXISTS (SELECT 1 FROM unique_locks ul WHERE ul.job_id = jobs.id " +
			"AND (CASE WHEN substr(ul.expires_at, -6) = substr(?, -6) THEN ul.expires_at > ? " +
			"ELSE julianday(ul.expires_at) > julianday(?) END))"
		lockBinds = []any{lockNow, lockNow, lockNow}
	}

	// The partial-index term, on the dialects that HAVE a partial index. See
	// retentionTerminalIndexPredicate: MySQL's idx_jobs_retention_terminal is a
	// plain (status, completed_at) index with no predicate to satisfy, so the extra
	// clause buys nothing there and is omitted rather than handed to an optimizer
	// it cannot help — the change is confined to the dialects it was measured on.
	partialIndexTerms := []string{retentionTerminalIndexPredicate}
	if s.dialect() == dialectMySQL {
		partialIndexTerms = nil
	}

	var deleted int64
	err := s.withSerializationRetry(ctx, func() error {
		deleted = 0
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var ids []core.UUID
			query := tx.Model(&core.Job{}).
				Where("status = ?", status)
			// Logically implied by `status = ?` above — status is validated to be one
			// of exactly these three at the top of this function — but a PARTIAL index
			// is only usable when the planner can see that its own WHERE clause is
			// satisfied, and SQLite matches partial-index predicates against the terms
			// actually present in the query rather than deriving them.
			// idx_jobs_retention_terminal is defined `WHERE status IN
			// ('completed','failed','cancelled') AND completed_at IS NOT NULL`, so
			// without this term the index built FOR this sweep can never serve it —
			// `INDEXED BY idx_jobs_retention_terminal` answers "no query solution" —
			// and the index is pure write carrying-cost.
			for _, term := range partialIndexTerms {
				query = query.Where(term)
			}
			query = query.
				Where("completed_at IS NOT NULL").
				Where("completed_at < ?", cutoff).
				Where(parentChildGuard).
				Where(rootChildGuard).
				Where("NOT EXISTS (SELECT 1 FROM fan_outs f WHERE f.parent_job_id = jobs.id AND f.status = 'pending')").
				Where(fanOutParentGuard).
				Where(liveUniqueLockGuard, lockBinds...).
				Order("completed_at ASC, id ASC").
				Limit(limit)
			query = s.lockForUpdate(query, true)
			if err := query.Pluck("id", &ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			// Delete by literal id list in bounded chunks. `limit` comes straight
			// from RetentionBatchSize, which an operator with a backlog is actively
			// encouraged to raise; an unchunked IN-list of that width blows past the
			// driver's bind-parameter ceiling (SQLite ~32k, Postgres 65535) and makes
			// EVERY pass fail with deleted=0, so the sweep dies silently exactly when
			// it is needed most. PurgeJobs bounds the same list the same way.
			for _, chunk := range chunkIDs(ids, retentionDeleteChunkSize) {
				if err := tx.Where("parent_job_id IN ?", chunk).Delete(&core.FanOut{}).Error; err != nil {
					return err
				}
				if err := tx.Where("job_id IN ?", chunk).Delete(&core.Checkpoint{}).Error; err != nil {
					return err
				}
				if err := tx.Where("job_id IN ?", chunk).Delete(&core.Signal{}).Error; err != nil {
					return err
				}
				// Only EXPIRED locks can reach here: liveUniqueLockGuard excluded
				// every job a live window still references.
				if err := tx.Where("job_id IN ?", chunk).Delete(&core.UniqueLock{}).Error; err != nil {
					return err
				}
				result := tx.Where("id IN ?", chunk).
					Where("status = ?", status).
					Where("completed_at IS NOT NULL").
					Where("completed_at < ?", cutoff).
					Delete(&core.Job{})
				if result.Error != nil {
					return result.Error
				}
				deleted += result.RowsAffected
			}
			return nil
		})
	})
	return deleted, err
}
