package storage

import (
	"context"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// Dialect names returned by GormStorage.dialect.
const (
	dialectSQLite   = "sqlite"
	dialectPostgres = "postgres"
	dialectMySQL    = "mysql"
)

// dialect returns a normalized backend name: "sqlite", "postgres", "mysql", or
// the raw driver name for anything else. Used to gate dialect-specific SQL.
func (s *GormStorage) dialect() string {
	if s.isSQLite {
		return dialectSQLite
	}
	name := strings.ToLower(s.db.Name())
	switch {
	case strings.Contains(name, "postgres"):
		return dialectPostgres
	case strings.Contains(name, "mysql"):
		return dialectMySQL
	default:
		return name
	}
}

// useDBClock reports whether lock/lease timing should be computed from the
// database server clock rather than the caller's wall clock.
//
// Multi-worker backends (Postgres, MySQL) MUST use the DB clock: a job's
// locked_until is written by one worker and compared by another (the dequeuer
// and the stale-lock reaper), so any skew between their wall clocks can make a
// reaper reclaim a lock that is in fact still live, causing the job to run
// twice. Anchoring every timestamp and every comparison to the single DB clock
// removes that failure mode entirely.
//
// SQLite is single-process by construction (it is explicitly documented as
// dev/single-instance only), so there is exactly one clock and no skew to
// correct. It also stores time as TEXT in a format that does not compare
// correctly against SQLite's CURRENT_TIMESTAMP, so it deliberately keeps using
// the caller's time.Time values.
func (s *GormStorage) useDBClock() bool {
	return !s.isSQLite
}

// nowExpr returns a SQL expression evaluating to the database server's current
// timestamp. Only valid when useDBClock is true (Postgres/MySQL both support
// NOW()). For fractional precision on MySQL, NOW(6) is used.
func (s *GormStorage) nowExpr() clause.Expr {
	if s.dialect() == dialectMySQL {
		return gorm.Expr("NOW(6)")
	}
	return gorm.Expr("NOW()")
}

func (s *GormStorage) dequeueEligibleExpr() string {
	if s.dialect() == dialectMySQL {
		return "dq_eligible_at"
	}
	return "COALESCE(run_at, created_at)"
}

// dqReadyExpr returns the SQL boolean expression for "eligible to run now",
// used when a write transitions a job into status=pending. now must be the
// same clock expression the caller uses elsewhere (s.nowExpr() for DB-clock).
func (s *GormStorage) dqReadyExpr(now any) clause.Expr {
	return gorm.Expr("(run_at IS NULL OR run_at <= ?)", now)
}

// lockForUpdate applies a SELECT ... FOR UPDATE row lock, with SKIP LOCKED when
// skipLocked is true. SQLite has no row locks, so it is a no-op there.
func (s *GormStorage) lockForUpdate(q *gorm.DB, skipLocked bool) *gorm.DB {
	if s.isSQLite {
		return q
	}
	opts := ""
	if skipLocked {
		opts = "SKIP LOCKED"
	}
	return q.Clauses(clause.Locking{Strength: "UPDATE", Options: opts})
}

// claimableCandidates applies the shared dequeue candidate predicate and
// ordering. Callers add locking, limits, exclusions, and read shape.
func (s *GormStorage) claimableCandidates(q *gorm.DB, queues []string, now any) *gorm.DB {
	eligExpr := s.dequeueEligibleExpr()
	return q.
		Where("queue IN ?", queues).
		// Exclude paused queues in-SQL (always fresh, no extra round-trip). Use an
		// UNCORRELATED subquery, NOT a correlated `NOT EXISTS (... qs.queue =
		// jobs.queue ...)`: a correlated anti-join sits between the index scan and
		// the ORDER BY, defeating idx_jobs_dequeue_eligible's ordering and forcing a
		// Sort/filesort on the dequeue hot path (PG abandons the index on a large
		// backlog). The uncorrelated form evaluates the tiny paused set once and
		// preserves the ordered, LIMIT-bounded index scan. queue_states.queue is
		// NOT NULL (PK), so the NOT IN never hits the NULL-elimination trap.
		Where("queue NOT IN (SELECT queue FROM queue_states WHERE paused = ?)", true).
		Where("status = ?", core.StatusPending).
		Where("dq_ready = ?", true).
		Where(eligExpr+" <= ?", now).
		Where("(locked_until IS NULL OR locked_until < ?)", now).
		Order("priority DESC, " + eligExpr + " ASC")
}

// PromoteReadyJobs is the wedge-backstop for the dq_ready hint: it flips
// dq_ready=true for any pending job that has become eligible (run_at passed) but
// is still flagged not-ready. Returns the number of rows promoted. Safe to call
// frequently; idempotent.
func (s *GormStorage) PromoteReadyJobs(ctx context.Context) (int64, error) {
	var now any
	if s.useDBClock() {
		now = s.nowExpr()
	} else {
		now = time.Now()
	}
	silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})

	// Two-step capped pass, mirroring ReleaseStaleLocks: pluck up to
	// maxResumeBatch eligible-but-unready ids under FOR UPDATE SKIP LOCKED, then
	// UPDATE that frozen set. This avoids `UPDATE ... LIMIT` (unsupported on
	// Postgres) and bounds each tick to maxResumeBatch rows so a thundering herd
	// of simultaneously-eligible jobs cannot make every worker rewrite the whole
	// backlog in one statement. SKIP LOCKED lets concurrent promoters/dequeuers
	// partition the work. Any overflow is promoted on the next tick (or a dequeue
	// self-heal), which is safe because dq_ready is only a hint — Dequeue still
	// gates on dq_eligible_at <= now.
	var promoted []core.UUID
	err := s.withSerializationRetry(ctx, func() error {
		promoted = nil
		return silentDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			q := tx.Model(&core.Job{}).
				Where("status = ?", core.StatusPending).
				Where("dq_ready = ?", false).
				Where("(run_at IS NULL OR run_at <= ?)", now).
				Limit(maxResumeBatch)
			q = s.lockForUpdate(q, true)
			if err := q.Pluck("id", &promoted).Error; err != nil {
				return err
			}
			if len(promoted) == 0 {
				return nil
			}
			return tx.Model(&core.Job{}).
				Where("id IN ?", promoted).
				Where("status = ?", core.StatusPending).
				Where("dq_ready = ?", false).
				Updates(map[string]any{
					"dq_ready":   true,
					"updated_at": now,
				}).Error
		})
	})
	if err != nil {
		return 0, err
	}
	return int64(len(promoted)), nil
}

// offsetExpr returns a SQL expression for (DB now + d). A negative d yields a
// time in the past. Only valid when useDBClock is true.
func (s *GormStorage) offsetExpr(d time.Duration) clause.Expr {
	if s.dialect() == dialectMySQL {
		// INTERVAL ? MICROSECOND keeps sub-second precision; DATE_ADD accepts a
		// negative argument, so a single form covers past and future offsets.
		return gorm.Expr("DATE_ADD(NOW(6), INTERVAL ? MICROSECOND)", d.Microseconds())
	}
	// make_interval(secs => ...) accepts a fractional/negative double, so this
	// covers both lock extension (+d) and stale cutoffs (-d).
	return gorm.Expr("(NOW() + make_interval(secs => ?))", d.Seconds())
}
