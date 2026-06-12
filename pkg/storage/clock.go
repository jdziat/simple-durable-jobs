package storage

import (
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
