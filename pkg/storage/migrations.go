package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// migrateMu serializes Migrate() within a single process. Even if several
// GormStorage instances share one connection pool, or one is called from
// multiple goroutines, only one migration runs at a time — so concurrent
// in-process callers can never starve each other of pool connections (the
// deadlock class a per-connection lock would otherwise create). Cross-process
// serialization is handled by the DB advisory lock in withMigrationLock.
var migrateMu sync.Mutex

// migrateMinConns is the connection floor Migrate needs: one for the fleet lock
// (held on a dedicated connection for the migration's duration) plus at least
// one for the work. gorm's AutoMigrate is multi-statement and takes a pool
// connection per statement (it does NOT stay on a single pinned connection — an
// attempt to run everything on the lock connection deadlocks), so the lock
// connection and the work connection must coexist.
const migrateMinConns = 2

// migrateAdvisoryLockKey is a fixed key for the Postgres session advisory lock
// that serializes Migrate() across a fleet. mysqlMigrateLockName is the
// equivalent MySQL GET_LOCK name. Both values are arbitrary but must be stable.
const (
	migrateAdvisoryLockKey int64  = 0x53444A4D4947 // "SDJMIG"
	mysqlMigrateLockName   string = "sdj_migrate"
	migrateLockTimeoutSecs        = 60
)

// withMigrationLock runs fn while holding a fleet-wide migration lock so that
// concurrent Migrate() calls — every worker runs Migrate() at startup — are
// serialized instead of racing on DDL (table/index/column creation and the
// ledger insert). Postgres uses a session advisory lock and MySQL a named
// GET_LOCK; both are cross-session mutexes, so the second worker blocks until
// the first finishes and then sees every table, index, and ledger row already
// present. SQLite is single-writer and needs no lock.
//
// The fleet lock is held on a DEDICATED connection while fn's work runs on the
// pool. Two things prevent the connection-starvation deadlock that a naive
// version of this had: (1) migrateMu ensures only one in-process caller runs at
// a time, so concurrent callers can't each park a lock-waiter connection; and
// (2) the pool is temporarily floored at migrateMinConns so the single caller
// always has its lock connection PLUS a work connection. (Running the work on
// the lock connection itself is not viable — gorm's AutoMigrate escapes a
// pinned connection and grabs its own.)
func (s *GormStorage) withMigrationLock(ctx context.Context, fn func(db *gorm.DB) error) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()

	db := s.db.WithContext(ctx)
	dialect := s.dialect()
	if s.isSQLite || (dialect != dialectPostgres && dialect != dialectMySQL) {
		return fn(db)
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}

	// Floor the pool so a pathologically small configured pool (e.g.
	// MaxOpenConns=1) still has room for the lock connection plus the work
	// connection. 0 means unlimited — leave it. Restore the user's setting after.
	if limit := sqlDB.Stats().MaxOpenConnections; limit > 0 && limit < migrateMinConns {
		sqlDB.SetMaxOpenConns(migrateMinConns)
		defer sqlDB.SetMaxOpenConns(limit)
	}

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Release on a non-cancelled context: a leaked session lock would block the
	// whole fleet until the connection is recycled.
	releaseCtx := context.WithoutCancel(ctx)
	switch dialect {
	case dialectPostgres:
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrateAdvisoryLockKey); err != nil {
			return fmt.Errorf("storage: acquire migration lock: %w", err)
		}
		defer func() { _, _ = conn.ExecContext(releaseCtx, "SELECT pg_advisory_unlock($1)", migrateAdvisoryLockKey) }()
	case dialectMySQL:
		var got sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", mysqlMigrateLockName, migrateLockTimeoutSecs).Scan(&got); err != nil {
			return fmt.Errorf("storage: acquire migration lock: %w", err)
		}
		if !got.Valid || got.Int64 != 1 {
			return fmt.Errorf("storage: timed out acquiring migration lock after %ds", migrateLockTimeoutSecs)
		}
		defer func() { _, _ = conn.ExecContext(releaseCtx, "SELECT RELEASE_LOCK(?)", mysqlMigrateLockName) }()
	}

	return fn(db)
}

// schemaMigration is one ordered schema change applied after AutoMigrate.
// Migrations run in ascending Version order; each is recorded in the
// schema_migrations ledger only after its body succeeds, so a version is
// applied at most once per database.
//
// CONCURRENCY: Migrate() is documented as safe to call from every worker at
// startup. The primary guard is the fleet-wide lock held by withMigrationLock
// for the whole of Migrate (a SESSION-level pg_advisory_lock / MySQL GET_LOCK
// held on a DEDICATED connection while the work runs on the pool — not a single
// transaction; each DDL statement auto-commits independently, and it is the held
// lock, not a transaction, that serializes the fleet). As a backstop, every
// migration body is also idempotent and tolerant of a racing worker (IF [NOT]
// EXISTS on PG/SQLite, benign-error tolerance on MySQL), and the ledger insert uses ON
// CONFLICT DO NOTHING.
type schemaMigration struct {
	Version int
	Name    string
	Up      func(ctx context.Context, db *gorm.DB, dialect string) error
}

// schemaMigrations is the ordered list of versioned migrations. Append new
// migrations with the next Version; never renumber or mutate a shipped entry,
// and keep every body idempotent (see the CONCURRENCY note above).
var schemaMigrations = []schemaMigration{
	{
		Version: 1,
		Name:    "rework_dequeue_index",
		Up:      migrateReworkDequeueIndex,
	},
	{
		Version: 2,
		Name:    "mysql_active_unique_key",
		Up:      migrateMySQLActiveUniqueKey,
	},
	{
		Version: 3,
		Name:    "concurrency_slots_live_index",
		Up:      migrateConcurrencySlotsLiveIndex,
	},
	{
		Version: 4,
		Name:    "rate_limit_windows",
		Up:      migrateRateLimitWindows,
	},
	{
		Version: 5,
		Name:    "retention_terminal_index",
		Up:      migrateRetentionTerminalIndex,
	},
}

// applyPendingMigrations runs every migration whose version is absent from the
// ledger, in order, recording each with an ON CONFLICT ledger insert so a
// racing worker that already recorded it is a no-op rather than a PK error.
// Called from Migrate while the fleet-wide migration lock is held (see
// withMigrationLock), so it is not racing other workers; the idempotent bodies
// and ON CONFLICT ledger insert remain as a backstop.
func (s *GormStorage) applyPendingMigrations(ctx context.Context, db *gorm.DB) error {
	var applied []core.SchemaMigration
	if err := db.Find(&applied).Error; err != nil {
		return fmt.Errorf("storage: read schema_migrations ledger: %w", err)
	}
	done := make(map[int]bool, len(applied))
	for _, m := range applied {
		done[m.Version] = true
	}

	dialect := s.dialect()
	for _, m := range schemaMigrations {
		if done[m.Version] {
			continue
		}
		if err := m.Up(ctx, db, dialect); err != nil {
			return fmt.Errorf("storage: migration %d (%s): %w", m.Version, m.Name, err)
		}
		// ON CONFLICT DO NOTHING: a concurrent worker may have recorded this
		// version already; that is success, not a duplicate-key error.
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).
			Create(&core.SchemaMigration{Version: m.Version, Name: m.Name}).Error; err != nil {
			return fmt.Errorf("storage: record migration %d (%s): %w", m.Version, m.Name, err)
		}
	}
	return nil
}

// isBenignDDLError reports whether a DDL error means "already applied by a
// concurrent worker" and can be ignored. Only MySQL needs this — Postgres and
// SQLite express the DDL with IF [NOT] EXISTS, which is itself race-tolerant
// (CREATE/DROP INDEX serialize on the table lock). MySQL 8 has no IF EXISTS for
// indexes, so a racing worker surfaces a duplicate/absent error we swallow.
func isBenignDDLError(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "Error 1061") || strings.Contains(m, "Duplicate key name") || // index already exists
		strings.Contains(m, "Error 1060") || strings.Contains(m, "Duplicate column name") || // column already exists
		strings.Contains(m, "Error 1091") || strings.Contains(m, "check that column/key exists") // dropping a missing index
}

// migrateReworkDequeueIndex replaces the original idx_jobs_dequeue, whose column
// order did not match the dequeue query's predicates, with one that does.
//
// Postgres/SQLite get a partial index over only pending rows; MySQL (no partial
// indexes) keeps a composite index led by the equality predicates. The bodies
// are idempotent so concurrent Migrate() calls cannot fail each other.
func migrateReworkDequeueIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		// MySQL has no IF [NOT] EXISTS for indexes; guard with the migrator and
		// swallow the benign duplicate/absent errors a concurrent worker causes.
		m := db.Migrator()
		if m.HasIndex(&core.Job{}, "idx_jobs_dequeue") {
			if err := m.DropIndex(&core.Job{}, "idx_jobs_dequeue"); err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop old idx_jobs_dequeue: %w", err)
			}
		}
		if err := db.Exec(
			"CREATE INDEX idx_jobs_dequeue ON jobs (status, queue, priority, created_at)",
		).Error; err != nil && !isBenignDDLError(err) {
			return err
		}
		return nil
	default: // sqlite, postgres — IF [NOT] EXISTS is race-tolerant
		if err := db.Exec("DROP INDEX IF EXISTS idx_jobs_dequeue").Error; err != nil {
			return fmt.Errorf("drop old idx_jobs_dequeue: %w", err)
		}
		return db.Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_dequeue ON jobs (queue, priority DESC, created_at ASC) WHERE status = 'pending'",
		).Error
	}
}

// migrateMySQLActiveUniqueKey gives MySQL the active-job uniqueness guarantee
// that Postgres/SQLite get from the idx_jobs_active_unique partial unique index,
// via a STORED generated column that is NULL once a job is terminal (MySQL
// allows duplicate NULLs in a unique index). Idempotent and race-tolerant.
func migrateMySQLActiveUniqueKey(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect != dialectMySQL {
		// sqlite/postgres already enforce this via the partial unique index
		// created in Migrate; nothing to do.
		return nil
	}
	m := db.Migrator()
	if !m.HasColumn(&core.Job{}, "active_unique_key") {
		if err := db.Exec(
			"ALTER TABLE jobs ADD COLUMN active_unique_key VARCHAR(255) " +
				"GENERATED ALWAYS AS (CASE WHEN status IN ('pending','running') AND unique_key <> '' " +
				"THEN unique_key ELSE NULL END) STORED",
		).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("add active_unique_key column: %w", err)
		}
	}
	if !m.HasIndex(&core.Job{}, "idx_jobs_active_unique") {
		if err := db.Exec(
			"CREATE UNIQUE INDEX idx_jobs_active_unique ON jobs (active_unique_key)",
		).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("create idx_jobs_active_unique: %w", err)
		}
	}
	return nil
}

func migrateConcurrencySlotsLiveIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasIndex(&core.ConcurrencySlot{}, "idx_concurrency_slots_live") {
			if err := db.Exec(
				"CREATE INDEX idx_concurrency_slots_live ON concurrency_slots (slot_name, expires_at)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_concurrency_slots_live: %w", err)
			}
		}
		return nil
	default:
		return db.Exec(
			"CREATE INDEX IF NOT EXISTS idx_concurrency_slots_live ON concurrency_slots (slot_name, expires_at)",
		).Error
	}
}

func migrateRateLimitWindows(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasIndex(&core.RateLimitWindow{}, "idx_rate_limit_windows_lookup") {
			if err := db.Exec(
				"CREATE INDEX idx_rate_limit_windows_lookup ON rate_limit_windows (limit_name, window_start)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_rate_limit_windows_lookup: %w", err)
			}
		}
		return nil
	default:
		return db.Exec(
			"CREATE INDEX IF NOT EXISTS idx_rate_limit_windows_lookup ON rate_limit_windows (limit_name, window_start)",
		).Error
	}
}

func migrateRetentionTerminalIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasIndex(&core.Job{}, "idx_jobs_retention_terminal") {
			if err := db.Exec(
				"CREATE INDEX idx_jobs_retention_terminal ON jobs (status, completed_at)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_retention_terminal: %w", err)
			}
		}
		return nil
	default:
		return db.Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_retention_terminal ON jobs (status, completed_at) WHERE status IN ('completed','failed','cancelled') AND completed_at IS NOT NULL",
		).Error
	}
}
