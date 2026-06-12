package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
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
	{
		Version: 6,
		Name:    "dead_letter_columns",
		Up:      migrateDeadLetterColumns,
	},
	{
		Version: 7,
		Name:    "scheduled_fire_last_fired_at",
		Up:      migrateScheduledFireLastFiredAt,
	},
	{
		Version: 8,
		Name:    "stale_lock_reaper_index",
		Up:      migrateStaleLockReaperIndex,
	},
	{
		Version: 9,
		Name:    "dequeue_order_index",
		Up:      migrateDequeueOrderIndex,
	},
	{
		Version: 10,
		Name:    "unique_locks",
		Up:      migrateUniqueLocks,
	},
	{
		Version: 11,
		Name:    "fix_dead_letter_index",
		Up:      migrateFixDeadLetterIndex,
	},
	{
		Version: 12,
		Name:    "drop_redundant_job_indexes",
		Up:      migrateDropRedundantJobIndexes,
	},
	{
		Version: 13,
		Name:    "dequeue_eligibility_index",
		Up:      migrateDequeueEligibilityIndex,
	},
	{
		Version: 14,
		Name:    "integrity_foreign_keys",
		Up:      migrateIntegrityForeignKeys,
	},
	{
		Version: 15,
		Name:    "dialect_correctness",
		Up:      migrateDialectCorrectness,
	},
	{
		Version: 16,
		Name:    "dlq_metadata_index",
		Up:      migrateDLQMetadataIndex,
	},
	{
		Version: 17,
		Name:    "mysql_dequeue_index_order",
		Up:      migrateMySQLDequeueIndexOrder,
	},
	{
		Version: 18,
		Name:    "deadletter_precision_align",
		Up:      migrateDeadLetterPrecisionAlign,
	},
	{
		Version: 19,
		Name:    "retention_workflow_indexes",
		Up:      migrateRetentionWorkflowIndexes,
	},
	{
		Version: 20,
		Name:    "scale_finish_indexes",
		Up:      migrateScaleFinishIndexes,
	},
	{
		Version: 21,
		Name:    "indexing_qc1",
		Up:      migrateIndexingQC1,
	},
	{
		Version: 22,
		Name:    "check_constraints",
		Up:      migrateCheckConstraints,
	},
	{
		Version: 23,
		Name:    "metadata_integrity",
		Up:      migrateMetadataIntegrity,
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
		strings.Contains(m, "Error 1091") || strings.Contains(m, "check that column/key exists") || // dropping a missing index
		strings.Contains(m, "Error 1826") || strings.Contains(m, "Duplicate foreign key constraint name") || // FK already exists
		strings.Contains(m, "Error 3822") || strings.Contains(m, "Duplicate check constraint name") // CHECK already exists
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

func migrateDeadLetterColumns(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasColumn(&core.Job{}, "dead_lettered_at") {
			if err := db.Exec("ALTER TABLE jobs ADD COLUMN dead_lettered_at DATETIME(6) NULL").Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add dead_lettered_at column: %w", err)
			}
		}
		if !m.HasColumn(&core.Job{}, "dead_letter_reason") {
			if err := db.Exec("ALTER TABLE jobs ADD COLUMN dead_letter_reason TEXT NULL").Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add dead_letter_reason column: %w", err)
			}
		}
		if !m.HasIndex(&core.Job{}, "idx_jobs_dead_lettered_at") {
			if err := db.Exec("CREATE INDEX idx_jobs_dead_lettered_at ON jobs (dead_lettered_at)").Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_dead_lettered_at: %w", err)
			}
		}
		return nil
	case dialectPostgres:
		if err := db.Exec("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dead_lettered_at timestamp with time zone NULL").Error; err != nil {
			return fmt.Errorf("add dead_lettered_at column: %w", err)
		}
		if err := db.Exec("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dead_letter_reason text NULL").Error; err != nil {
			return fmt.Errorf("add dead_letter_reason column: %w", err)
		}
		return db.Exec("CREATE INDEX IF NOT EXISTS idx_jobs_dead_lettered_at ON jobs (dead_lettered_at) WHERE dead_lettered_at IS NOT NULL").Error
	default:
		m := db.Migrator()
		if !m.HasColumn(&core.Job{}, "dead_lettered_at") {
			if err := db.Exec("ALTER TABLE jobs ADD COLUMN dead_lettered_at datetime NULL").Error; err != nil {
				return fmt.Errorf("add dead_lettered_at column: %w", err)
			}
		}
		if !m.HasColumn(&core.Job{}, "dead_letter_reason") {
			if err := db.Exec("ALTER TABLE jobs ADD COLUMN dead_letter_reason text NULL").Error; err != nil {
				return fmt.Errorf("add dead_letter_reason column: %w", err)
			}
		}
		return db.Exec("CREATE INDEX IF NOT EXISTS idx_jobs_dead_lettered_at ON jobs (dead_lettered_at) WHERE dead_lettered_at IS NOT NULL").Error
	}
}

func migrateScheduledFireLastFiredAt(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasColumn(&core.ScheduledFire{}, "last_fired_at") {
			if err := db.Exec("ALTER TABLE scheduled_fires ADD COLUMN last_fired_at DATETIME(6) NULL").Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add last_fired_at column: %w", err)
			}
		}
		return nil
	case dialectPostgres:
		return db.Exec("ALTER TABLE scheduled_fires ADD COLUMN IF NOT EXISTS last_fired_at timestamp with time zone NULL").Error
	default:
		m := db.Migrator()
		if !m.HasColumn(&core.ScheduledFire{}, "last_fired_at") {
			if err := db.Exec("ALTER TABLE scheduled_fires ADD COLUMN last_fired_at datetime NULL").Error; err != nil {
				return fmt.Errorf("add last_fired_at column: %w", err)
			}
		}
		return nil
	}
}

// migrateStaleLockReaperIndex adds an index supporting the reaper's new
// freshness predicate: ReleaseStaleLocks now selects running rows where
// COALESCE(last_heartbeat_at, started_at, locked_until) < cutoff. No column is
// added — last_heartbeat_at/started_at/locked_until already exist on core.Job
// and are created by AutoMigrate; this migration is index-only.
//
// Postgres/SQLite support partial + expression indexes, so they index the
// COALESCE expression over only running rows. MySQL has neither partial nor a
// version-stable functional index, so it gets a status-led composite index
// instead. The index is a pure optimization (running rows are few); correctness
// never depends on it.
func migrateStaleLockReaperIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasIndex(&core.Job{}, "idx_jobs_stale_lock") {
			if err := db.Exec(
				"CREATE INDEX idx_jobs_stale_lock ON jobs (status, last_heartbeat_at, started_at, locked_until)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_stale_lock: %w", err)
			}
		}
		return nil
	default:
		return db.Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_stale_lock ON jobs (COALESCE(last_heartbeat_at, started_at, locked_until)) WHERE status = 'running'",
		).Error
	}
}

// migrateDequeueOrderIndex adds an order-first dequeue index for the
// multi-queue query shape:
//
//	status='pending' AND queue IN (...) ORDER BY priority DESC, created_at ASC
//
// The v1 idx_jobs_dequeue remains in place for queue-filtered scans, but live
// EXPLAIN showed multi-queue dequeue fell back to a full scan + filesort on
// MySQL and a status/queue scan + Sort on Postgres. This index lets the planner
// walk pending jobs directly in global priority order and filter the queue from
// the index, avoiding the sort for the LIMIT/SKIP LOCKED hot path.
func migrateDequeueOrderIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasIndex(&core.Job{}, "idx_jobs_dequeue_order") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_jobs_dequeue_order ON jobs (status, priority DESC, created_at ASC, queue)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_dequeue_order: %w", err)
			}
		}
		return nil
	default:
		return db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_dequeue_order ON jobs (priority DESC, created_at ASC, queue) WHERE status = 'pending'",
		).Error
	}
}

func migrateUniqueLocks(ctx context.Context, db *gorm.DB, dialect string) error {
	m := db.Migrator()
	if !m.HasTable(&core.UniqueLock{}) {
		switch dialect {
		case dialectMySQL:
			if err := db.WithContext(ctx).Exec(`
				CREATE TABLE unique_locks (
					scope_hash VARCHAR(64) NOT NULL,
					job_id VARCHAR(36) NOT NULL,
					expires_at DATETIME(6) NOT NULL,
					created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
					PRIMARY KEY (scope_hash)
				)
			`).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create unique_locks table: %w", err)
			}
		case dialectPostgres:
			if err := db.WithContext(ctx).Exec(`
				CREATE TABLE IF NOT EXISTS unique_locks (
					scope_hash VARCHAR(64) PRIMARY KEY,
					job_id VARCHAR(36) NOT NULL,
					expires_at timestamp with time zone NOT NULL,
					created_at timestamp with time zone NOT NULL DEFAULT NOW()
				)
			`).Error; err != nil {
				return fmt.Errorf("create unique_locks table: %w", err)
			}
		default:
			if err := db.WithContext(ctx).Exec(`
				CREATE TABLE IF NOT EXISTS unique_locks (
					scope_hash VARCHAR(64) PRIMARY KEY,
					job_id VARCHAR(36) NOT NULL,
					expires_at datetime NOT NULL,
					created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
				)
			`).Error; err != nil {
				return fmt.Errorf("create unique_locks table: %w", err)
			}
		}
	}

	switch dialect {
	case dialectMySQL:
		if !m.HasIndex(&core.UniqueLock{}, "idx_unique_locks_expires_at") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_unique_locks_expires_at ON unique_locks (expires_at)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_unique_locks_expires_at: %w", err)
			}
		}
		return nil
	default:
		return db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_unique_locks_expires_at ON unique_locks (expires_at)",
		).Error
	}
}

func migrateFixDeadLetterIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		var precision sql.NullInt64
		if err := db.WithContext(ctx).Raw(`
			SELECT DATETIME_PRECISION
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE()
			  AND TABLE_NAME = 'jobs'
			  AND COLUMN_NAME = 'dead_lettered_at'
		`).Scan(&precision).Error; err != nil {
			return fmt.Errorf("read dead_lettered_at precision: %w", err)
		}
		if precision.Valid && precision.Int64 == 6 {
			return nil
		}
		if err := db.WithContext(ctx).Exec(
			"ALTER TABLE jobs MODIFY COLUMN dead_lettered_at DATETIME(6) NULL",
		).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("modify dead_lettered_at precision: %w", err)
		}
		return nil
	case dialectPostgres:
		if err := db.WithContext(ctx).Exec("ALTER TABLE jobs ALTER COLUMN dead_lettered_at TYPE timestamp with time zone").Error; err != nil {
			return fmt.Errorf("normalize dead_lettered_at type: %w", err)
		}
		if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS idx_jobs_dead_lettered_at").Error; err != nil {
			return fmt.Errorf("drop idx_jobs_dead_lettered_at: %w", err)
		}
		return db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_dead_lettered_at ON jobs (dead_lettered_at) WHERE dead_lettered_at IS NOT NULL",
		).Error
	default:
		if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS idx_jobs_dead_lettered_at").Error; err != nil {
			return fmt.Errorf("drop idx_jobs_dead_lettered_at: %w", err)
		}
		return db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_dead_lettered_at ON jobs (dead_lettered_at) WHERE dead_lettered_at IS NOT NULL",
		).Error
	}
}

func migrateDropRedundantJobIndexes(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		for _, indexName := range []string{
			"idx_jobs_priority",
			"idx_jobs_queue",
			"idx_jobs_locked_until",
			"idx_jobs_dequeue",
		} {
			if m.HasIndex(&core.Job{}, indexName) {
				if err := m.DropIndex(&core.Job{}, indexName); err != nil && !isBenignDDLError(err) {
					return fmt.Errorf("drop %s: %w", indexName, err)
				}
			}
		}
		if !m.HasIndex(&core.Job{}, "idx_jobs_unique_key") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_jobs_unique_key ON jobs (unique_key)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_unique_key: %w", err)
			}
		}
		return nil
	default:
		for _, indexName := range []string{
			"idx_jobs_priority",
			"idx_jobs_queue",
			"idx_jobs_locked_until",
			"idx_jobs_dequeue",
			"idx_jobs_unique_key",
		} {
			if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS " + indexName).Error; err != nil {
				return fmt.Errorf("drop %s: %w", indexName, err)
			}
		}
		return nil
	}
}

func migrateDequeueEligibilityIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasColumn(&core.Job{}, "dq_eligible_at") {
			if err := db.WithContext(ctx).Exec(
				"ALTER TABLE jobs ADD COLUMN dq_eligible_at datetime(6) " +
					"GENERATED ALWAYS AS (IF(status = 'pending', COALESCE(run_at, created_at), NULL)) STORED",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add dq_eligible_at column: %w", err)
			}
		}
		if !m.HasIndex(&core.Job{}, "idx_jobs_dequeue_eligible") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_jobs_dequeue_eligible ON jobs (dq_eligible_at, priority, queue)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_dequeue_eligible: %w", err)
			}
		}
		if m.HasIndex(&core.Job{}, "idx_jobs_dequeue_order") {
			if err := m.DropIndex(&core.Job{}, "idx_jobs_dequeue_order"); err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop idx_jobs_dequeue_order: %w", err)
			}
		}
		return nil
	default:
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_dequeue_eligible ON jobs (priority DESC, (COALESCE(run_at, created_at)), queue) WHERE status = 'pending'",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_dequeue_eligible: %w", err)
		}
		if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS idx_jobs_dequeue_order").Error; err != nil {
			return fmt.Errorf("drop idx_jobs_dequeue_order: %w", err)
		}
		return nil
	}
}

func migrateMySQLDequeueIndexOrder(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect != dialectMySQL {
		return nil
	}

	var leadingColumn sql.NullString
	if err := db.WithContext(ctx).Raw(`
		SELECT COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND INDEX_NAME = 'idx_jobs_dequeue_eligible'
		  AND SEQ_IN_INDEX = 1
	`).Scan(&leadingColumn).Error; err != nil {
		return fmt.Errorf("read idx_jobs_dequeue_eligible leading column: %w", err)
	}
	if leadingColumn.Valid && strings.EqualFold(leadingColumn.String, "status") {
		return nil
	}

	m := db.Migrator()
	if m.HasIndex(&core.Job{}, "idx_jobs_dequeue_eligible") {
		if err := m.DropIndex(&core.Job{}, "idx_jobs_dequeue_eligible"); err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("drop idx_jobs_dequeue_eligible: %w", err)
		}
	}
	if err := db.WithContext(ctx).Exec(
		"CREATE INDEX idx_jobs_dequeue_eligible ON jobs (status, priority DESC, dq_eligible_at, queue)",
	).Error; err != nil && !isBenignDDLError(err) {
		return fmt.Errorf("create idx_jobs_dequeue_eligible: %w", err)
	}
	return nil
}

func migrateDeadLetterPrecisionAlign(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect != dialectMySQL {
		return nil
	}

	var precision sql.NullInt64
	if err := db.WithContext(ctx).Raw(`
		SELECT DATETIME_PRECISION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = 'dead_lettered_at'
	`).Scan(&precision).Error; err != nil {
		return fmt.Errorf("read dead_lettered_at precision: %w", err)
	}
	if precision.Valid && precision.Int64 == 3 {
		return nil
	}

	if err := db.WithContext(ctx).Exec(
		"ALTER TABLE jobs MODIFY dead_lettered_at datetime(3) NULL",
	).Error; err != nil && !isBenignDDLError(err) {
		return fmt.Errorf("modify dead_lettered_at precision: %w", err)
	}
	return nil
}

func migrateRetentionWorkflowIndexes(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasColumn(&core.Job{}, "pending_parent_ref") {
			if err := db.WithContext(ctx).Exec(
				"ALTER TABLE jobs ADD COLUMN pending_parent_ref varchar(36) " +
					"GENERATED ALWAYS AS (CASE WHEN status NOT IN ('completed','failed','cancelled') " +
					"THEN parent_job_id END) STORED",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add pending_parent_ref column: %w", err)
			}
		}
		if !m.HasColumn(&core.Job{}, "pending_root_ref") {
			if err := db.WithContext(ctx).Exec(
				"ALTER TABLE jobs ADD COLUMN pending_root_ref varchar(36) " +
					"GENERATED ALWAYS AS (CASE WHEN status NOT IN ('completed','failed','cancelled') " +
					"THEN root_job_id END) STORED",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add pending_root_ref column: %w", err)
			}
		}
		if !m.HasIndex(&core.Job{}, "idx_jobs_parent_nonterminal") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_jobs_parent_nonterminal ON jobs (pending_parent_ref)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_parent_nonterminal: %w", err)
			}
		}
		if !m.HasIndex(&core.Job{}, "idx_jobs_root_nonterminal") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_jobs_root_nonterminal ON jobs (pending_root_ref)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_root_nonterminal: %w", err)
			}
		}
		return nil
	default:
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_parent_nonterminal ON jobs (parent_job_id) WHERE status NOT IN ('completed','failed','cancelled')",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_parent_nonterminal: %w", err)
		}
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_root_nonterminal ON jobs (root_job_id) WHERE status NOT IN ('completed','failed','cancelled')",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_root_nonterminal: %w", err)
		}
		return nil
	}
}

func migrateScaleFinishIndexes(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		if m.HasIndex(&core.Job{}, "idx_jobs_status") {
			if err := m.DropIndex(&core.Job{}, "idx_jobs_status"); err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop idx_jobs_status: %w", err)
			}
		}
		if !m.HasIndex(&core.Signal{}, "idx_signals_consumed_at") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_signals_consumed_at ON signals (consumed_at, id)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_signals_consumed_at: %w", err)
			}
		}
		if m.HasIndex(&core.Job{}, "idx_jobs_retention_terminal") {
			if err := m.DropIndex(&core.Job{}, "idx_jobs_retention_terminal"); err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop idx_jobs_retention_terminal: %w", err)
			}
		}
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX idx_jobs_retention_terminal ON jobs (status, completed_at, id)",
		).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("create idx_jobs_retention_terminal: %w", err)
		}
		return nil
	default:
		if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS idx_jobs_status").Error; err != nil {
			return fmt.Errorf("drop idx_jobs_status: %w", err)
		}
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_signals_consumed_at ON signals (consumed_at, id) WHERE consumed_at IS NOT NULL",
		).Error; err != nil {
			return fmt.Errorf("create idx_signals_consumed_at: %w", err)
		}
		if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS idx_jobs_retention_terminal").Error; err != nil {
			return fmt.Errorf("drop idx_jobs_retention_terminal: %w", err)
		}
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_retention_terminal ON jobs (status, completed_at, id) WHERE status IN ('completed','failed','cancelled') AND completed_at IS NOT NULL",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_retention_terminal: %w", err)
		}
		return nil
	}
}

func migrateIndexingQC1(ctx context.Context, db *gorm.DB, dialect string) error {
	// Keep idx_concurrency_slots_expires_at: DeleteExpiredConcurrencySlots is a
	// global expires_at sweep, and idx_concurrency_slots_live leads slot_name.
	switch dialect {
	case dialectMySQL:
		m := db.Migrator()
		for _, indexName := range []string{
			"idx_jobs_run_at",
			"idx_jobs_fan_out_id",
		} {
			if m.HasIndex(&core.Job{}, indexName) {
				if err := m.DropIndex(&core.Job{}, indexName); err != nil && !isBenignDDLError(err) {
					return fmt.Errorf("drop %s: %w", indexName, err)
				}
			}
		}
		if m.HasIndex(&core.RateLimitWindow{}, "idx_rate_limit_windows_lookup") {
			if err := m.DropIndex(&core.RateLimitWindow{}, "idx_rate_limit_windows_lookup"); err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop idx_rate_limit_windows_lookup: %w", err)
			}
		}
		for _, spec := range []struct {
			name string
			sql  string
		}{
			{
				name: "idx_jobs_status_created",
				sql:  "CREATE INDEX idx_jobs_status_created ON jobs (status, created_at DESC)",
			},
			{
				name: "idx_jobs_queue_created",
				sql:  "CREATE INDEX idx_jobs_queue_created ON jobs (queue, created_at DESC)",
			},
			{
				name: "idx_jobs_fan_out_status",
				sql:  "CREATE INDEX idx_jobs_fan_out_status ON jobs (fan_out_id, status)",
			},
		} {
			if !m.HasIndex(&core.Job{}, spec.name) {
				if err := db.WithContext(ctx).Exec(spec.sql).Error; err != nil && !isBenignDDLError(err) {
					return fmt.Errorf("create %s: %w", spec.name, err)
				}
			}
		}
		// R19 parity: MySQL's v8 idx_jobs_stale_lock is a plain composite
		// (status, last_heartbeat_at, started_at, locked_until), so the reaper's
		// ORDER BY COALESCE(last_heartbeat_at, started_at, locked_until) cannot
		// ride it and filesorts the whole running set every tick. Rebuild it as
		// an expression index (status, (COALESCE(...))) so MySQL gets the same
		// index-ordered range scan PG/SQLite get from their functional/partial
		// idx_jobs_stale_lock. Functional key parts need MySQL 8.0.13+.
		if m.HasIndex(&core.Job{}, "idx_jobs_stale_lock") {
			if err := m.DropIndex(&core.Job{}, "idx_jobs_stale_lock"); err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop idx_jobs_stale_lock: %w", err)
			}
		}
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX idx_jobs_stale_lock ON jobs (status, ((COALESCE(last_heartbeat_at, started_at, locked_until))))",
		).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("create idx_jobs_stale_lock expression index: %w", err)
		}
		return nil
	default:
		for _, indexName := range []string{
			"idx_jobs_run_at",
			"idx_jobs_fan_out_id",
			"idx_rate_limit_windows_lookup",
		} {
			if err := db.WithContext(ctx).Exec("DROP INDEX IF EXISTS " + indexName).Error; err != nil {
				return fmt.Errorf("drop %s: %w", indexName, err)
			}
		}
		for _, spec := range []struct {
			name string
			sql  string
		}{
			{
				name: "idx_jobs_status_created",
				sql:  "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at DESC)",
			},
			{
				name: "idx_jobs_queue_created",
				sql:  "CREATE INDEX IF NOT EXISTS idx_jobs_queue_created ON jobs (queue, created_at DESC)",
			},
			{
				name: "idx_jobs_fan_out_status",
				sql:  "CREATE INDEX IF NOT EXISTS idx_jobs_fan_out_status ON jobs (fan_out_id, status)",
			},
		} {
			if err := db.WithContext(ctx).Exec(spec.sql).Error; err != nil {
				return fmt.Errorf("create %s: %w", spec.name, err)
			}
		}
		return nil
	}
}

// migrateCheckConstraints is migration-only: no gorm check tags, so AutoMigrate
// never creates a dialect-mismatched constraint or flaps these on startup.
// Postgres adds each CHECK as NOT VALID and then validates it; MySQL applies
// utf8mb4_0900_as_cs COLLATE on string-enum operands so enum checks are
// case-sensitive (R05x). SQLite is a no-op because ALTER TABLE ADD CHECK is not
// supported there.
func migrateCheckConstraints(ctx context.Context, db *gorm.DB, dialect string) error {
	type checkConstraint struct {
		model     any
		table     string
		name      string
		pgExpr    string
		mysqlExpr string
	}

	const jobStatuses = "'pending','running','completed','failed','retrying','waiting','cancelled','paused'"
	const fanOutStrategies = "'fail_fast','collect_all','threshold'"
	const fanOutStatuses = "'pending','completed','failed'"

	constraints := []checkConstraint{
		{
			model:     &core.Job{},
			table:     "jobs",
			name:      "chk_jobs_status",
			pgExpr:    "status IN (" + jobStatuses + ")",
			mysqlExpr: "status COLLATE utf8mb4_0900_as_cs IN (" + jobStatuses + ")",
		},
		{
			model:     &core.Job{},
			table:     "jobs",
			name:      "chk_jobs_attempt_nonneg",
			pgExpr:    "attempt >= 0",
			mysqlExpr: "attempt >= 0",
		},
		{
			model:     &core.Job{},
			table:     "jobs",
			name:      "chk_jobs_max_retries_nonneg",
			pgExpr:    "max_retries >= 0",
			mysqlExpr: "max_retries >= 0",
		},
		{
			model:     &core.FanOut{},
			table:     "fan_outs",
			name:      "chk_fan_outs_strategy",
			pgExpr:    "strategy IN (" + fanOutStrategies + ")",
			mysqlExpr: "strategy COLLATE utf8mb4_0900_as_cs IN (" + fanOutStrategies + ")",
		},
		{
			model:     &core.FanOut{},
			table:     "fan_outs",
			name:      "chk_fan_outs_status",
			pgExpr:    "status IN (" + fanOutStatuses + ")",
			mysqlExpr: "status COLLATE utf8mb4_0900_as_cs IN (" + fanOutStatuses + ")",
		},
		{
			model:     &core.FanOut{},
			table:     "fan_outs",
			name:      "chk_fan_outs_counts_nonneg",
			pgExpr:    "total_count >= 0 AND completed_count >= 0 AND failed_count >= 0 AND cancelled_count >= 0",
			mysqlExpr: "total_count >= 0 AND completed_count >= 0 AND failed_count >= 0 AND cancelled_count >= 0",
		},
	}

	m := db.Migrator()
	switch dialect {
	case dialectPostgres:
		for _, constraint := range constraints {
			if m.HasConstraint(constraint.model, constraint.name) {
				continue
			}
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID", constraint.table, constraint.name, constraint.pgExpr),
			).Error; err != nil {
				return fmt.Errorf("add %s: %w", constraint.name, err)
			}
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", constraint.table, constraint.name),
			).Error; err != nil {
				return fmt.Errorf("validate %s: %w", constraint.name, err)
			}
		}
		return nil
	case dialectMySQL:
		for _, constraint := range constraints {
			if m.HasConstraint(constraint.model, constraint.name) {
				continue
			}
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)", constraint.table, constraint.name, constraint.mysqlExpr),
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add %s: %w", constraint.name, err)
			}
		}
		return nil
	default:
		// CHECK enforcement is on the production engines (Postgres/MySQL);
		// SQLite dev tables rely on the Go enum types.
		return nil
	}
}

// migrateMetadataIntegrity is migration-only: no gorm check tag, so
// AutoMigrate never creates or flaps it. The OR guard accepts NULL, empty
// string, and valid JSON, and rejects only non-empty invalid JSON, which the
// json serializer never produces.
func migrateMetadataIntegrity(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		const (
			name = "chk_jobs_metadata_json"
			expr = "metadata IS NULL OR metadata = '' OR JSON_VALID(metadata)"
		)
		m := db.Migrator()
		if m.HasConstraint(&core.Job{}, name) {
			return nil
		}
		if err := db.WithContext(ctx).Exec(
			fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)", "jobs", name, expr),
		).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("add %s: %w", name, err)
		}
		return nil
	case dialectPostgres:
		// PG already validates metadata via the idx_jobs_metadata_gin expression
		// index ((NULLIF(metadata,'')::jsonb)), so a CHECK would be redundant.
		return nil
	default:
		// SQLite is dev-only and ALTER TABLE ADD CHECK is unsupported there.
		return nil
	}
}

func migrateIntegrityForeignKeys(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectPostgres, dialectMySQL:
		// Continue below.
	default:
		// SQLite test/prod DSNs have foreign_keys=OFF, and adding a FK to an
		// existing SQLite table requires a full table rebuild. SQLite integrity
		// rests on the app-level workflow-aware retention instead.
		return nil
	}

	for _, stmt := range []string{
		"DELETE FROM checkpoints WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = checkpoints.job_id)",
		"DELETE FROM signals WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = signals.job_id)",
		"DELETE FROM fan_outs WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = fan_outs.parent_job_id)",
	} {
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return fmt.Errorf("pre-clean integrity orphans: %w", err)
		}
	}

	type foreignKey struct {
		model any
		table string
		name  string
		col   string
	}
	const jobsRef = "REFERENCES jobs(id) ON DELETE CASCADE"
	keys := []foreignKey{
		{model: &core.Checkpoint{}, table: "checkpoints", name: "fk_checkpoints_job", col: "job_id"},
		{model: &core.Signal{}, table: "signals", name: "fk_signals_job", col: "job_id"},
		{model: &core.FanOut{}, table: "fan_outs", name: "fk_fanouts_parent", col: "parent_job_id"},
	}

	// S16: these VALIDATE / FK-add statements run under the fleet migration
	// lock. On huge populated installs operators should run them out-of-band.
	m := db.Migrator()
	for _, key := range keys {
		if m.HasConstraint(key.model, key.name) {
			continue
		}
		switch dialect {
		case dialectPostgres:
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) %s NOT VALID", key.table, key.name, key.col, jobsRef),
			).Error; err != nil {
				return fmt.Errorf("add %s: %w", key.name, err)
			}
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", key.table, key.name),
			).Error; err != nil {
				return fmt.Errorf("validate %s: %w", key.name, err)
			}
		case dialectMySQL:
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) %s", key.table, key.name, key.col, jobsRef),
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add %s: %w", key.name, err)
			}
		}
	}
	return nil
}

func migrateDialectCorrectness(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectMySQL:
		return migrateMySQLDialectCorrectness(ctx, db)
	case dialectPostgres:
		return migratePostgresDialectCorrectness(ctx, db)
	default:
		return nil
	}
}

func migrateDLQMetadataIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	switch dialect {
	case dialectPostgres:
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_metadata_gin ON jobs USING GIN ((NULLIF(metadata, '')::jsonb) jsonb_path_ops)",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_metadata_gin: %w", err)
		}
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs (tenant)",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_tenant: %w", err)
		}
		return nil
	case dialectMySQL:
		m := db.Migrator()
		if !m.HasIndex(&core.Job{}, "idx_jobs_tenant") {
			if err := db.WithContext(ctx).Exec(
				"CREATE INDEX idx_jobs_tenant ON jobs (tenant)",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create idx_jobs_tenant: %w", err)
			}
		}
		return nil
	default:
		if err := db.WithContext(ctx).Exec(
			"CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs (tenant)",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_tenant: %w", err)
		}
		return nil
	}
}

func migrateMySQLDialectCorrectness(ctx context.Context, db *gorm.DB) error {
	const collation = "utf8mb4_0900_as_cs"

	uniqueKeyCollation, err := mysqlColumnCollation(ctx, db, "unique_key")
	if err != nil {
		return fmt.Errorf("read unique_key collation: %w", err)
	}
	if uniqueKeyCollation != collation {
		if err := db.WithContext(ctx).Exec(
			"ALTER TABLE jobs MODIFY unique_key VARCHAR(255) COLLATE utf8mb4_0900_as_cs",
		).Error; err != nil {
			return fmt.Errorf("modify unique_key collation: %w", err)
		}
	}

	activeUniqueKeyCollation, err := mysqlColumnCollation(ctx, db, "active_unique_key")
	if err != nil {
		return fmt.Errorf("read active_unique_key collation: %w", err)
	}
	if activeUniqueKeyCollation != collation {
		if err := db.WithContext(ctx).Exec(
			"ALTER TABLE jobs MODIFY active_unique_key VARCHAR(255) COLLATE utf8mb4_0900_as_cs " +
				"GENERATED ALWAYS AS (CASE WHEN status IN ('pending','running') AND unique_key <> '' " +
				"THEN unique_key ELSE NULL END) STORED",
		).Error; err != nil {
			return fmt.Errorf("modify active_unique_key collation: %w", err)
		}
	}

	dispatcherNotNull, err := mysqlDispatcherColumnsNotNull(ctx, db)
	if err != nil {
		return fmt.Errorf("read dispatcher column nullability: %w", err)
	}
	if !dispatcherNotNull {
		if err := db.WithContext(ctx).Exec(
			"ALTER TABLE jobs " +
				"MODIFY status VARCHAR(20) NOT NULL DEFAULT 'pending', " +
				"MODIFY queue VARCHAR(255) NOT NULL DEFAULT 'default', " +
				"MODIFY priority BIGINT NOT NULL DEFAULT 0, " +
				"MODIFY attempt BIGINT NOT NULL DEFAULT 0, " +
				"MODIFY max_retries BIGINT NOT NULL DEFAULT 3",
		).Error; err != nil {
			return fmt.Errorf("modify dispatcher columns not null: %w", err)
		}
	}

	return nil
}

func mysqlColumnCollation(ctx context.Context, db *gorm.DB, columnName string) (string, error) {
	var collation sql.NullString
	if err := db.WithContext(ctx).Raw(`
		SELECT COLLATION_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = ?
	`, columnName).Scan(&collation).Error; err != nil {
		return "", err
	}
	if !collation.Valid {
		return "", nil
	}
	return collation.String, nil
}

func mysqlDispatcherColumnsNotNull(ctx context.Context, db *gorm.DB) (bool, error) {
	var count int
	if err := db.WithContext(ctx).Raw(`
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME IN ('status', 'queue', 'priority', 'attempt', 'max_retries')
		  AND IS_NULLABLE = 'NO'
	`).Scan(&count).Error; err != nil {
		return false, err
	}
	return count == 5, nil
}

func migratePostgresDialectCorrectness(ctx context.Context, db *gorm.DB) error {
	if err := db.WithContext(ctx).Exec(`
		UPDATE jobs
		SET status = COALESCE(status, 'pending'),
		    queue = COALESCE(queue, 'default'),
		    priority = COALESCE(priority, 0),
		    attempt = COALESCE(attempt, 0),
		    max_retries = COALESCE(max_retries, 3)
		WHERE status IS NULL
		   OR queue IS NULL
		   OR priority IS NULL
		   OR attempt IS NULL
		   OR max_retries IS NULL
	`).Error; err != nil {
		return fmt.Errorf("backfill dispatcher column nulls: %w", err)
	}
	if err := db.WithContext(ctx).Exec(
		"ALTER TABLE jobs " +
			"ALTER COLUMN status SET NOT NULL, " +
			"ALTER COLUMN queue SET NOT NULL, " +
			"ALTER COLUMN priority SET NOT NULL, " +
			"ALTER COLUMN attempt SET NOT NULL, " +
			"ALTER COLUMN max_retries SET NOT NULL",
	).Error; err != nil {
		return fmt.Errorf("set dispatcher columns not null: %w", err)
	}

	var thresholdType string
	if err := db.WithContext(ctx).Raw(`
		SELECT data_type
		FROM information_schema.COLUMNS
		WHERE table_schema = CURRENT_SCHEMA()
		  AND table_name = 'fan_outs'
		  AND column_name = 'threshold'
	`).Scan(&thresholdType).Error; err != nil {
		return fmt.Errorf("read fan_outs.threshold type: %w", err)
	}
	if thresholdType == "double precision" {
		return nil
	}
	if err := db.WithContext(ctx).Exec("ALTER TABLE fan_outs ALTER COLUMN threshold TYPE double precision").Error; err != nil {
		return fmt.Errorf("alter fan_outs.threshold type: %w", err)
	}
	return nil
}
