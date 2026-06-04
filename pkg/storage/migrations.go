package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

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
// GET_LOCK, each held on a DEDICATED connection for fn's whole duration; the
// migration work itself runs on the normal pool. Because both locks are
// cross-session mutexes, holding one on a side connection serializes the fleet
// regardless of which pooled connection runs each statement. SQLite is
// single-writer and needs no lock.
//
// This is what makes Migrate() genuinely idempotent under concurrency: the
// second worker blocks until the first finishes, then sees every table, index,
// and ledger row already present and does nothing.
func (s *GormStorage) withMigrationLock(ctx context.Context, fn func() error) error {
	if s.isSQLite {
		return fn()
	}
	dialect := s.dialect()
	if dialect != dialectPostgres && dialect != dialectMySQL {
		return fn()
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	switch dialect {
	case dialectPostgres:
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrateAdvisoryLockKey); err != nil {
			return fmt.Errorf("storage: acquire migration lock: %w", err)
		}
		defer func() {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrateAdvisoryLockKey)
		}()
	case dialectMySQL:
		var got sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", mysqlMigrateLockName, migrateLockTimeoutSecs).Scan(&got); err != nil {
			return fmt.Errorf("storage: acquire migration lock: %w", err)
		}
		if !got.Valid || got.Int64 != 1 {
			return fmt.Errorf("storage: timed out acquiring migration lock after %ds", migrateLockTimeoutSecs)
		}
		defer func() {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT RELEASE_LOCK(?)", mysqlMigrateLockName)
		}()
	}
	return fn()
}

// schemaMigration is one ordered schema change applied after AutoMigrate.
// Migrations run in ascending Version order; each is recorded in the
// schema_migrations ledger only after its body succeeds, so a version is
// applied at most once per database.
//
// CONCURRENCY: Migrate() is documented as safe to call from every worker at
// startup, so a migration body MUST be idempotent and tolerant of a racing
// worker that already applied (part of) it. On Postgres the whole loop runs
// under a transaction-level advisory lock (and DDL is transactional); on MySQL
// (DDL auto-commits, can't span a transaction) and SQLite (single writer) the
// idempotent + benign-error-tolerant bodies plus the ON CONFLICT ledger insert
// are what make concurrent Migrate() safe.
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
}

// runMigrations applies any schemaMigrations not yet recorded in the ledger.
// Called from Migrate while the fleet-wide migration lock is held (see
// withMigrationLock), so it is not racing other workers. The idempotent bodies
// and ON CONFLICT ledger insert remain as a backstop.
func (s *GormStorage) runMigrations(ctx context.Context) error {
	return s.applyPendingMigrations(ctx, s.db.WithContext(ctx))
}

// applyPendingMigrations runs every migration whose version is absent from the
// ledger, in order, recording each with an ON CONFLICT ledger insert so a
// racing worker that already recorded it is a no-op rather than a PK error.
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
