package storage

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// schemaMigration is one ordered, idempotent schema change applied after
// AutoMigrate. Migrations run in ascending Version order; each is recorded in
// the schema_migrations ledger only after its body succeeds, so a version is
// applied at most once per database.
//
// Bodies MUST be idempotent. On MySQL, DDL auto-commits and cannot be rolled
// back inside the surrounding transaction, so if a migration fails partway the
// ledger row is not written and the migration re-runs on the next Migrate();
// an idempotent body (guarded by HasColumn/HasIndex checks) makes that re-run a
// no-op for the parts that already landed.
type schemaMigration struct {
	Version int
	Name    string
	Up      func(ctx context.Context, db *gorm.DB, dialect string) error
}

// schemaMigrations is the ordered list of versioned migrations. Append new
// migrations with the next Version; never renumber or mutate a shipped entry.
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
// Called from Migrate after AutoMigrate has created/extended the base tables.
func (s *GormStorage) runMigrations(ctx context.Context) error {
	db := s.db.WithContext(ctx)

	if err := db.AutoMigrate(&core.SchemaMigration{}); err != nil {
		return fmt.Errorf("storage: create schema_migrations ledger: %w", err)
	}

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
		if err := db.Create(&core.SchemaMigration{Version: m.Version, Name: m.Name}).Error; err != nil {
			return fmt.Errorf("storage: record migration %d (%s): %w", m.Version, m.Name, err)
		}
	}
	return nil
}

// migrateReworkDequeueIndex replaces the original idx_jobs_dequeue, whose column
// order did not match the dequeue query's predicates, with one that does.
//
// Postgres/SQLite get a partial index over only pending rows — the dequeue set
// is exactly the pending rows, so the index stays small and is fully used for
// the queue filter plus the (priority DESC, created_at ASC) ordering. MySQL has
// no partial indexes, so it keeps a composite index led by the equality
// predicates (status, queue).
func migrateReworkDequeueIndex(ctx context.Context, db *gorm.DB, dialect string) error {
	m := db.Migrator()
	if m.HasIndex(&core.Job{}, "idx_jobs_dequeue") {
		if err := m.DropIndex(&core.Job{}, "idx_jobs_dequeue"); err != nil {
			return fmt.Errorf("drop old idx_jobs_dequeue: %w", err)
		}
	}
	switch dialect {
	case dialectMySQL:
		return db.Exec(
			"CREATE INDEX idx_jobs_dequeue ON jobs (status, queue, priority, created_at)",
		).Error
	default: // sqlite, postgres
		return db.Exec(
			"CREATE INDEX idx_jobs_dequeue ON jobs (queue, priority DESC, created_at ASC) WHERE status = 'pending'",
		).Error
	}
}

// migrateMySQLActiveUniqueKey gives MySQL the active-job uniqueness guarantee
// that Postgres/SQLite get from the idx_jobs_active_unique partial unique index.
//
// MySQL cannot express a partial index, so we add a STORED generated column
// active_unique_key that equals unique_key only while the job is active
// (pending/running) and is NULL otherwise, then put a plain unique index on it.
// MySQL allows duplicate NULLs in a unique index, so terminal jobs free the key
// — identical semantics to the partial index. MySQL recomputes the STORED
// generated column on every row UPDATE, so a job transitioning to a terminal
// status automatically releases its key.
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
		).Error; err != nil {
			return fmt.Errorf("add active_unique_key column: %w", err)
		}
	}
	if !m.HasIndex(&core.Job{}, "idx_jobs_active_unique") {
		if err := db.Exec(
			"CREATE UNIQUE INDEX idx_jobs_active_unique ON jobs (active_unique_key)",
		).Error; err != nil {
			return fmt.Errorf("create idx_jobs_active_unique: %w", err)
		}
	}
	return nil
}
