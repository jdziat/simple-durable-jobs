package storage

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestMigratePostgresLegacyBaselineMissingSignalsAndOrphans reproduces the two
// production blockers in the v3 native-uuid conversion against a legacy (pre-v3)
// Postgres baseline, and asserts the conversion now converges cleanly:
//
//  1. Missing signals table — a pre-v3 baseline can predate `signals` entirely.
//     The conversion must NOT abort with `relation "signals" does not exist`
//     (42P01) when it drops/re-adds child foreign keys; the table is created
//     afterwards by AutoMigrate with the correct native-uuid shape.
//  2. Orphaned child rows — legacy schemas had no child FKs, so checkpoints/
//     fan_outs/signals rows whose parent job was already pruned can exist. The
//     conversion must prune them before re-adding the FK, or ADD CONSTRAINT
//     aborts with a 23503 foreign-key violation.
//
// Strategy: build the full v3 schema, downgrade it in place to the legacy shape
// (varchar UUID columns, no `signals` table, no child FKs, an orphan checkpoint),
// clear the run-once fence row so the conversion runs again on re-migrate, then
// re-run Migrate and assert it succeeds with parent data preserved and the orphan
// pruned. Gated on TEST_DATABASE_URL like the other Postgres tests.
func TestMigratePostgresLegacyBaselineMissingSignalsAndOrphans(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	// Run in a dedicated schema so the downgrade + the data-survival assertions are
	// fully isolated from the shared public schema that the rest of the suite uses
	// (a neighbor's cleanup/retention against public must never reap this test's
	// seeded rows). The conversion's information_schema queries use CURRENT_SCHEMA(),
	// so search_path scoping is sufficient.
	schemaName := uniqueSchemaAssertionsName("uuid_legacy_baseline")
	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open postgres admin db")
	closeDBOnCleanup(t, adminDB)
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+quotePostgresIdent(schemaName)).Error)
	t.Cleanup(func() {
		require.NoError(t, adminDB.Exec("DROP SCHEMA IF EXISTS "+quotePostgresIdent(schemaName)+" CASCADE").Error)
	})

	db, err := gorm.Open(postgres.Open(postgresDSNWithSearchPath(t, dsn, schemaName)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open isolated postgres schema")
	closeDBOnCleanup(t, db)

	ctx := context.Background()

	// Converge to the full v3 schema first inside the isolated schema.
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx), "initial v3 migrate")

	// --- Downgrade to a legacy pre-v3 baseline -----------------------------

	// Drop every foreign key that references jobs(id) so jobs.id can be reverted
	// to varchar (a legacy schema had none of these).
	type fkRow struct {
		Table      string `gorm:"column:table_name"`
		Constraint string `gorm:"column:constraint_name"`
	}
	var fks []fkRow
	require.NoError(t, db.Raw(`
		SELECT tc.table_name, tc.constraint_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage ccu
		  ON tc.constraint_name = ccu.constraint_name
		 AND tc.table_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name = 'jobs'
	`).Scan(&fks).Error)
	for _, fk := range fks {
		require.NoError(t, db.Exec("ALTER TABLE "+fk.Table+" DROP CONSTRAINT IF EXISTS "+fk.Constraint).Error)
	}

	// Revert native-uuid columns back to legacy varchar(36).
	legacyReverts := []struct{ table, column string }{
		{"checkpoints", "id"}, {"checkpoints", "job_id"},
		{"fan_outs", "id"}, {"fan_outs", "parent_job_id"},
		{"signals", "id"}, {"signals", "job_id"},
		{"concurrency_slots", "job_id"}, {"unique_locks", "job_id"},
		{"jobs", "parent_job_id"}, {"jobs", "root_job_id"}, {"jobs", "fan_out_id"},
		{"jobs", "id"},
	}
	for _, r := range legacyReverts {
		if !db.Migrator().HasTable(r.table) || !db.Migrator().HasColumn(r.table, r.column) {
			continue
		}
		require.NoError(t, db.Exec("ALTER TABLE "+r.table+" ALTER COLUMN "+r.column+" DROP DEFAULT").Error)
		require.NoError(t, db.Exec(
			"ALTER TABLE "+r.table+" ALTER COLUMN "+r.column+" TYPE varchar(36) USING "+r.column+"::text",
		).Error, "revert %s.%s", r.table, r.column)
	}

	// Blocker #1: a baseline that predates the signals table.
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS signals CASCADE").Error)

	// Seed a valid parent job + valid checkpoint, plus an ORPHAN checkpoint whose
	// parent job does not exist (blocker #2).
	const validJob = "11111111-1111-1111-1111-111111111111"
	const orphanJob = "99999999-9999-9999-9999-999999999999"
	require.NoError(t, db.Exec(
		"INSERT INTO jobs (id, type) VALUES (?, 'test')", validJob,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO checkpoints (id, job_id, call_index, call_type) VALUES (?, ?, 0, 'step')",
		"22222222-2222-2222-2222-222222222222", validJob,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO checkpoints (id, job_id, call_index, call_type) VALUES (?, ?, 0, 'step')",
		"33333333-3333-3333-3333-333333333333", orphanJob,
	).Error)

	// The run-once pre-migration fence (FW-runonce-premigration) records the
	// conversion as DONE after the initial migrate above; clear that row so the
	// conversion runs again over this legacy baseline (mirrors a real first-ever
	// v3 cutover, where the fence has no row yet).
	require.NoError(t, db.Exec("DELETE FROM schema_pre_migrations WHERE name = 'uuid_binary_conversion'").Error)

	// --- Re-migrate over the legacy baseline: must converge ----------------
	require.NoError(t, s.Migrate(ctx), "re-migrate over legacy baseline (missing signals + orphan) must succeed")

	// jobs.id is native uuid again.
	var idType string
	require.NoError(t, db.Raw(
		"SELECT data_type FROM information_schema.columns WHERE table_name='jobs' AND column_name='id'",
	).Scan(&idType).Error)
	require.Equal(t, "uuid", idType, "jobs.id should be native uuid after re-migrate")

	// signals table was recreated by AutoMigrate.
	require.True(t, db.Migrator().HasTable("signals"), "signals table should be recreated by AutoMigrate")

	// Parent job + valid checkpoint preserved; orphan checkpoint pruned.
	var jobCount, validCkpt, orphanCkpt int64
	require.NoError(t, db.Raw("SELECT count(*) FROM jobs WHERE id = ?", validJob).Scan(&jobCount).Error)
	require.Equal(t, int64(1), jobCount, "valid parent job preserved")
	require.NoError(t, db.Raw("SELECT count(*) FROM checkpoints WHERE job_id = ?", validJob).Scan(&validCkpt).Error)
	require.Equal(t, int64(1), validCkpt, "valid checkpoint retained")
	require.NoError(t, db.Raw("SELECT count(*) FROM checkpoints WHERE job_id = ?", orphanJob).Scan(&orphanCkpt).Error)
	require.Equal(t, int64(0), orphanCkpt, "orphan checkpoint pruned before FK re-add")
}
