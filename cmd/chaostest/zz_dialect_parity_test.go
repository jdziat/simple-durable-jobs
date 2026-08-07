package main

import (
	"context"
	"os"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// EVERY existing harness test calls checkExactlyOnce with dialectSQLite. Zero
// exercise the Postgres or MySQL branches — and checkExactlyOnce's window-checkpoint
// join is written THREE different ways, one per dialect, because the two sides of
// the comparison are different types:
//
//	Postgres  cp.job_id::text                       (uuid column)
//	MySQL     BIN_TO_UUID(cp.job_id)                (binary(16) column)
//	SQLite    hand-assembled hex(...) with dashes   (16-byte blob, no UUID type)
//
// That asymmetry is not incidental — it is the whole defect this sub-check was
// repaired from. The code's own comment records that the join "was effectively dead
// since the v3 binary-UUID migration" on BOTH non-SQLite backends, because
// `cp.job_id = ce.job_id` raises "operator does not exist: uuid = text" on Postgres
// and silently never matches on MySQL, and an error-swallowing .Scan() hid both.
//
// So the repair for a join that was dead on Postgres and MySQL was verified only on
// SQLite. That is the same defect one layer out, and it matters more than it looks:
// ci.yml's release-gating chaos smoke runs `scripts/chaos-test.sh postgres`, so the
// dialect whose branch is unproven is the exact dialect that gates every release.
//
// This runs the two-sided proof — the join must NOT match a re-exec marker without
// a checkpoint, and MUST match one with it — against whichever real backend is
// configured. It is the same assertion TestWindowCheckpointJoinActuallyMatches makes
// on SQLite, which is the point: parity, not a new property.
//
// Skips when the backend is not configured, so `go test ./...` stays green offline.
func TestWindowCheckpointJoinMatchesOnEveryDialect(t *testing.T) {
	for _, tc := range []struct {
		name    string
		envVar  string
		dialect string
		open    func(string) gorm.Dialector
		// insertCheckpoint differs per backend only in how a UUID literal is bound;
		// the library's own codec handles it, so the raw SQL is dialect-shaped.
		checkpointSQL string
	}{
		{
			name:    "postgres",
			envVar:  "TEST_DATABASE_URL",
			dialect: dialectPostgres,
			open:    func(dsn string) gorm.Dialector { return postgres.Open(dsn) },
			checkpointSQL: `INSERT INTO checkpoints (id, job_id, call_index, call_type, result, created_at)
			                VALUES (?, ?, -1, 'extract', '"ok"', NOW())`,
		},
		{
			name:    "mysql",
			envVar:  "TEST_MYSQL_URL",
			dialect: dialectMySQL,
			open:    func(dsn string) gorm.Dialector { return gormmysql.Open(dsn) },
			checkpointSQL: `INSERT INTO checkpoints (id, job_id, call_index, call_type, result, created_at)
			                VALUES (?, ?, -1, 'extract', '"ok"', NOW(6))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := os.Getenv(tc.envVar)
			if dsn == "" {
				t.Skipf("%s not set; this dialect's branch of the window-checkpoint join is UNVERIFIED in this run", tc.envVar)
			}

			ctx := context.Background()
			db, err := gorm.Open(tc.open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlDB.Close() })

			// Isolate from whatever else shares this database: drop and rebuild the
			// chaos ledger, and scope every assertion to job IDs minted here.
			require.NoError(t, db.Exec(`DROP TABLE IF EXISTS chaos_effects`).Error)
			store := jobs.NewGormStorage(db)
			require.NoError(t, store.Migrate(ctx), "migrate the library schema")
			require.NoError(t, ensureLedger(ctx, db, tc.dialect), "create the chaos ledger")
			t.Cleanup(func() { _ = db.Exec(`DROP TABLE IF EXISTS chaos_effects`).Error })

			// checkpoints.job_id carries a REAL foreign key to jobs on Postgres and
			// MySQL. SQLite does not enforce it by default, which is why the SQLite
			// version of this proof gets away with a checkpoint for a job that was
			// never enqueued — and is a small illustration of the same point this
			// test exists to make: a fixture that works on SQLite can be impossible
			// on the backend that gates releases.
			job := &jobs.Job{
				ID: jobs.NewID(), Type: "chaos.pipeline_window", Queue: "chaos",
				Args: []byte(`{}`), Status: "running", MaxRetries: 3,
			}
			require.NoError(t, store.Enqueue(ctx, job), "seed the job the checkpoint hangs off")
			jobID := job.ID

			// One atomic effect so the population guard is satisfied either way —
			// otherwise a "fail" below could be the empty-population guard firing
			// rather than the join matching, and the test would prove nothing.
			require.NoError(t, insertEffectAttempt(ctx, db, jobs.NewID(), "phase:x", newAttemptNonce()))
			require.NoError(t, insertEffect(ctx, db, jobID, "window-reexec:extract"))

			clean := checkExactlyOnce(ctx, db, tc.dialect)
			require.True(t, clean.pass,
				"a re-exec marker with NO checkpoint is the documented at-least-once window and must pass: %s",
				clean.detail)
			require.Contains(t, clean.detail, "checkpointed_reexec_markers=0",
				"the join must return a real zero here, not an error swallowed into one")

			// Now commit the phase checkpoint the marker refers to. call_index -1 is
			// the phase-checkpoint key; call_type is the bare phase name.
			require.NoError(t, db.Exec(tc.checkpointSQL, jobs.NewID(), jobID).Error)

			dirty := checkExactlyOnce(ctx, db, tc.dialect)
			assert.False(t, dirty.pass,
				"a re-exec whose checkpoint HAD committed is a genuine exactly-once violation. "+
					"If this PASSES on %s, the join is not matching and this sub-check is dead on the "+
					"dialect the release gate actually runs: %s", tc.name, dirty.detail)
			assert.Contains(t, dirty.detail, "checkpointed_reexec_markers=1",
				"the join must match exactly the one seeded row")
		})
	}
}
