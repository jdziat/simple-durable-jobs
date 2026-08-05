package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestR45_UniqueDedupLookupIsIndexed pins the fix for the dedup pre-check that
// full-scanned the jobs table on Postgres and SQLite.
//
// EnqueueUnique / EnqueueUniqueTx / EnqueueBatch-with-unique-keys all run
//
//	SELECT * FROM jobs WHERE unique_key = ? AND status IN (<core.ActiveDedupStatuses>)
//
// ActiveDedupStatuses is five statuses (pending, running, retrying, waiting,
// paused). The only unique_key index on Postgres/SQLite used to be the PARTIAL
// UNIQUE idx_jobs_active_unique, whose predicate covers just two of them
// (pending, running) — a strict SUBSET, so no planner on any engine may use it:
// a `waiting` duplicate is not in the index, and answering from it would return
// a wrong answer. The plain idx_jobs_unique_key that serves the lookup was
// created on MySQL and DROPPED on Postgres/SQLite as "redundant" (migration 12),
// leaving every idempotency-keyed enqueue O(jobs).
//
// Migration 41 restores that plain index on Postgres/SQLite, which is exactly
// what MySQL has carried all along.
//
// This test asserts on the PLAN, not on wall-clock time, so it cannot go
// false-green on a fast machine.
func TestR45_UniqueDedupLookupIsIndexed(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("EXPLAIN QUERY PLAN assertion is SQLite-specific; see schema_assertions_test.go for the PG/MySQL index assertions")
	}
	ctx := context.Background()

	// A little data so the planner has something to choose between.
	for i := 0; i < 200; i++ {
		job := newTestJob("default", "dedup-plan")
		job.ID = core.NewID()
		require.NoError(t, s.EnqueueUnique(ctx, job, "k-"+core.NewID().String()))
	}

	plan := explainDedupLookup(t, s)
	require.NotContains(t, strings.ToUpper(plan), "SCAN JOBS",
		"the unique-key dedup lookup must not full-scan jobs; plan was:\n%s", plan)
	require.Contains(t, plan, "idx_jobs_unique_key",
		"the dedup lookup must be served by idx_jobs_unique_key; plan was:\n%s", plan)
}

// TestR45_UniqueDedupIndexExistsOnSQLite pins the index itself, so dropping it
// again (or an AutoMigrate-only definition that never reaches an existing
// database) fails here rather than silently reintroducing the full scan.
func TestR45_UniqueDedupIndexExistsOnSQLite(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("sqlite-specific")
	}
	var n int64
	require.NoError(t, s.db.Raw(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_jobs_unique_key'",
	).Scan(&n).Error)
	require.EqualValues(t, 1, n, "idx_jobs_unique_key must exist on sqlite after Migrate")

	// Re-running Migrate must be a no-op, not an error: migrations are
	// re-runnable by contract (every worker calls Migrate at startup).
	require.NoError(t, s.Migrate(context.Background()), "second Migrate must succeed")
	require.NoError(t, s.db.Raw(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_jobs_unique_key'",
	).Scan(&n).Error)
	require.EqualValues(t, 1, n, "idx_jobs_unique_key must survive a second Migrate")
}

// TestR45_UniqueKeyIndexReachesAnExistingDatabase is the UPGRADE path, and it is
// the half a fresh-database test cannot cover.
//
// A fresh Migrate runs migration 12 (which drops idx_jobs_unique_key on
// Postgres/SQLite) and then migration 41 (which recreates it), so the index is
// present at the end no matter which mechanism put it there — an AutoMigrate
// struct tag would satisfy the fresh case just as well as a versioned migration,
// and a tag never reaches a database that already has the table. Every deployed
// database is in exactly that state: schema at v40, no index, and the ONLY thing
// that can add one is a numbered migration the ledger has not recorded yet.
//
// So this rewinds a migrated database to a genuine pre-41 state — index dropped
// AND the ledger row removed — and asserts the next Migrate puts it back.
func TestR45_UniqueKeyIndexReachesAnExistingDatabase(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("sqlite-specific; the Postgres equivalent is TestPostgresSchemaAssertions")
	}
	ctx := context.Background()

	const version = 41
	var name string
	require.NoError(t, s.db.Raw(
		"SELECT name FROM schema_migrations WHERE version = ?", version,
	).Scan(&name).Error)
	require.Equal(t, "unique_key_lookup_index", name,
		"migration %d must be the unique-key lookup index; renumbering it invalidates this guard", version)

	// Give it data first: a database that upgrades in place is never empty, and a
	// migration that quietly only does its work on an empty table would otherwise
	// pass this test while helping nobody.
	for i := 0; i < 50; i++ {
		job := newTestJob("default", "upgrade")
		job.ID = core.NewID()
		require.NoError(t, s.EnqueueUnique(ctx, job, "upgrade-"+core.NewID().String()))
	}

	// Rewind to a pre-41 database.
	require.NoError(t, s.db.Exec("DROP INDEX idx_jobs_unique_key").Error)
	require.NoError(t, s.db.Exec("DELETE FROM schema_migrations WHERE version = ?", version).Error)
	require.False(t, sqliteIndexExists(t, s, "idx_jobs_unique_key"), "rewind failed; the rest of this test would be vacuous")

	require.NoError(t, s.Migrate(ctx), "upgrading an existing database must succeed")
	require.True(t, sqliteIndexExists(t, s, "idx_jobs_unique_key"),
		"migration %d did not reach an already-migrated database: the dedup lookup stays a full scan on every "+
			"deployment that upgrades in place, which is every real one", version)

	// And the lookup is actually served by it after the upgrade, not merely
	// present in sqlite_master.
	plan := explainDedupLookup(t, s)
	require.Contains(t, plan, "idx_jobs_unique_key", "plan after upgrade:\n%s", plan)
}

func sqliteIndexExists(t *testing.T, s *GormStorage, name string) bool {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Raw(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name = ?", name,
	).Scan(&n).Error)
	return n == 1
}

// TestR45_ActiveUniqueConstraintStillEnforced is the guard against fixing the
// LOOKUP by loosening the CONSTRAINT. idx_jobs_active_unique is what makes a
// second active job with the same key impossible at the database level; the new
// plain index is additive and must not have replaced it.
func TestR45_ActiveUniqueConstraintStillEnforced(t *testing.T) {
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("sqlite-specific: MySQL enforces this via the active_unique_key generated column")
	}
	ctx := context.Background()

	first := newTestJob("default", "dedup-constraint")
	first.ID = core.NewID()
	require.NoError(t, s.EnqueueUnique(ctx, first, "held"))

	second := newTestJob("default", "dedup-constraint")
	second.ID = core.NewID()
	require.ErrorIs(t, s.EnqueueUnique(ctx, second, "held"), core.ErrDuplicateJob)

	// The check above only proves the APPLICATION pre-check still returns
	// ErrDuplicateJob — it survives even if the database constraint is gone. So
	// bypass EnqueueUnique entirely and INSERT a second active row with the same
	// key directly: only the partial UNIQUE index can reject that, and rejecting
	// it is the whole point of the index (it is the backstop for Postgres's
	// absent-row FOR UPDATE gap, where two concurrent enqueues both see no row).
	raw := newTestJob("default", "dedup-constraint")
	raw.ID = core.NewID()
	raw.UniqueKey = "held"
	fillEnqueueDefaults(raw)
	row, err := s.encodedJobForCreate(raw)
	require.NoError(t, err)
	require.Error(t, s.db.WithContext(ctx).Create(row).Error,
		"a direct INSERT of a second ACTIVE job with unique_key='held' must be rejected by "+
			"idx_jobs_active_unique; if it succeeds, the database-level uniqueness constraint is gone")

	// A terminal holder must NOT block the key — that is what makes the index
	// PARTIAL rather than a plain UNIQUE index, and a "fix" that widened it to
	// every status would break this.
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", first.ID).
		Update("status", core.StatusCompleted).Error)
	third := newTestJob("default", "dedup-constraint")
	third.ID = core.NewID()
	require.NoError(t, s.EnqueueUnique(ctx, third, "held"),
		"the unique key must be released once its holder is terminal")

	// And the index that backstops it is still UNIQUE and still PARTIAL. Matched
	// on the DDL PREFIX, not on the substring "UNIQUE": the indexed column is
	// named unique_key, so a bare Contains("UNIQUE") passes against a non-unique
	// index and certifies nothing.
	var ddl string
	require.NoError(t, s.db.Raw(
		"SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_jobs_active_unique'",
	).Scan(&ddl).Error)
	normalized := strings.Join(strings.Fields(strings.ToUpper(ddl)), " ")
	require.True(t, strings.HasPrefix(normalized, "CREATE UNIQUE INDEX"),
		"idx_jobs_active_unique must stay a UNIQUE index:\n%s", ddl)
	require.Contains(t, normalized, " WHERE ", "idx_jobs_active_unique must stay PARTIAL:\n%s", ddl)
}

func explainDedupLookup(t *testing.T, s *GormStorage) string {
	t.Helper()
	rows, err := s.db.Raw(
		"EXPLAIN QUERY PLAN SELECT * FROM jobs WHERE unique_key = ? AND status IN (?,?,?,?,?) LIMIT 1",
		"k-probe",
		core.StatusPending, core.StatusRunning, core.StatusRetrying, core.StatusWaiting, core.StatusPaused,
	).Rows()
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		out = append(out, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(out, "\n")
}
