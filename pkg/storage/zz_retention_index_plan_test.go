package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// retentionCandidateSelect runs one retention pass against a statement-capturing
// storage and returns the SQL of the candidate SELECT that pass actually issued.
//
// It captures the SHIPPED statement rather than rebuilding a lookalike: the whole
// point of the guard below is that the query and the partial index must agree,
// and a query a test wrote itself agrees with whatever the test wants.
func retentionCandidateSelect(t *testing.T, ctx context.Context, s *GormStorage, capture *stmtCaptureLogger, status core.JobStatus) string {
	t.Helper()
	capture.stmts = nil
	_, err := s.DeleteTerminalJobsOlderThan(ctx, status, time.Hour, 100)
	require.NoError(t, err)

	var found string
	for _, stmt := range capture.stmts {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "SELECT") &&
			strings.Contains(stmt, "completed_at") {
			found = stmt
			break
		}
	}
	require.NotEmpty(t, found, "retention pass issued no candidate SELECT; captured: %v", capture.stmts)
	return found
}

// TestRetentionCandidateSelectUsesItsIndex pins that the retention sweep's
// candidate SELECT can actually USE idx_jobs_retention_terminal.
//
// The index is PARTIAL — `WHERE status IN ('completed','failed','cancelled') AND
// completed_at IS NOT NULL` — and the query filters `status = ?`. Those are
// logically equivalent for the three statuses this function accepts, but SQLite
// matches a partial index against the terms the query actually carries rather
// than deriving them, so without the redundant IN term the index built for this
// sweep cannot serve it and is pure write cost on every terminal transition.
//
// No behavioural test can see this: the sweep deletes exactly the same rows
// either way. Only the plan changes.
func TestRetentionCandidateSelectUsesItsIndex(t *testing.T) {
	ctx := context.Background()
	capture := &stmtCaptureLogger{Interface: logger.Default.LogMode(logger.Silent)}
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("EXPLAIN QUERY PLAN output is SQLite-specific")
	}
	db.Logger = capture
	s := NewGormStorage(db, WithHotStatsCacheTTL(0))
	require.NoError(t, s.Migrate(ctx))

	// Enough terminal history for the planner to prefer an index over a scan,
	// plus live rows the index deliberately does not cover.
	old := time.Now().Add(-48 * time.Hour).UTC()
	for i := 0; i < 400; i++ {
		job := &core.Job{Type: "retplan", Queue: fmt.Sprintf("q%d", i%4)}
		require.NoError(t, s.Enqueue(ctx, job))
		if i%10 == 0 {
			continue // stays pending: not covered by the partial index
		}
		status := core.StatusCompleted
		switch i % 3 {
		case 1:
			status = core.StatusFailed
		case 2:
			status = core.StatusCancelled
		}
		require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
			Where("id = ?", job.ID).
			Updates(map[string]any{"status": status, "completed_at": old}).Error)
	}
	require.NoError(t, s.db.WithContext(ctx).Exec("ANALYZE").Error)

	for _, status := range core.TerminalJobStatuses {
		t.Run(string(status), func(t *testing.T) {
			sql := retentionCandidateSelect(t, ctx, s, capture, status)
			plan := explainQueryPlanSQL(t, s, sql)
			t.Logf("plan: %s", plan)
			assert.Contains(t, plan, "idx_jobs_retention_terminal",
				"the retention candidate SELECT must be able to use the partial index built "+
					"for it; dropping the redundant status IN (...) term makes the index "+
					"unusable (INDEXED BY answers \"no query solution\") while changing no row "+
					"the sweep deletes — see retentionTerminalIndexPredicate")
		})
	}
}

// shippedRetentionIndexDDL is the index DDL as ALREADY-RELEASED versions created
// it, spelled out here and never regenerated from retentionTerminalIndexPredicate.
// A migration that has run on a user's database is history: the index on that
// database carries THIS text no matter what the constant later says.
const shippedRetentionIndexDDL = "CREATE INDEX idx_jobs_retention_terminal ON jobs (status, completed_at, id) " +
	"WHERE status IN ('completed','failed','cancelled') AND completed_at IS NOT NULL"

// TestRetentionQueryMatchesAlreadyMigratedDatabases is the guard on
// retentionTerminalIndexPredicate DRIFTING away from the index that is already on
// disk everywhere.
//
// Because the migration now builds the index from the same constant the query
// repeats, editing the constant keeps a FRESH database self-consistent — and
// silently stops matching every database an earlier release already migrated.
// This recreates the index exactly as those releases wrote it and then asserts
// the shipped query still plans onto it.
func TestRetentionQueryMatchesAlreadyMigratedDatabases(t *testing.T) {
	ctx := context.Background()
	capture := &stmtCaptureLogger{Interface: logger.Default.LogMode(logger.Silent)}
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("EXPLAIN QUERY PLAN output is SQLite-specific")
	}
	db.Logger = capture
	s := NewGormStorage(db, WithHotStatsCacheTTL(0))
	require.NoError(t, s.Migrate(ctx))

	// Replace the just-migrated index with the one a released version wrote.
	require.NoError(t, s.db.WithContext(ctx).Exec("DROP INDEX IF EXISTS idx_jobs_retention_terminal").Error)
	require.NoError(t, s.db.WithContext(ctx).Exec(shippedRetentionIndexDDL).Error)

	old := time.Now().Add(-48 * time.Hour).UTC()
	for i := 0; i < 400; i++ {
		job := &core.Job{Type: "retlegacy", Queue: fmt.Sprintf("q%d", i%4)}
		require.NoError(t, s.Enqueue(ctx, job))
		if i%10 == 0 {
			continue
		}
		require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
			Where("id = ?", job.ID).
			Updates(map[string]any{"status": core.StatusCompleted, "completed_at": old}).Error)
	}
	require.NoError(t, s.db.WithContext(ctx).Exec("ANALYZE").Error)

	sql := retentionCandidateSelect(t, ctx, s, capture, core.StatusCompleted)
	plan := explainQueryPlanSQL(t, s, sql)
	t.Logf("plan: %s", plan)
	assert.Contains(t, plan, "idx_jobs_retention_terminal",
		"retentionTerminalIndexPredicate must keep matching the index text that "+
			"already-released migrations put on disk; see shippedRetentionIndexDDL")
}

// explainQueryPlanSQL explains a raw SQL string (vars already inlined by GORM's
// statement capture), unlike explainQueryPlan which takes a built *gorm.Statement.
func explainQueryPlanSQL(t *testing.T, s *GormStorage, sql string) string {
	t.Helper()
	// The capture logger is attached to s.db; explaining through it would append
	// to the capture. Use a silent session.
	rows, err := s.db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}).
		Raw("EXPLAIN QUERY PLAN " + sql).Rows()
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, plan, "EXPLAIN QUERY PLAN returned nothing")
	return strings.Join(plan, " | ")
}
