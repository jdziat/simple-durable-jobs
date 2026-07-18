package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestDequeueBatchPerQueue_ExhaustedQueueDoesNotStarveOthers guards the teardown g3
// fix: a deep, budget-exhausted queue must not prevent other queues' jobs from
// being claimed (the locked path now excludes an exhausted queue from subsequent
// scans instead of re-scanning + SKIP-LOCK-locking its whole backlog). On
// Postgres/MySQL this exercises dequeueBatchLocked (where the fix lives); on sqlite
// it exercises the single-process path. Either way the budget + no-starvation
// contract must hold.
func TestDequeueBatchPerQueue_ExhaustedQueueDoesNotStarveOthers(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("hot", "t")))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("cold", "t")))
	}

	jobs, err := s.DequeueBatchPerQueue(ctx, "w", map[string]int{"hot": 1, "cold": 5})
	require.NoError(t, err)

	counts := map[string]int{}
	for _, j := range jobs {
		counts[j.Queue]++
	}
	assert.Equal(t, 1, counts["hot"], "hot queue's budget of 1 must be respected despite its deep backlog")
	assert.Equal(t, 3, counts["cold"], "cold queue's jobs must not be starved by the deep, exhausted hot queue")
}

type poisonDecodeCodec struct{}

func (poisonDecodeCodec) Encode(plaintext []byte) ([]byte, error) { return plaintext, nil }

func (poisonDecodeCodec) Decode(stored []byte) ([]byte, error) {
	if bytes.Contains(stored, []byte("POISON")) {
		return nil, errors.New("poison decode failure")
	}
	return stored, nil
}

// TestDequeueBatch_PoisonPayloadReleasedNotStranded is the regression test for
// teardown g3: a single undecodable payload (a custom codec failing post-claim)
// used to fail the whole DequeueBatch call, stranding EVERY claimed row locked
// until the reaper (~45m). The poison row must instead be released back to pending
// and excluded, while the successfully-decoded jobs are returned.
func TestDequeueBatch_PoisonPayloadReleasedNotStranded(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db, WithCodec(poisonDecodeCodec{}))
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	good := &core.Job{Type: "t", Queue: "default", Args: []byte(`{"ok":1}`)}
	poison := &core.Job{Type: "t", Queue: "default", Args: []byte(`{"x":"POISON"}`)}
	require.NoError(t, s.Enqueue(ctx, good))
	require.NoError(t, s.Enqueue(ctx, poison))

	jobs, err := s.DequeueBatch(ctx, []string{"default"}, "w", 10)
	require.NoError(t, err, "a poison payload must not fail/strand the whole batch")

	require.Len(t, jobs, 1, "only the decodable job is returned")
	assert.Equal(t, good.ID, jobs[0].ID)

	// The poison row must be released back to pending, not left stranded
	// running/locked until the reaper.
	var poisonRow core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&poisonRow, "id = ?", poison.ID).Error)
	assert.Equal(t, core.StatusPending, poisonRow.Status,
		"the undecodable row must be released to pending, not stranded as running")
	assert.Empty(t, poisonRow.LockedBy, "the released row must not retain a lock owner")
}

// F4: on PG/SQLite the per-queue batch claim (dequeueBatchReturning) runs one
// MATERIALIZED-CTE UPDATE per queue. A non-serialization DB error on a LATER
// queue's statement must not strand the EARLIER queues' already-claimed rows
// (status=running, locked_by=self, never dispatched or released) until the
// stale-lock reaper. The multi-queue claim must be atomic so a mid-batch failure
// rolls the earlier claims back to pending.
func TestDequeueBatchPerQueue_MidBatchErrorDoesNotStrandEarlierClaims(t *testing.T) {
	ctx := context.Background()
	var s *GormStorage
	switch {
	case os.Getenv("TEST_DATABASE_URL") != "":
		s = newPostgresTestStorage(t) // real PG tables — the primary target of this fix
	case os.Getenv("TEST_MYSQL_URL") != "":
		t.Skip("dequeueBatchReturning is the PG/SQLite path; MySQL uses the already-transactional locked path")
	default:
		// Temp-FILE sqlite (not :memory:) so a claim we deliberately fail below
		// cannot surface a fresh, tableless pool connection to the later
		// verification queries — file-backed tables persist across connections.
		db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "f4.db")), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err)
		s = NewGormStorage(db, WithHotStatsCacheTTL(0))
		require.NoError(t, s.Migrate(ctx))
	}

	// Two queues, each with a job and its own budget -> TWO claim statements.
	require.NoError(t, s.Enqueue(ctx, newTestJob("qa", "t")))
	require.NoError(t, s.Enqueue(ctx, newTestJob("qb", "t")))

	// Fail from the SECOND per-queue claim onward (persistent, so a rolled-back
	// retry cannot slip through), simulating a transient DB error mid-batch. A
	// Raw(...).Scan claim can surface through the raw/row/query pipeline depending
	// on GORM internals, so hook all three and dedup by the shared Statement so a
	// single claim is counted exactly once.
	var claims int
	seen := map[*gorm.Statement]bool{}
	inject := func(db *gorm.DB) {
		sql := db.Statement.SQL.String()
		if !strings.Contains(sql, "UPDATE jobs") || !strings.Contains(sql, "SELECT id FROM claimed") {
			return
		}
		if seen[db.Statement] {
			return
		}
		seen[db.Statement] = true
		claims++
		if claims >= 2 {
			_ = db.AddError(errors.New("injected mid-batch failure"))
		}
	}
	require.NoError(t, s.db.Callback().Raw().After("gorm:raw").Register("test:f4_raw", inject))
	require.NoError(t, s.db.Callback().Row().After("gorm:row").Register("test:f4_row", inject))
	require.NoError(t, s.db.Callback().Query().After("gorm:query").Register("test:f4_query", inject))
	removeInjector := func() {
		_ = s.db.Callback().Raw().Remove("test:f4_raw")
		_ = s.db.Callback().Row().Remove("test:f4_row")
		_ = s.db.Callback().Query().Remove("test:f4_query")
	}
	defer removeInjector()

	_, err := s.DequeueBatchPerQueue(ctx, "w", map[string]int{"qa": 5, "qb": 5})
	require.Error(t, err, "a mid-batch claim failure must surface")

	// Remove the injector before the verification queries.
	removeInjector()

	// No job may be left claimed (running/locked) after the failed batch — the
	// earlier queue's claim must have rolled back, not autocommitted-then-stranded.
	var stranded int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("status = ? AND locked_by <> ?", core.StatusRunning, "").
		Count(&stranded).Error)
	assert.Equal(t, int64(0), stranded, "no job may be stranded running/locked after a mid-batch failure")

	// Both jobs remain pending and re-claimable.
	var pending int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("status = ?", core.StatusPending).Count(&pending).Error)
	assert.Equal(t, int64(2), pending, "both jobs remain pending after the rolled-back batch")
}
