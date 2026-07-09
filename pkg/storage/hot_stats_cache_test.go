package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newHotStatsTestStorage builds a migrated storage with an explicit cache TTL and
// a single DB connection. The single connection keeps SQLite ":memory:" one
// database (a multi-connection pool gives each connection its own :memory: DB) so
// seeded rows are visible to the aggregate load.
func newHotStatsTestStorage(t *testing.T, ttl time.Duration) (*GormStorage, *gorm.DB) {
	t.Helper()
	db := openTestDB(t)
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	s := NewGormStorage(db, WithHotStatsCacheTTL(ttl))
	require.NoError(t, s.Migrate(context.Background()))
	return s, db
}

// countMatchingQueries registers a gorm query callback that counts executed
// statements whose SQL contains marker (lowercased), so a test can assert how
// many times an aggregate actually hit the DB.
func countMatchingQueries(t *testing.T, db *gorm.DB, marker string) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	err := db.Callback().Query().After("gorm:query").Register("test:count:"+marker, func(d *gorm.DB) {
		if strings.Contains(strings.ToLower(d.Statement.SQL.String()), marker) {
			n.Add(1)
		}
	})
	require.NoError(t, err)
	return &n
}

// TestHotStatsCache_CoalescesConcurrentCallers proves the load-bearing property:
// many concurrent dashboard/metrics readers collapse onto ONE DB aggregate, a
// second read within the TTL adds zero, and a read after the TTL adds one. This
// asserts query COUNT only, so it does not depend on seeded data.
func TestHotStatsCache_CoalescesConcurrentCallers(t *testing.T) {
	ctx := context.Background()
	s, db := newHotStatsTestStorage(t, 200*time.Millisecond)
	n := countMatchingQueries(t, db, "count(*) as count")

	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			_, err := s.GetQueueDepthStats(ctx)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), n.Load(), "concurrent callers must coalesce onto one aggregate")

	// A read within the TTL is served from cache — no new query.
	_, err := s.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), n.Load(), "read within TTL must not re-query")

	// After the TTL, exactly one refresh.
	time.Sleep(250 * time.Millisecond)
	_, err = s.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), n.Load(), "read after TTL must refresh exactly once")
}

// TestHotStatsCache_TTLZeroBypass proves TTL<=0 disables the cache so every call
// hits the DB (the mode newTestStorage uses to keep exact-count tests
// deterministic).
func TestHotStatsCache_TTLZeroBypass(t *testing.T) {
	ctx := context.Background()
	s, db := newHotStatsTestStorage(t, 0)
	n := countMatchingQueries(t, db, "count(*) as count")

	for i := 0; i < 3; i++ {
		_, err := s.GetQueueDepthStats(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, int64(3), n.Load(), "TTL<=0 must bypass the cache")
}

// TestHotStatsCache_DoesNotCacheError proves a failed load is surfaced and NOT
// memoized: a transient failure must not stick for the whole TTL.
func TestHotStatsCache_DoesNotCacheError(t *testing.T) {
	ctx := context.Background()
	s, db := newHotStatsTestStorage(t, time.Minute) // long TTL: if the error were cached it would stick

	var failNext atomic.Bool
	failNext.Store(true)
	err := db.Callback().Query().After("gorm:query").Register("test:failonce", func(d *gorm.DB) {
		if strings.Contains(strings.ToLower(d.Statement.SQL.String()), "count(*) as count") &&
			failNext.CompareAndSwap(true, false) {
			_ = d.AddError(errors.New("injected transient failure"))
		}
	})
	require.NoError(t, err)

	_, err1 := s.GetQueueDepthStats(ctx)
	require.Error(t, err1, "first load must surface the injected error")

	_, err2 := s.GetQueueDepthStats(ctx)
	require.NoError(t, err2, "a subsequent call must NOT be served the memoized error")
}

// TestHotStatsCache_ReturnsDeepCopy proves callers get an isolated copy: mutating
// a returned value must not corrupt the shared cached snapshot.
func TestHotStatsCache_ReturnsDeepCopy(t *testing.T) {
	ctx := context.Background()
	s, _ := newHotStatsTestStorage(t, time.Minute)
	require.NoError(t, s.Enqueue(ctx, newTestJob("q", "work")))

	a, err := s.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, a)
	a[0].Pending = 999999 // caller mutates its copy

	b, err := s.GetQueueDepthStats(ctx) // served from cache
	require.NoError(t, err)
	require.NotEmpty(t, b)
	assert.NotEqual(t, int64(999999), b[0].Pending, "cached snapshot must not be corrupted by a caller mutation")
}

// TestHotStatsCache_DeadLetterAndOldestPendingCoalesce covers the two
// metrics_counts.go aggregates the same way.
func TestHotStatsCache_DeadLetterAndOldestPendingCoalesce(t *testing.T) {
	ctx := context.Background()
	s, db := newHotStatsTestStorage(t, 200*time.Millisecond)
	dlq := countMatchingQueries(t, db, "dead_lettered_at")
	oldest := countMatchingQueries(t, db, "oldest_pending_at")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = s.QueueDeadLetterCounts(ctx) }()
		go func() { defer wg.Done(); _, _ = s.QueueOldestPendingAt(ctx) }()
	}
	wg.Wait()
	require.Equal(t, int64(1), dlq.Load(), "dead-letter aggregate must coalesce")
	require.Equal(t, int64(1), oldest.Load(), "oldest-pending aggregate must coalesce")
}

// TestHotStatsCache_LoaderPanicDoesNotWedge proves a panic in the load fn is
// converted to an error and does NOT wedge the aggregate (c.load reset,
// load.done closed) or leak waiters — the cache stays usable afterward.
func TestHotStatsCache_LoaderPanicDoesNotWedge(t *testing.T) {
	var c hotStatCache[int]
	identity := func(v int) int { return v }

	_, err := c.do(context.Background(), time.Minute, identity, func(context.Context) (int, error) {
		panic("boom")
	})
	require.Error(t, err, "a panicking loader must surface an error, not hang")

	got, err := c.do(context.Background(), time.Minute, identity, func(context.Context) (int, error) {
		return 42, nil
	})
	require.NoError(t, err, "the cache must be usable after a loader panic")
	require.Equal(t, 42, got)
}

// TestGetQueueDepthStats_PausedReadErrorNotSwallowed (B-02) proves a failed
// pause-state read is surfaced, not silently rendered as all-unpaused. Uses TTL=0
// storage so the aggregate runs live.
func TestGetQueueDepthStats_PausedReadErrorNotSwallowed(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t) // WithHotStatsCacheTTL(0)
	require.NoError(t, s.Enqueue(ctx, newTestJob("q1", "work")))

	// Break ONLY the paused-queue read (GetPausedQueues reads queue_states),
	// leaving the jobs aggregates intact so the function runs end-to-end and
	// reaches the pause read. Inject the error via a query callback rather than
	// DropTable(queue_states): on the shared MySQL DB a later test's Migrate would
	// recreate queue_states WITHOUT its case-sensitive (as_cs) collation — the
	// collation migration is recorded as already-applied and won't re-run — which
	// corrupts cross-table queue comparisons (Error 1267) for every later test.
	require.NoError(t, s.db.Callback().Query().After("gorm:query").Register("test:failpaused", func(d *gorm.DB) {
		if strings.Contains(strings.ToLower(d.Statement.SQL.String()), "queue_states") {
			_ = d.AddError(errors.New("injected paused-read failure"))
		}
	}))
	t.Cleanup(func() { _ = s.db.Callback().Query().Remove("test:failpaused") })

	_, err := s.GetQueueDepthStats(ctx)
	require.Error(t, err, "a failed paused-queue read must not be swallowed (rendering queues unpaused)")
}

// TestGetQueueDepthStats_ReflectsPauseState guards the happy path: propagating the
// pause-read error did not break normal pause reporting.
func TestGetQueueDepthStats_ReflectsPauseState(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	require.NoError(t, s.Enqueue(ctx, newTestJob("q1", "work")))
	require.NoError(t, s.Enqueue(ctx, newTestJob("q2", "work")))
	require.NoError(t, s.PauseQueue(ctx, "q1"))

	stats, err := s.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	byQueue := map[string]bool{}
	for _, qs := range stats {
		byQueue[qs.Name] = qs.IsPaused
	}
	assert.True(t, byQueue["q1"], "q1 must report paused")
	assert.False(t, byQueue["q2"], "q2 must report not paused")
}
