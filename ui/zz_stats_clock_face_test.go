package ui

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// job_stats.timestamp is written from the collector's LOCAL wall clock
// (StatsCollector.snapshot -> time.Now().Truncate(time.Minute)), and on SQLite
// that column is TEXT carrying its own UTC offset. A DST fall-back makes ONE
// process in ONE zone render TWO offsets inside a single hour, so a bare
// `timestamp >= ?` / `timestamp <= ?` — a LEXICAL compare of wall faces — puts
// rows on the -08:00 face above rows on the -07:00 face even though their
// instants run the other way.
//
// These fixtures pin the zone explicitly rather than relying on the host TZ, so
// they assert the same thing under TZ=UTC and TZ=America/Los_Angeles.
const (
	// 2026-11-01 is the US fall-back date: 01:59:59-07:00 is followed by
	// 01:00:00-08:00, so 08:35Z..09:35Z spans both faces.
	dstFoldWindowStartUTC = "2026-11-01T08:35:00Z"
	dstFoldWindowEndUTC   = "2026-11-01T09:35:00Z"
)

func newStatsStorageForFaceTest(t *testing.T) (StatsStorage, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	s := NewGormStatsStorage(db)
	require.NoError(t, s.MigrateStats(context.Background()))
	return s, db
}

// seedMinuteBuckets writes one bucket per minute over [start, end] the way
// StatsCollector.snapshot writes them: the instant rendered on zone's face,
// truncated to the minute.
func seedMinuteBuckets(t *testing.T, s StatsStorage, zone *time.Location, start, end time.Time) int {
	t.Helper()
	ctx := context.Background()
	n := 0
	for ts := start; !ts.After(end); ts = ts.Add(time.Minute) {
		require.NoError(t, s.UpsertStatCounters(ctx, "q", ts.In(zone).Truncate(time.Minute), 1, 0, 0))
		n++
	}
	return n
}

func mustParseUTC(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts
}

// GetStatsHistory must return every bucket whose INSTANT is inside the window,
// whatever face it was stored on. Before the fix it returned 2 of 61 across the
// fall-back hour: the dashboard's Throughput chart read near-zero for an hour in
// which the queue was working normally, with no error anywhere.
func TestGetStatsHistory_KeepsEveryBucketAcrossTheDSTFallBack(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	s, _ := newStatsStorageForFaceTest(t)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	written := seedMinuteBuckets(t, s, la, start, end)
	require.Equal(t, 61, written, "PREMISE: the fixture did not write a full hour of buckets")

	// Exactly what parsePeriod("1h") yields for a process in America/Los_Angeles
	// standing at the end of the fold: the two bounds land on DIFFERENT faces.
	until := end.In(la)
	since := until.Add(-time.Hour)
	require.NotEqual(t, since.Format("-07:00"), until.Format("-07:00"),
		"PREMISE: the two window bounds landed on the same face, so this proves nothing")

	got, err := s.GetStatsHistory(context.Background(), "q", since, until)
	require.NoError(t, err)
	require.Len(t, got, written,
		"every minute-bucket whose INSTANT is inside the 1h window must be returned; "+
			"the DST fall-back hour is being dropped")

	// The chart's own number, not just the row count.
	var completed int64
	for _, row := range got {
		completed += row.Completed
	}
	require.Equal(t, int64(written), completed, "throughput under-reported across the fold")
}

// Control: the identical fixture on a FIXED-offset zone — one face, no fold —
// must return all 61 both before and after the fix. If this ever goes red the
// fixture, not the predicate, is what broke.
func TestGetStatsHistory_ControlSingleFaceWindowIsWhole(t *testing.T) {
	s, _ := newStatsStorageForFaceTest(t)
	fixed := time.FixedZone("PST", -8*60*60)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	written := seedMinuteBuckets(t, s, fixed, start, end)

	until := end.In(fixed)
	got, err := s.GetStatsHistory(context.Background(), "q", until.Add(-time.Hour), until)
	require.NoError(t, err)
	require.Len(t, got, written)
}

// Ordering must be by INSTANT too. The fold writes 01:00..01:59 twice, once on
// each face, and a face-blind ORDER BY interleaves them.
func TestGetStatsHistory_ReturnsFoldRowsInInstantOrder(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	s, _ := newStatsStorageForFaceTest(t)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	seedMinuteBuckets(t, s, la, start, end)

	got, err := s.GetStatsHistory(context.Background(), "q",
		start.Add(-time.Minute), end.Add(time.Minute))
	require.NoError(t, err)
	require.NotEmpty(t, got)
	for i := 1; i < len(got); i++ {
		require.False(t, got[i].Timestamp.Before(got[i-1].Timestamp),
			"row %d (%s) sorts before row %d (%s): the ORDER BY is comparing wall faces",
			i, got[i].Timestamp.Format(time.RFC3339), i-1, got[i-1].Timestamp.Format(time.RFC3339))
	}
}

// PruneStats must never delete a row NEWER than its cutoff. The same lexical
// compare in the retention prune deleted rows on the PST half of the fold — which
// are newer than a PDT-faced cutoff but render a smaller wall string — so up to an
// hour of stats was destroyed, once a year per DST zone.
func TestPruneStats_KeepsEveryRowInsideTheRetentionWindow(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	s, _ := newStatsStorageForFaceTest(t)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	written := seedMinuteBuckets(t, s, la, start, end)

	// Verbatim StatsCollector.prune's cutoff: now.Add(-retention), on the process
	// face. Every seeded row is strictly inside this window.
	const retention = time.Hour
	cutoff := end.In(la).Add(-retention).Add(-time.Minute)

	deleted, err := s.PruneStats(context.Background(), cutoff)
	require.NoError(t, err)
	require.Zero(t, deleted, "PruneStats deleted rows inside the retention window")

	got, err := s.GetStatsHistory(context.Background(), "q",
		start.Add(-time.Minute), end.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, got, written, "rows inside the retention window did not survive the prune")
}

// The other half of the prune contract, so a fix that simply stops deleting
// cannot pass: rows OLDER than the cutoff must still go, across the fold.
func TestPruneStats_StillDeletesEveryRowOlderThanTheCutoff(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	s, _ := newStatsStorageForFaceTest(t)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	written := seedMinuteBuckets(t, s, la, start, end)

	// A cutoff past the whole fixture: everything is older, so everything goes.
	deleted, err := s.PruneStats(context.Background(), end.In(la).Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(written), deleted,
		"the prune stopped deleting rows it is supposed to reap")

	got, err := s.GetStatsHistory(context.Background(), "q", time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Empty(t, got)
}

// A cutoff INSIDE the fold must split it by instant, not by wall text: the rows
// before it go, the rows after it stay, and the two counts add up.
func TestPruneStats_SplitsTheFoldByInstant(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	s, _ := newStatsStorageForFaceTest(t)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	written := seedMinuteBuckets(t, s, la, start, end)

	// 08:35Z + 20m = 08:55Z, still on the -07:00 half; 20 rows are strictly older.
	cutoff := start.Add(20 * time.Minute).In(la)
	deleted, err := s.PruneStats(context.Background(), cutoff)
	require.NoError(t, err)
	require.Equal(t, int64(20), deleted, "the fold was split by wall text, not by instant")

	got, err := s.GetStatsHistory(context.Background(), "q", time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Len(t, got, written-20)
	for _, row := range got {
		require.False(t, row.Timestamp.Before(cutoff),
			"a row older than the cutoff survived the prune: %s", row.Timestamp.Format(time.RFC3339))
	}
}

// The loose bare-column clause is what keeps SQLite's index RANGE restriction on
// idx_job_stats_timestamp once the exact comparison wraps the column in
// CASE/julianday. Deleting it changes no result — it is a pure prefilter — so no
// row-count assertion can see it go. Its EFFECT can be seen, and that is what this
// pins, on the SQL production actually issues rather than on a hand-built
// lookalike.
//
// Measured cost of losing it, 357,120 rows: read 5.5ms -> 23.1ms scoped,
// 85.3ms -> 172.4ms all-queues, prune 25.5ms -> 62.6ms with a full SCAN.
func TestStatsTimestampWindowKeepsTheIndexRangeRestriction(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	s, db := newStatsStorageForFaceTest(t)
	require.NoError(t, db.Exec("CREATE INDEX IF NOT EXISTS idx_job_stats_timestamp ON job_stats (timestamp)").Error)
	start := mustParseUTC(t, dstFoldWindowStartUTC)
	end := mustParseUTC(t, dstFoldWindowEndUTC)
	seedMinuteBuckets(t, s, la, start, end)
	require.NoError(t, db.Exec("ANALYZE").Error)

	for _, tc := range []struct {
		name string
		hook string
		want string
		run  func()
	}{
		{"read", "gorm:query", "timestamp>", func() {
			_, err := s.GetStatsHistory(context.Background(), "", start, end)
			require.NoError(t, err)
		}},
		{"prune", "gorm:delete", "timestamp<", func() {
			_, err := s.PruneStats(context.Background(), start)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql, vars := captureStatsSQL(t, db, tc.hook, tc.run)
			var plan []struct {
				ID, Parent, NotUsed int
				Detail              string
			}
			require.NoError(t, db.Raw("EXPLAIN QUERY PLAN "+sql, vars...).Scan(&plan).Error)
			joined := ""
			for _, p := range plan {
				joined += p.Detail + " | "
			}
			require.Contains(t, joined, tc.want,
				"the index range restriction on job_stats.timestamp was lost; plan = %s", joined)
		})
	}
}

// captureStatsSQL records the SQL and binds a production call actually issues.
func captureStatsSQL(t *testing.T, db *gorm.DB, hook string, run func()) (string, []any) {
	t.Helper()
	var sql string
	var vars []any
	const name = "zz_r30_capture_stats_sql"
	register := db.Callback().Query().After(hook).Register
	remove := db.Callback().Query().Remove
	if hook == "gorm:delete" {
		register = db.Callback().Delete().After(hook).Register
		remove = db.Callback().Delete().Remove
	}
	require.NoError(t, register(name, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != "job_stats" || sql != "" {
			return
		}
		sql = tx.Statement.SQL.String()
		vars = append([]any(nil), tx.Statement.Vars...)
	}))
	defer func() { _ = remove(name) }()
	run()
	require.NotEmpty(t, sql, "no job_stats statement was captured")
	return sql, vars
}

// The prefilter is only safe because its slop is at least as wide as the widest
// real clock face. A row on +14:00 renders a wall text 14 hours ABOVE its instant
// and one on -12:00 renders 12 hours below, so a slop narrower than that would let
// the bare clause REJECT a row the exact comparison admits — the one thing a
// prefilter may never do. Nothing else in this file can distinguish a 26h slop
// from a 1h one.
func TestStatsTimestampPrefilterSurvivesTheWidestClockFaces(t *testing.T) {
	s, _ := newStatsStorageForFaceTest(t)
	ctx := context.Background()

	mid := mustParseUTC(t, "2026-11-01T09:00:00Z")
	faces := map[string]*time.Location{
		"kiritimati+14": time.FixedZone("+14", 14*60*60),
		"gmt-12":        time.FixedZone("-12", -12*60*60),
		"utc":           time.UTC,
	}
	for q, loc := range faces {
		require.NoError(t, s.UpsertStatCounters(ctx, q, mid.In(loc).Truncate(time.Minute), 1, 0, 0))
	}

	got, err := s.GetStatsHistory(ctx, "", mid.Add(-time.Minute), mid.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, got, len(faces),
		"a row on an extreme clock face was rejected by the bare prefilter")

	// And the prune's own prefilter: a cutoff after them all must reap them all.
	deleted, err := s.PruneStats(ctx, mid.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(len(faces)), deleted)
}

// The dialect gate, asserted directly because this test matrix is SQLite-only and
// cannot reach the other two backends. julianday() and substr()-of-a-timestamp are
// SQLite spellings; Postgres and MySQL store a real instant and already compare
// instants, so they must keep the bare form — emitting the CASE there would fail
// the query outright, not merely mis-order it.
func TestStatsTimestampPredicate_NonSQLiteStaysBare(t *testing.T) {
	bound := mustParseUTC(t, "2026-11-01T09:00:00Z")
	for _, cmp := range []string{">=", "<=", "<"} {
		pred, args := statsTimestampPredicate(false, cmp, bound)
		require.Equal(t, "timestamp "+cmp+" ?", pred)
		require.Equal(t, []any{bound}, args)
	}
	// And on SQLite it is emphatically NOT bare, so the assertion above cannot be
	// passing because the function stopped doing anything.
	pred, args := statsTimestampPredicate(true, ">=", bound)
	require.Contains(t, pred, "julianday(")
	require.Len(t, args, 4, "the loose prefilter bind is missing")
}

// The prefilter must be loosened DOWNWARDS for a lower bound and UPWARDS for an
// upper one — the direction that can only admit more. A sign error here is
// invisible to a single-face fixture, so it is asserted on the binds themselves.
func TestStatsTimestampPredicate_PrefilterLoosensOutwards(t *testing.T) {
	bound := mustParseUTC(t, "2026-11-01T09:00:00Z")
	for _, tc := range []struct {
		cmp   string
		below bool
	}{{">=", true}, {"<=", false}, {"<", false}} {
		_, args := statsTimestampPredicate(true, tc.cmp, bound)
		require.Len(t, args, 4, "cmp %q lost its prefilter", tc.cmp)
		loose, ok := args[0].(time.Time)
		require.True(t, ok)
		if tc.below {
			require.True(t, loose.Before(bound), "cmp %q loosened the wrong way", tc.cmp)
		} else {
			require.True(t, loose.After(bound), "cmp %q loosened the wrong way", tc.cmp)
		}
		require.Equal(t, statsFaceSlop, loose.Sub(bound).Abs(),
			"cmp %q: the prefilter slop is not the full clock-face bound", tc.cmp)
	}
}
