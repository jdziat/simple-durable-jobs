package ui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// gormStatsStorage implements StatsStorage using GORM.
type gormStatsStorage struct {
	db *gorm.DB
}

// NewGormStatsStorage creates a GORM-backed stats storage.
func NewGormStatsStorage(db *gorm.DB) StatsStorage {
	return &gormStatsStorage{db: db}
}

func (s *gormStatsStorage) MigrateStats(ctx context.Context) error {
	if err := s.db.WithContext(ctx).AutoMigrate(&JobStat{}); err != nil {
		return err
	}
	return s.ensureTimestampIndex(ctx)
}

// jobStatsTimestampIndex indexes job_stats(timestamp).
//
// The table's only index is unique on (queue, timestamp), whose LEADING column
// is queue — so the retention prune (WHERE timestamp < ?) and the all-queues
// history read cannot use it and scanned the whole table, which grows by a row
// per queue per minute.
//
// pkg/storage carries the same index as versioned migration v38, which is the
// copy that holds the fleet lock and the one schema_migrations records. This
// copy exists for the case v38 structurally cannot cover: on a first-ever boot
// Migrate() runs before the dashboard is mounted, so job_stats does not exist
// yet, v38 correctly no-ops, and it is recorded as applied and never runs again.
//
// Racing creators are expected — this runs without the fleet lock — so a
// concurrent creator winning is not an error worth failing the mount over.
const jobStatsTimestampIndex = "idx_job_stats_timestamp"

func (s *gormStatsStorage) ensureTimestampIndex(ctx context.Context) error {
	if s.db.Migrator().HasIndex(&JobStat{}, jobStatsTimestampIndex) {
		return nil
	}
	return s.createTimestampIndex(ctx)
}

// createTimestampIndex issues the CREATE and tolerates losing the race.
//
// Split out from ensureTimestampIndex so the recovery is reachable from a test:
// through ensureTimestampIndex the leading HasIndex guard short-circuits before
// the CREATE, so no sequential test can ever enter this path.
func (s *gormStatsStorage) createTimestampIndex(ctx context.Context) error {
	err := s.db.WithContext(ctx).Exec(
		"CREATE INDEX " + jobStatsTimestampIndex + " ON job_stats (timestamp)",
	).Error
	if err != nil && s.db.Migrator().HasIndex(&JobStat{}, jobStatsTimestampIndex) {
		return nil // lost the race to a peer; the index is there, which is all we wanted
	}
	return err
}

func (s *gormStatsStorage) UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error {
	ts = ts.Truncate(time.Minute)

	for attempt := 0; attempt < 3; attempt++ {
		result := s.db.WithContext(ctx).Model(&JobStat{}).
			Where("queue = ? AND timestamp = ?", queue, ts).
			Updates(map[string]any{
				"completed": gorm.Expr("completed + ?", completed),
				"failed":    gorm.Expr("failed + ?", failed),
				"retried":   gorm.Expr("retried + ?", retried),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			return nil
		}

		insert := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&JobStat{
			Queue:     queue,
			Timestamp: ts,
			Completed: completed,
			Failed:    failed,
			Retried:   retried,
		})
		if insert.Error != nil {
			return insert.Error
		}
		if insert.RowsAffected == 1 {
			return nil
		}
	}

	return fmt.Errorf("upsert stat counters did not converge for queue %q at %s", queue, ts.Format(time.RFC3339))
}

func (s *gormStatsStorage) SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time, pending, running int64) error {
	ts = ts.Truncate(time.Minute)

	var existing JobStat
	result := s.db.WithContext(ctx).
		Where("queue = ? AND timestamp = ?", queue, ts).
		First(&existing)

	if result.Error == gorm.ErrRecordNotFound {
		return s.db.WithContext(ctx).Create(&JobStat{
			Queue:     queue,
			Timestamp: ts,
			Pending:   pending,
			Running:   running,
		}).Error
	}
	if result.Error != nil {
		return result.Error
	}

	return s.db.WithContext(ctx).Model(&existing).Updates(map[string]any{
		"pending": pending,
		"running": running,
	}).Error
}

// statsFaceSlop bounds how far a stored job_stats.timestamp's WALL-CLOCK text can
// sit from the same instant rendered in UTC. The widest offsets in tzdata are
// -12:00 and +14:00, so 14h is the true maximum; 26h is that with headroom. It is
// the same constant, for the same reason, as pkg/storage's
// maxStoredClockFaceOffset — read that one's godoc for the full derivation.
const statsFaceSlop = 26 * time.Hour

// statsTimestampPredicate returns a "timestamp <cmp> ?" predicate, plus its
// binds, that compares INSTANTS whatever clock face the stored value wears.
//
// # WHY NOT A BARE `timestamp >= ?`
//
// StatsCollector.snapshot and UpsertStatCounters write this column from the
// process's LOCAL wall clock (time.Now().Truncate(time.Minute)). SQLite has no
// datetime type: mattn/go-sqlite3 renders every time.Time as TEXT carrying its own
// offset ("2026-11-01 01:35:00-07:00") and SQLite compares that text LEXICALLY, so
// a bare bound orders WALL FACES, not instants.
//
// One process in ONE zone renders TWO faces across a DST fall-back. At 09:35Z in
// America/Los_Angeles, parsePeriod("1h") yields since="2026-11-01 01:35:00-07:00"
// and until="2026-11-01 01:35:00-08:00" — bounds that STRADDLE the fold — and a row
// written at 08:40Z renders "01:40:00-07:00", lexically ABOVE until, so it is
// dropped although its instant is squarely inside the window. Measured on the real
// fixture: 59 of 61 minute-buckets lost, i.e. the dashboard's Throughput chart
// reads near-zero for an hour in which the queue was working normally, silently.
// The same compare in the prune DELETED rows newer than its cutoff, which is data
// loss rather than a mis-read.
//
// # THE FORM, AND WHY THE LOOSE CLAUSE IS KEPT
//
// This is pkg/storage's timeBoundPredicate applied to a second column; that
// function's godoc carries the full derivation (why julianday() and not
// strftime(), why the bound is not simply re-faced, and the accepted 1ms limit on
// cross-face comparisons). The short form:
//
//   - same trailing offset on both sides -> compare the raw TEXT, which keeps the
//     driver's full precision and is the single-zone common case;
//   - different offsets -> julianday() parses each into the instant SQLite
//     computed for it, so the comparison is on instants for any stored face.
//
// Wrapping the column in CASE/julianday makes it a computed value, which costs
// SQLite the index RANGE restriction on idx_job_stats_timestamp — so a deliberately
// LOOSE bare clause is emitted first to restore it. It can admit rows the exact
// clause then rejects, but it can never REJECT a row the exact clause would admit:
// a row inside the window has wall = instant + offset, and |offset| <= 14h < 26h.
//
// MEASURED before choosing, on a temp-file SQLite database holding 357,120 rows
// (8 queues x 31 days x 1 row/min — the shipped retention default at a realistic
// queue count), ANALYZEd, mean of 5 runs after 2 warm-ups:
//
//	                              bare        CASE only     loose+CASE
//	read  queue=? + 24h window    10.7ms      23.1ms        5.5ms
//	read  all queues + 24h        85.3ms     172.4ms       95.8ms
//	prune count (115,200 rows)     8.4ms      62.6ms       25.5ms
//
//	plan  queue=? + window   bare, loose+CASE: SEARCH ... idx_job_stats_queue_ts
//	                           (queue=? AND timestamp>? AND timestamp<?)
//	                         CASE only:        ... (queue=?)        <- range LOST
//	plan  all queues         bare, loose+CASE: SEARCH ... idx_job_stats_timestamp
//	                           (timestamp>? AND timestamp<?)
//	                         CASE only:        SCAN job_stats       <- range LOST
//	plan  prune              bare, loose+CASE: SEARCH ... COVERING idx_job_stats_timestamp
//	                         CASE only:        SCAN job_stats       <- range LOST
//
// So the exact clause alone costs 2.0x-7.4x and loses every index range; with the
// loose prefilter the worst case is 3.0x on a background prune (8.4ms -> 25.5ms at
// 357k rows) and roughly parity on the dashboard read. That is the trade taken.
//
// The ORDER BY is deliberately NOT normalized — the same round measured that at
// 487-554x on an indexed sort. GetStatsHistory re-orders by instant in Go instead,
// over a window-bounded slice it has already materialized.
//
// Postgres and MySQL store a real instant and already compare instants, so they
// keep the plain form. That gate is not cosmetic: julianday() is a SQLite
// function and neither other backend has it, so emitting the CASE there would
// fail the query outright rather than mis-order it. It is asserted directly by
// TestStatsTimestampPredicate_NonSQLiteStaysBare, because the SQLite-only test
// matrix cannot reach it.
//
// cmp is one of ">=", "<=" or "<"; anything starting with ">" is treated as a
// lower bound for the purposes of which way the prefilter is loosened.
func statsTimestampPredicate(isSQLite bool, cmp string, bound time.Time) (string, []any) {
	// RANGE-CHECK FIRST, ON EVERY DIALECT — before the SQLite branch, not inside
	// it. MySQL's DATETIME ends at 9999-12-31 23:59:59, so an out-of-range bound
	// bound raw there returns zero rows or fails the query outright. That is the
	// same defect, in the same shape, that pkg/storage was fixed for; putting the
	// check inside the SQLite arm reintroduced it here.
	bound = statsRepresentableBound(bound)

	if !isSQLite {
		return "timestamp " + cmp + " ?", []any{bound}
	}

	// substr(x, -6) is the trailing "+HH:MM"/"-HH:MM" the driver always writes,
	// SIGN INCLUDED: "+05:30" and "-05:30" are 11 hours apart and must not share
	// the raw-text arm.
	exact := "CASE WHEN substr(timestamp, -6) = substr(?, -6) " +
		"THEN timestamp " + cmp + " ? " +
		"ELSE julianday(timestamp) " + cmp + " julianday(?) END"

	// A LOWER bound must be loosened downwards and an upper bound upwards, so the
	// prefilter can only ever admit more than the exact clause.
	slop := statsFaceSlop
	if strings.HasPrefix(cmp, ">") {
		slop = -statsFaceSlop
	}
	shifted := bound.UTC().Add(slop)
	// Drop the prefilter when the shift wraps int64 or leaves the four-digit year
	// band, where a lexical compare no longer tracks instant order. The exact
	// clause alone is already correct; only the index range is lost.
	moved := shifted.After(bound)
	if slop < 0 {
		moved = shifted.Before(bound)
	}
	if !moved || shifted.Year() < 1 || shifted.Year() > 9999 {
		return "(" + exact + ")", []any{bound, bound, bound}
	}
	return "(timestamp " + cmp + " ? AND " + exact + ")", []any{shifted, bound, bound, bound}
}

func (s *gormStatsStorage) isSQLite() bool {
	return s.db != nil && strings.Contains(strings.ToLower(s.db.Name()), "sqlite")
}

func (s *gormStatsStorage) GetStatsHistory(ctx context.Context, queue string, since time.Time, until time.Time) ([]JobStat, error) {
	var stats []JobStat
	sqlite := s.isSQLite()
	q := s.db.WithContext(ctx).Order("timestamp ASC")

	if queue != "" {
		q = q.Where("queue = ?", queue)
	}
	if !since.IsZero() {
		pred, args := statsTimestampPredicate(sqlite, ">=", since)
		q = q.Where(pred, args...)
	}
	if !until.IsZero() {
		pred, args := statsTimestampPredicate(sqlite, "<=", until)
		q = q.Where(pred, args...)
	}

	if err := q.Find(&stats).Error; err != nil {
		return nil, err
	}
	// `ORDER BY timestamp ASC` sorts SQLite's stored TEXT, which is a wall face,
	// so the two halves of a DST fall-back come back interleaved: the -08:00 01:00
	// bucket sorts before the -07:00 01:35 one though it is 35 minutes later. The
	// chart is a time series and would zig-zag.
	//
	// Re-order here rather than in SQL: normalizing an indexed ORDER BY measured
	// 487-554x last round, while this slice is bounded by the window the caller
	// asked for (60 rows for "1h", 10,080 for "7d" per queue) and is already
	// materialized. Stable, so equal instants keep the database's order. A no-op on
	// Postgres and MySQL, where the rows arrive instant-ordered already.
	sort.SliceStable(stats, func(i, j int) bool {
		return stats[i].Timestamp.Before(stats[j].Timestamp)
	})
	return stats, nil
}

func (s *gormStatsStorage) PruneStats(ctx context.Context, before time.Time) (int64, error) {
	pred, args := statsTimestampPredicate(s.isSQLite(), "<", before)
	result := s.db.WithContext(ctx).Where(pred, args...).Delete(&JobStat{})
	return result.RowsAffected, result.Error
}

// statsRepresentableBoundCeil / Floor are the widest instants every supported
// backend can hold AND compare. They are deliberately the same values as
// pkg/storage's representableBoundCeil / Floor; package ui cannot import that
// unexported helper, so the rule is restated rather than shared.
//
// The ceiling stops at millisecond .999 because SQLite rounds the seconds
// fraction into its millisecond julian-day integer, and anything from
// '…23:59:59.9995' upward rounds past the end of the supported range and yields
// NULL — measured, with '…59.9994' still parsing. MySQL independently needs the
// same ceiling: DATETIME ends at 9999-12-31 23:59:59.
var (
	statsRepresentableBoundFloor = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	statsRepresentableBoundCeil  = time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC)
)

// statsRepresentableBound clamps a bound into the range the backend can store and
// compare.
//
// IT COMPARES THE INSTANT, NOT THE YEAR, and that distinction is the whole point.
// An earlier version tested `bound.Year() > 9999`, which every instant in the
// final half-millisecond of year 9999 passes — including timestamppb's maximum,
// 9999-12-31T23:59:59.999999999Z, which is exactly the sentinel this repo already
// treats as the canonical open-ended upper bound. The clamp never fired for it,
// julianday(?) went NULL, and `julianday(timestamp) <= NULL` dropped every row
// whose stored clock face differed from the bound's: half the throughput buckets
// silently missing, and a "prune everything" cutoff that no-ops on most rows.
//
// It CLAMPS rather than returning an empty predicate. pkg/storage can safely
// return "" for a bound that excludes nothing, because all of its callers are
// SELECT filters where the failure is over-inclusion. This predicate is also
// handed to PruneStats' DELETE, where "excludes nothing" would mean DELETE EVERY
// ROW. Clamping expresses the same intent for both and cannot turn a filter into
// a table wipe.
func statsRepresentableBound(bound time.Time) time.Time {
	// RE-FACE FIRST: an offset beyond ±14:00 is one SQLite's own date parser
	// refuses outright, so "…+15:00" makes julianday() NULL for an instant that is
	// otherwise perfectly ordinary. Re-facing is INSTANT-PRESERVING, so it changes
	// no row's membership.
	if _, offsetSeconds := bound.Zone(); absStatsDuration(time.Duration(offsetSeconds)*time.Second) > statsMaxParsableFaceOffset {
		bound = bound.UTC()
	}

	switch {
	case bound.After(statsRepresentableBoundCeil):
		return statsRepresentableBoundCeil
	case bound.Before(statsRepresentableBoundFloor):
		return statsRepresentableBoundFloor
	}

	// AND RE-FACE AGAIN AFTER THE CLAMP. The instant is inside the representable
	// range, but every comparison it feeds is against its RENDERED WALL, and
	// wall = instant + offset. A positive face pushes an instant inside the LAST
	// 14 HOURS of year 9999 into a five-digit year: the ceil on a +05:30 face
	// renders "10000-01-01 05:29:59.999+05:30". The instant checks above cannot
	// see that — it is not After the ceil — and the offset re-face only fires
	// beyond ±14:00. Both lexical arms then invert ("10000-" sorts BELOW "2026-")
	// and julianday() returns NULL, so an upper bound at the end of time returns
	// an EMPTY page instead of everything.
	//
	// Only the HIGH side needs this: a negative face moves the wall EARLIER, and
	// the earliest reachable wall is floor on -14:00 — still four digits, still
	// below every real row. That asymmetry is why the symmetric-looking instant
	// guard hid it.
	if bound.Year() > 9999 {
		bound = bound.UTC()
	}
	return bound
}

// statsMaxParsableFaceOffset is the widest timezone suffix SQLite's date parser
// accepts (measured in pkg/storage: '+15:00' -> NULL, '+14:00' and '-12:00'
// parse). Same value and same reason as pkg/storage's sqliteMaxParsableFaceOffset.
const statsMaxParsableFaceOffset = 14 * time.Hour

func absStatsDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
