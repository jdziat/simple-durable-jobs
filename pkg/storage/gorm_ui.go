package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
)

const (
	maxUISearchLength = 256
	maxUIQueryLimit   = 200
)

// GetQueueStats returns per-queue job counts grouped by status.
func (s *GormStorage) GetQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	return s.GetQueueDepthStats(ctx)
}

// GetQueueDepthQueueOnly returns per-queue PENDING and RUNNING counts only.
//
// It exists because GetQueueDepthStats is the wrong query for a caller that only
// wants depth: that one groups over EVERY status with no WHERE, so it is
// unbounded in table size — and the default retention keeps completed jobs for 30
// days, making "a few pending, millions completed" the normal shape. Measured on
// live databases at 300k jobs (a few thousand pending, the rest completed), the
// unfiltered form is a parallel seq scan on Postgres — 4,935 buffers, 32ms — and
// on MySQL a full scan of idx_jobs_dequeue_eligible, the index the claim path
// depends on, evicting exactly the buffer-pool pages the dequeue needs.
// Restricting to the two statuses read gives an index scan: 609 buffers, 0.5ms
// with the live rows clustered at the end of the heap, which is the common shape.
// Spread the same 300k through the heap instead and it measures ~3,000 buffers /
// ~12ms — still bounded by LIVE work rather than table size, which is the property
// that matters, but the headline figure is a best case.
//
// The win is that the cost becomes bounded by queue DEPTH rather than by TABLE
// SIZE, which is what matters when the default retention keeps 30 days of
// completed rows. It is not a guarantee of an index scan in every shape: under a
// genuinely large LIVE backlog the planner can still choose a sequential scan on
// Postgres, and MySQL range-scans the claim index — but both are then
// proportional to the backlog the operator actually has, not to history.
//
// It also touches only the jobs table. GetQueueDepthStats additionally reads
// queue_states for pause flags and propagates that error, so an unrelated failure
// there would blank a depth sample that was perfectly readable.
func (s *GormStorage) GetQueueDepthQueueOnly(ctx context.Context) (map[string][2]int64, error) {
	type row struct {
		Queue  string
		Status string
		Count  int64
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, status, count(*) as count").
		Where("status IN ?", []string{string(core.StatusPending), string(core.StatusRunning)}).
		Group("queue, status").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	depth := make(map[string][2]int64, len(rows))
	for _, r := range rows {
		d := depth[r.Queue]
		switch core.JobStatus(r.Status) {
		case core.StatusPending:
			d[0] = r.Count
		case core.StatusRunning:
			d[1] = r.Count
		}
		depth[r.Queue] = d
	}
	return depth, nil
}

// GetQueueDepthStats returns accurate per-queue depth counts using aggregate
// queries instead of fetching job rows.
func (s *GormStorage) GetQueueDepthStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	if s.hotStats == nil { // zero-value storage: bypass the cache
		return s.getQueueDepthStats(ctx)
	}
	return s.hotStats.queueDepth.do(ctx, s.hotStatsTTLValue(), cloneQueueStatsSlice, s.getQueueDepthStats)
}

func (s *GormStorage) getQueueDepthStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	type row struct {
		Queue  string
		Status string
		Count  int64
	}
	var rows []row
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Select("queue, status, count(*) as count").
		Group("queue, status").
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	statsMap := make(map[string]*jobsv1.QueueStats)
	for _, r := range rows {
		qs, ok := statsMap[r.Queue]
		if !ok {
			qs = &jobsv1.QueueStats{Name: r.Queue}
			statsMap[r.Queue] = qs
		}
		switch core.JobStatus(r.Status) {
		case core.StatusPending:
			qs.Pending += r.Count
		case core.StatusRunning:
			qs.Running += r.Count
		case core.StatusCompleted:
			qs.Completed += r.Count
		case core.StatusFailed:
			qs.Failed += r.Count
		case core.StatusPaused:
			qs.Paused += r.Count
		case core.StatusRetrying:
			qs.Retrying += r.Count
		case core.StatusWaiting:
			qs.Waiting += r.Count
		case core.StatusCancelled:
			qs.Cancelled += r.Count
		}
	}

	// Share the metrics loader instead of repeating the aggregate here. The
	// dashboard and the jobs.backlog.oldest_age gauge used to run two independent
	// copies of the same MIN(created_at), and both copies carried the same defect:
	// a scheduled job aged the backlog from when it was CREATED rather than from
	// when it became due, so a single job due next month pinned this card at "a
	// month old" forever. One loader means a fix cannot land in only one of them.
	oldestByQueue, err := s.queueOldestPendingAt(ctx)
	if err != nil {
		return nil, err
	}
	for queueName, oldestPendingAt := range oldestByQueue {
		qs, ok := statsMap[queueName]
		if !ok {
			qs = &jobsv1.QueueStats{Name: queueName}
			statsMap[queueName] = qs
		}
		qs.OldestPendingAt = timestamppb.New(oldestPendingAt)
	}

	// Check which queues are paused. Surface a failed pause-state read rather than
	// silently rendering every queue as UNPAUSED — a paused/quarantined queue shown
	// as draining is a safety-relevant lie on the dashboard. Both callers
	// (GetStats, ListQueues) already propagate this error.
	pausedQueues, err := s.GetPausedQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("read paused queues: %w", err)
	}
	pausedSet := make(map[string]struct{}, len(pausedQueues))
	for _, q := range pausedQueues {
		pausedSet[q] = struct{}{}
	}

	result := make([]*jobsv1.QueueStats, 0, len(statsMap))
	for _, qs := range statsMap {
		if _, ok := pausedSet[qs.Name]; ok {
			qs.IsPaused = true
		}
		result = append(result, qs)
	}
	return result, nil
}

// parseDBTimestamp accepts the timestamp strings returned when aggregate
// expressions are scanned through sql.NullString: pgx/MySQL convertAssign use
// T-separated RFC3339, while SQLite returns a space-separated value with offset.
func parseDBTimestamp(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
	} {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

// CountActiveWorkers returns distinct workers currently holding running jobs.
func (s *GormStorage) CountActiveWorkers(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("status = ? AND locked_by <> ?", core.StatusRunning, "").
		Select("COUNT(DISTINCT locked_by)").
		Count(&count).Error
	return count, err
}

// SearchJobs returns jobs matching the filter with pagination and total count.
func (s *GormStorage) SearchJobs(ctx context.Context, filter core.JobFilter) ([]*core.Job, int64, error) {
	q := s.db.WithContext(ctx).Model(&core.Job{})

	if filter.Status != "" {
		q = q.Where("status = ?", filter.Status)
	}
	if filter.Queue != "" {
		q = q.Where("queue = ?", filter.Queue)
	}
	if filter.Type != "" {
		q = q.Where("type = ?", filter.Type)
	}
	if filter.Tenant != "" {
		q = q.Where("tenant = ?", filter.Tenant)
	}
	q = applyMetaContains(s, q, filter.MetaContains)
	q = applyJobSearch(s, q, filter.Search)
	q = applyTimeWindow(s, q, "created_at", filter.Since, filter.Until)

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var jobs []*core.Job
	limit, offset := clampUIPagination(filter.Limit, filter.Offset)
	err := q.Order(jobSortOrder(filter)).
		Offset(offset).
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, 0, err
	}

	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

// maxStoredClockFaceOffset bounds how far a stored timestamp's WALL-CLOCK text
// can sit from the same instant expressed in UTC.
//
// On SQLite a timestamp is TEXT carrying its own offset ("2026-08-01
// 09:30:08.357431592-07:00"), so a lexical comparison orders rows by their WALL
// FACE, not by instant. wall = instant + offset, and the widest real offsets in
// tzdata are -12:00 (Etc/GMT+12) and +14:00 (Pacific/Kiritimati), so a row's wall
// text can never be more than 14h away from its UTC rendering in either
// direction. 26h is that maximum with 12h of headroom.
//
// It exists so timeBoundPredicate can keep a BARE-COLUMN range clause alongside
// the exact predicate: the bare clause is deliberately loose (it can admit rows
// the exact clause then rejects) but it can never REJECT a row the exact clause
// would admit, and it is the only form SQLite can use to restrict an index range
// (measured — see timeBoundPredicate).
//
// The one input it cannot cover is a row whose stored offset exceeds ±26:00. No
// tzdata zone produces one; it would take a hand-built time.FixedZone passed as
// an explicit Job.CreatedAt. Such a row is already beyond repair anyway: SQLite's
// own date parser hard-caps a timezone suffix at ±14:00 hours, so julianday(),
// strftime() and datetime() all return NULL for it (measured: '...+15:00' ->
// NULL, '...+14:00' -> parses). No read-side form can normalize a face SQLite
// refuses to read.
const maxStoredClockFaceOffset = 26 * time.Hour

// timeBoundDirection selects which side of a time window a predicate expresses.
type timeBoundDirection int

const (
	// boundAtOrAfter is the INCLUSIVE lower bound: column >= bound.
	boundAtOrAfter timeBoundDirection = iota
	// boundAtOrBefore is the INCLUSIVE upper bound: column <= bound.
	boundAtOrBefore
)

// timeBoundPredicate returns a "column <cmp> bound" predicate, plus its binds,
// that compares INSTANTS whatever clock face the stored value happens to wear.
// Both directions are INCLUSIVE, which is what core.JobFilter.Since/Until and
// core.DeadLetterFilter.DeadLetteredSince/Until document.
//
// # WHY THIS IS NOT A BARE `column >= ?`
//
// SQLite has no datetime type. mattn/go-sqlite3 renders every bound time.Time
// with "2006-01-02 15:04:05.999999999-07:00" — on whatever face the value
// carries, and a UTC value renders "+00:00", never "Z" — and SQLite compares it
// against the stored TEXT LEXICALLY. A lexical comparison of two differently
// offset strings orders WALL FACES, not instants, and is wrong by the full delta
// between the two zones. This is the same hazard clock.go documents for
// nowWriteValue and gorm.go's scheduleCursorLess, and it is not hypothetical
// here: timestamppb.AsTime is unconditionally UTC, so every bound arriving
// through the ListJobs RPC is UTC-faced.
//
// # WHY THE BOUND IS NOT SIMPLY CONVERTED TO ONE FACE
//
// Converting the bind (e.g. bound.Local()) is only correct when every stored row
// wears the READER's face. Rows do not:
//
//   - created_at is GORM autoCreateTime from time.Now(), so it wears the face of
//     the process that WROTE it. `sdj --driver sqlite --dsn ./jobs.db` and the
//     standalone UI binaries are documented second processes against one SQLite
//     file, and they need not share a TZ with the worker. Measured: normalizing
//     the bind to Local fixes reader==writer and BREAKS writer=UTC/reader!=UTC,
//     which the unfixed code got right.
//   - A single zone still writes TWO faces across a DST transition (-07:00 and
//     -08:00 for America/Los_Angeles). Whichever face the bind picks is wrong for
//     rows on the other one, inside the fall-back hour, in one process.
//   - dead_lettered_at is written on ONE face NOW (nowWriteValue — see
//     deadLetterOrderColumn), but rows written by earlier releases carry the same
//     mixture, so this predicate still has to serve them.
//
// Normalizing on the WRITE path instead (what bff9da0 did for run_at, and what
// dead_lettered_at now does) is right for a column with in-package writers and no
// legacy comparison to honour. It is not sufficient HERE: it leaves every
// already-stored row on its original face, and no write-side choice can make one
// bind face correct for a column that legitimately holds several. It is also not
// available to created_at at all — see jobSortOrder for why the dequeue fence
// pins that column to the local face.
//
// # THE FORM, AND WHY IT IS THE ONE ALREADY IN THIS PACKAGE
//
// The exact half is scheduleCursorLess (gorm.go) applied to a different column:
//
//   - Same trailing offset on both sides -> compare the raw TEXT. Two identically
//     offset strings sort by instant, and this keeps the driver's full
//     NANOSECOND precision. This is the common case (one process, one zone, no
//     DST crossing).
//   - Different offsets -> julianday() parses each into the instant SQLite
//     computed for it, so the comparison is on instants for ANY stored face, with
//     no migration and no dependence on which process wrote the row. Applying it
//     unconditionally is what an earlier version of scheduleCursorLess did with
//     the equivalent expression, and it measurably truncated precision
//     (datetime() is worse still: whole seconds).
//
// # WHY julianday() AND NOT strftime()
//
// This branch used to normalize both sides to TEXT with
// strftime('%Y-%m-%d %H:%M:%f', …), on the stated premise — repeated on two public
// godocs — that "strftime('%f') renders a given instant identically on every
// face". That premise is FALSE, and the predicate DROPPED rows because of it.
//
// SQLite parses a timestamp into a struct holding both the raw Y/M/D h:m:s it read
// and a millisecond integer iJD computed from them. A NON-ZERO offset has to be
// applied to iJD, which invalidates the raw fields, so every component is
// re-rendered from the ROUNDED iJD. A ZERO offset — which is exactly what the
// driver writes for a UTC value, "+00:00" — leaves the raw fields valid, so the
// clock and the seconds print as parsed. Measured, the two renderings disagree in
// three bands:
//
//	minute tail   ss.9995+ : zero offset clamps to :59.999, non-zero rolls the
//	                         minute to :00.000                      (1ms apart)
//	half-ms tie   ss.0025  : zero offset prints .002, non-zero .003 (1ms apart)
//	DAY tail                 the last 0.5ms of a day whose day-of-month is >= 29:
//	                         the rounded iJD advances the DATE while the raw clock
//	                         still prints 23:59:59.999, so
//	                         "2026-12-31 23:59:59.9995+00:00" renders
//	                         "2027-01-01 23:59:59.999"       (nearly 24h apart)
//
// A row sitting on an inclusive bound in any of those bands was lost — an empty
// page from ListJobs, an under-count from CountDeadLettered. The day-tail band is
// also why adding a millisecond of slack to the bound is not the fix: the error
// there is not bounded by a millisecond.
//
// julianday() is iJD/86400000.0. iJD is built by ONE arithmetic path — date to
// milliseconds, plus h/m/s with a single rounding of the seconds, minus a
// whole-minute offset — so it cannot depend on the face, and nothing is re-derived
// from raw fields afterwards. Measured face-independent over 840,042 comparisons
// (7 faces x 6 anchors x 20,001 sub-second offsets, years 1900-9998, under both
// TZ=UTC and TZ=America/Los_Angeles), with identical NULL behaviour to strftime on
// every out-of-range input this file already guards. It is a float64, but adjacent
// milliseconds stay ~10 ULPs apart across the whole representable range, so
// ordering survives; that is asserted, not assumed.
//
// # THE ACCEPTED LIMIT: CROSS-FACE COMPARISONS RESOLVE TO MILLISECONDS
//
// iJD is a millisecond integer, so the cross-face branch — and ONLY that branch —
// compares at millisecond resolution: a row and a bound that wear DIFFERENT
// offsets and sit inside one millisecond collapse and compare EQUAL, so an
// inclusive bound can admit a row up to 1ms outside the window. The error is
// bounded by 1ms and is over-inclusion in every case; the predicate never drops a
// row that belongs in the window. The same-face fast path is exact to the
// nanosecond, and same-face is what a single-zone deployment always takes, so this
// needs a genuinely mixed-face column AND a sub-millisecond window bound to reach.
//
// This is a deliberate trade, not an oversight: the alternative is normalizing
// every comparison, which costs the same 1ms on the common path too. It is pinned
// by TestSearchJobs_CrossFaceWindowResolvesToMilliseconds so it cannot silently
// widen, and it is restated on the public ui.JobFilter and core.DeadLetterFilter
// godoc where a caller reads it.
//
// The "never drops" half of that claim is the load-bearing one, and the two tests
// that pin it are chosen so neither can go inert:
// TestTimeBoundPredicate_CrossFaceNormalizationIsFaceIndependent measures the
// premise directly (and keeps strftime's divergence as a live control, so a revert
// to text cannot pass), and
// TestSearchJobs_CrossFaceBoundExactlyOnARowIsNeverDropped puts a bound EXACTLY on
// a row inside each band across the full writer-face x reader-face matrix. A
// window that brackets its row by a few milliseconds cannot distinguish a correct
// predicate from one whose error is smaller than the bracket — which is precisely
// how the false premise survived review. Read both before weakening this.
//
// A migration that rewrote stored timestamp text was tried in this repo and
// corrupted ordering on every SQLite database. This is read-side only.
//
// # THE BARE-COLUMN HALF, AND WHAT IT BUYS
//
// julianday()/CASE around the column makes it a computed value, which costs
// SQLite the index RANGE restriction. created_at IS indexed — the migrations
// create idx_jobs_status_created (status, created_at DESC) and
// idx_jobs_queue_created (queue, created_at DESC) — and the dead-letter view has
// idx_jobs_dead_lettered_at.
//
// Measured with EXPLAIN QUERY PLAN and wall-clock timings against the real
// migrated schema, 200k rows over ~139 days, 8 queues, ANALYZEd, one-day window,
// mean of 5 runs after 2 warm-ups:
//
//	                        bare (pristine)   CASE only    loose+CASE
//	count  queue=?+window        17us          4.443ms       133us
//	list   queue=?+window        22us          2.890ms        96us
//	count  status=?+window       54us         63.062ms       905us
//	list   status=?+window       28us         35.963ms       416us
//	count  window only           55us         56.889ms       959us
//
//	plan   queue=?+window   bare, loose+CASE: SEARCH jobs USING [COVERING] INDEX
//	                          idx_jobs_queue_created
//	                          (queue=? AND created_at>? AND created_at<?)
//	                        CASE only:        ... (queue=?)            <- range LOST
//	plan   status=?+window  bare, loose+CASE: SEARCH jobs USING [COVERING] INDEX
//	                          idx_jobs_status_created
//	                          (status=? AND created_at>? AND created_at<?)
//	                        CASE only:        ... (status=?)           <- range LOST
//	plan   window only      bare, loose+CASE: SEARCH ... idx_jobs_queue_created
//	                          (ANY(queue) AND created_at>? AND created_at<?)
//	                        CASE only:        SCAN jobs USING COVERING INDEX
//
// So the loose bare clause is emitted FIRST and restores every range restriction
// the CASE alone loses; the CASE then runs as a residual filter over the narrowed
// range. Dropping it turns a 0.9ms dashboard count into a 63ms one at 200k rows,
// and that cost grows with the TABLE while the loose form's grows with the WINDOW.
// The plan is asserted by TestSearchJobs_WindowKeepsTheIndexRangeRestriction, not
// left as a claim in this comment.
//
// # WHY THE BOUND IS RANGE-CHECKED FIRST
//
// Every one of the three binds above is only meaningful inside the range SQLite
// can actually work with, and a bound outside it fails SILENTLY — as a dropped
// row, not as an error:
//
//   - Both comparisons are LEXICAL, and lexical order tracks instant order only
//     while the year has FOUR digits. Go's "2006" verb pads but does not truncate,
//     so year 10000 renders "10000-…", which sorts BELOW every real "2026-…" row.
//     On the upper bound that inverts the comparison and rejects EVERYTHING.
//   - julianday() returns NULL for anything SQLite's date parser refuses, and
//     `NULL <= x` is NULL, which is not true, so the row is dropped. Measured, it
//     refuses two things a caller can supply: a value that rounds past the end of
//     its supported range ('9999-12-31 23:59:59.999999999' -> NULL, while
//     '…59.999' parses) and a timezone suffix beyond ±14:00 ('+15:00' -> NULL,
//     '+14:00' parses). strftime() refuses exactly the same inputs, so swapping
//     the normalizer changed nothing here — re-measured, not assumed.
//
// Both are reachable from the public API. timestamppb.IsValid accepts
// 9999-12-31T23:59:59.999999999Z, which is the natural "no upper bound" sentinel
// for a programmatic Connect client, and it hits BOTH: it rounds to NULL under
// julianday and it overflows the four-digit year once the ±26h prefilter slop is
// added. Unpatched, that request returns rows; a naive fix returns an empty page.
//
// So the bound is normalized before any SQL is built, always in the direction
// that can only ADMIT more rows, never fewer:
//
//	face beyond ±14:00      -> re-faced to UTC. Instant-preserving, and no row
//	                           SQLite can read wears such a face either, so the
//	                           same-face fast path is unaffected.
//	past the far end in the -> the bound restricts nothing storable: emit NO
//	  direction that would    predicate at all (an absent bound), which is what
//	  admit everything        an open-ended sentinel means.
//	past the far end in the -> clamped to that end. It can then admit a row
//	  other direction         exactly at the end that should have been excluded;
//	                          over-inclusion, never a lost row.
//	±26h prefilter shift    -> checked separately: when the shift leaves the
//	  leaves the year band     four-digit band (or wraps int64), the loose clause
//	                          is OMITTED for that side. It is a pure prefilter, so
//	                          dropping it costs an index range and nothing else.
//
// Pinned by TestTimeBoundPredicate_BoundsOutsideSQLitesRangeDegradeSafely and
// TestSearchJobs_OpenEndedFarFutureUntilKeepsEveryRow.
//
// Postgres and MySQL store a real instant and already compare instants, so past
// the shared range check they keep the plain form with the caller's time.Time
// untouched. They are NOT exempt from that check: an earlier version of this
// comment claimed they were unaffected because the SQLite branch gated them out,
// and live MySQL disproved it — DATETIME ends at 9999-12-31 23:59:59.
//
// An empty predicate means "no restriction on this side"; applyTimeWindow skips it.
func (s *GormStorage) timeBoundPredicate(column string, dir timeBoundDirection, bound time.Time) (string, []any) {
	cmp := ">="
	slop := -maxStoredClockFaceOffset
	if dir == boundAtOrBefore {
		cmp = "<="
		slop = maxStoredClockFaceOffset
	}
	// RANGE-CHECK FIRST, ON EVERY DIALECT. An open-ended sentinel bound is not a
	// SQLite concern: MySQL's DATETIME tops out at 9999-12-31 23:59:59, so binding
	// a protobuf-max `until` there returns ZERO rows and a year-10000 one fails the
	// query outright. Verified on live MySQL — both were red before this moved.
	bound, restricts := representableBound(bound, dir)
	if !restricts {
		return "", nil
	}

	if !s.isSQLite {
		return column + " " + cmp + " ?", []any{bound}
	}

	// julianday(), NOT strftime() — see "THE ACCEPTED LIMIT" above. strftime
	// renders one instant DIFFERENTLY depending on whether its offset is zero, so
	// normalizing through text drops rows; julianday is computed from the parsed
	// instant and is face-independent.
	const instant = `julianday(`
	// substr(x, -6) is the trailing "+HH:MM" / "-HH:MM" the driver always writes,
	// SIGN INCLUDED — "+05:30" and "-05:30" are 11 hours apart and must not take
	// the raw-text arm together.
	exact := "CASE WHEN substr(" + column + ", -6) = substr(?, -6) " +
		"THEN " + column + " " + cmp + " ? " +
		"ELSE " + instant + column + ") " + cmp + " " + instant + "?) END"

	loose, ok := lexicalPrefilterBound(bound, slop)
	if !ok {
		return "(" + exact + ")", []any{bound, bound, bound}
	}
	return "(" + column + " " + cmp + " ? AND " + exact + ")", []any{loose, bound, bound, bound}
}

// representableBoundFloor / representableBoundCeil are the widest instants EVERY
// supported backend can hold and compare, so they bound the predicate on all
// three dialects rather than on SQLite alone.
//
// SQLite sets the shape: they are the widest instants whose driver-rendered text
// BOTH keeps a four-digit year and is accepted by SQLite's date parser, and the
// ceiling stops at millisecond .999 because SQLite rounds the fraction into its
// millisecond julian-day integer and '…59.9999' rounds past the end of the
// supported range, yielding NULL from julianday() and strftime() alike (measured
// on both).
//
// MySQL independently needs the SAME ceiling: DATETIME ends at
// 9999-12-31 23:59:59, so a protobuf-max `until` bound matched ZERO rows and a
// year-10000 one failed the query. Postgres reaches year 294276 and so is never
// constrained by these — but omitting a bound no stored row could exceed is
// over-inclusive there, which is the safe direction.
var (
	representableBoundFloor = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	representableBoundCeil  = time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC)
)

// sqliteMaxParsableFaceOffset is the widest timezone suffix SQLite's own date
// parser accepts (measured: '+15:00' -> NULL, '+14:00' and '-12:00' parse). It is
// also wider than any tzdata zone, so only a hand-built time.FixedZone reaches it.
const sqliteMaxParsableFaceOffset = 14 * time.Hour

// representableBound moves a bound into the range the BACKEND can store and
// compare, reporting restricts=false when the bound excludes nothing the backend
// could hold — in which case the caller must emit no predicate rather than one
// that would silently reject every row, or fail the query outright.
//
// Every adjustment is in the over-inclusive direction: this function can widen a
// window, never narrow it.
func representableBound(bound time.Time, dir timeBoundDirection) (time.Time, bool) {
	if _, offsetSeconds := bound.Zone(); absDuration(time.Duration(offsetSeconds)*time.Second) > sqliteMaxParsableFaceOffset {
		bound = bound.UTC()
	}
	switch {
	case dir == boundAtOrAfter && bound.Before(representableBoundFloor):
		return time.Time{}, false // "at or after the dawn of time" restricts nothing
	case dir == boundAtOrBefore && bound.After(representableBoundCeil):
		return time.Time{}, false // the open-ended upper sentinel restricts nothing
	case bound.Before(representableBoundFloor):
		return representableBoundFloor, true
	case bound.After(representableBoundCeil):
		return representableBoundCeil, true
	}
	// The instant is inside the representable range, but every comparison this
	// bound feeds is against its RENDERED WALL TEXT, and wall = instant + offset.
	// A positive face pushes an instant inside the LAST 14 HOURS of year 9999 into
	// a five-digit year: representableBoundCeil.In(+05:30) renders
	// "10000-01-01 05:29:59.999+05:30". The instant checks above cannot see that —
	// it is not After the ceil — and the offset re-face above only fires beyond
	// ±14:00. Both lexical arms then invert ("10000-" sorts BELOW "2026-") and
	// julianday() returns NULL for a year-10000 text, so an upper bound at the end
	// of time returned an EMPTY page instead of everything.
	//
	// Re-facing to UTC is INSTANT-PRESERVING — it changes no row's membership,
	// unlike a clamp — and a UTC rendering of any instant in [floor, ceil] is
	// always a four-digit year by construction, because floor and ceil are
	// themselves defined on UTC.
	//
	// Only the HIGH side needs this. A negative face moves the wall EARLIER, and
	// the earliest reachable wall is floor on a -14:00 face — "0000-12-31 …",
	// still four digits, still sorting below every real row and still parsed by
	// julianday. Anything below that is Before(floor) and was already clamped.
	// That asymmetry is why the symmetric-looking instant guard hid this.
	if bound.Year() > 9999 {
		bound = bound.UTC()
	}
	return bound, true
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// lexicalPrefilterBound shifts bound by slop for the index-preserving bare-column
// prefilter, reporting ok=false when the result can no longer be compared
// LEXICALLY against stored timestamp text — i.e. when the ±26h shift pushes an
// otherwise-valid bound out of the four-digit year band. Callers then drop the
// prefilter; the exact predicate alone is already correct, so only the index range
// is lost, and a bound 8000 years out was never restricting a useful range.
//
// The wrap check covers a bound so extreme that Add overflows the internal int64
// second count, which would otherwise move the bound in the WRONG direction.
func lexicalPrefilterBound(bound time.Time, slop time.Duration) (time.Time, bool) {
	shifted := bound.UTC().Add(slop)
	if slop > 0 && !shifted.After(bound) {
		return time.Time{}, false
	}
	if slop < 0 && !shifted.Before(bound) {
		return time.Time{}, false
	}
	if year := shifted.Year(); year < 1 || year > 9999 {
		return time.Time{}, false
	}
	return shifted, true
}

// applyTimeWindow adds the INCLUSIVE [since, until] bounds for one timestamp
// column. A zero value means "no bound" on that side, and so does an empty
// predicate — timeBoundPredicate returns one for a bound that excludes nothing
// the backend can store (see representableBound).
//
// The `pred != ""` guards are for the READER, not for GORM: `Where("")` is a
// silent no-op that returns a nil error (measured, gorm v1.31.1 — an earlier
// version of this comment claimed it raises "empty condition", which is false).
// They are kept because skipping a bound is the INTENT and an unconditional
// Where would leave that intent resting on an undocumented GORM behaviour. They
// are deliberately unpinnable: no test can distinguish them from their absence,
// which is exactly why the reason lives here instead of in a test name.
//
// column is a package-internal literal at every call site, never caller data.
func applyTimeWindow(s *GormStorage, q *gorm.DB, column string, since, until time.Time) *gorm.DB {
	if !since.IsZero() {
		if pred, args := s.timeBoundPredicate(column, boundAtOrAfter, since); pred != "" {
			q = q.Where(pred, args...)
		}
	}
	if !until.IsZero() {
		if pred, args := s.timeBoundPredicate(column, boundAtOrBefore, until); pred != "" {
			q = q.Where(pred, args...)
		}
	}
	return q
}

func applyMetaContains(s *GormStorage, q *gorm.DB, m *core.MetadataMap) *gorm.DB {
	if m == nil || len(*m) == 0 {
		return q
	}
	switch s.dialect() {
	case dialectPostgres:
		jsonBytes, err := json.Marshal(*m)
		if err != nil {
			_ = q.AddError(fmt.Errorf("marshal metadata contains: %w", err))
			return q
		}
		return q.Where("(NULLIF(metadata, '')::jsonb) @> ?::jsonb", string(jsonBytes))
	case dialectMySQL:
		declared := s.indexedMetadataKeyList()
		indexed := make(map[string]struct{}, len(declared))
		for _, key := range declared {
			if indexedMetadataKeyPattern.MatchString(key) {
				indexed[key] = struct{}{}
			}
		}
		// Route a declared key to its STORED generated column (meta_<key>, indexed)
		// when the filter value fits the VARCHAR(255) width; longer values would be
		// truncated in the column and could false-match, so they fall back to the
		// untruncated JSON_CONTAINS. This relies on metadata values being JSON
		// strings: core.Job.Metadata is map[string]string serialized via
		// serializer:json, so the gencol (JSON_VALUE ... RETURNING CHAR) and
		// JSON_CONTAINS agree. A non-string JSON value could only be injected via
		// raw out-of-band SQL, which is outside the library's contract.
		fallback := make(map[string]string, len(*m))
		for key, value := range *m {
			if _, ok := indexed[key]; ok && len(value) <= 255 {
				q = q.Where(mysqlMetadataGenColumn(key)+" = ?", value)
				continue
			}
			fallback[key] = value
		}
		if len(fallback) == 0 {
			return q
		}
		// JSON_CONTAINS is canonical containment (matches PG @>, unlike
		// substring LIKE which can false-match). MySQL has no general
		// JSON-containment index (PG GIN has no MySQL equivalent —
		// multi-valued indexes are per-array-path only) so this remains a
		// correct full scan, acceptable because metadata search is a
		// dashboard/admin cold path not the dequeue hot path. The metadata <>
		// empty-string guard prevents JSON_CONTAINS erroring on empty rows.
		jsonBytes, err := json.Marshal(fallback)
		if err != nil {
			_ = q.AddError(fmt.Errorf("marshal metadata contains: %w", err))
			return q
		}
		return q.Where("metadata IS NOT NULL AND metadata <> '' AND JSON_CONTAINS(metadata, CAST(? AS JSON))", string(jsonBytes))
	default:
		for key, value := range *m {
			pattern := `%` + escapeLikePattern(metadataPairFragment(key, value)) + `%`
			q = q.Where(metadataTextExpression(s)+" LIKE ? ESCAPE ?", pattern, `\`)
		}
		return q
	}
}

func mysqlMetadataGenColumn(key string) string {
	return "meta_" + key
}

func mysqlMetadataGenColumnDefinition(key string) string {
	column := mysqlMetadataGenColumn(key)
	return column + " VARCHAR(255) COLLATE utf8mb4_bin " +
		"GENERATED ALWAYS AS (JSON_VALUE(metadata, '$." + key + "' RETURNING CHAR(255) NULL ON ERROR)) STORED"
}

func metadataTextExpression(s *GormStorage) string {
	switch s.dialect() {
	case dialectPostgres:
		return "metadata::text"
	default:
		return "CAST(metadata AS CHAR)"
	}
}

// jobSortColumns whitelists the columns SearchJobs may ORDER BY. The client's
// sort key is looked up here and mapped to a fixed column literal — the raw key
// is NEVER interpolated into SQL — so server-side sort cannot be an injection
// vector regardless of what the dashboard sends.
var jobSortColumns = map[string]string{
	"created_at":   "created_at",
	"run_at":       "run_at",
	"started_at":   "started_at",
	"completed_at": "completed_at",
	"priority":     "priority",
	"status":       "status",
	"queue":        "queue",
	"type":         "type",
	"attempt":      "attempt",
}

// jobSortOrder builds a safe, deterministic ORDER BY clause from a JobFilter.
// An empty/unknown SortKey falls back to created_at; SortDir is asc or desc
// (default desc). A created_at,id tiebreak keeps paging stable when the chosen
// column has ties.
//
// # ACCEPTED RESIDUAL: ON SQLITE THIS ORDERS WALL FACES, NOT INSTANTS
//
// created_at is TEXT carrying the offset of whichever process wrote it, so a bare
// `created_at DESC` is a LEXICAL compare — the same hazard timeBoundPredicate's
// godoc spells out for the WHERE clause.
//
// IT IS NOT ONLY created_at, and naming just that one would mislead: run_at and
// started_at are whitelisted sort keys with the identical defect. run_at is
// deliberately re-faced to time.Local by normalizeRunAtZone, and started_at is
// written from dequeueOnce's process-local time.Now(), so `ORDER BY run_at DESC`
// inverts across a DST fall-back exactly as created_at does. dead_lettered_at is
// the ONE timestamp here that is now face-independent, because its ORDER BY is
// the one this round could fix on the write side without touching a correctness
// fence.
//
// Two ways in, neither hypothetical:
//
//   - ONE worker in a DST zone renders two offsets across the fall-back hour, so
//     for that hour every year "newest first" inverts by up to the fold.
//   - Two processes in different zones against one SQLite file (the CLI and the
//     standalone UI binaries are documented second processes) diverge by their
//     full offset delta, and on a page boundary the newest rows land on page 2.
//
// Postgres and MySQL store a real instant and are unaffected.
//
// # WHY IT IS NOT FIXED HERE, MEASURED
//
// Normalizing the ORDER BY through julianday() would be instant-correct, and it
// costs SQLite the index it was walking in order. Measured on the real migrated
// schema, 200k rows over ~139 days, 8 queues, ANALYZEd, LIMIT 50, mean of 5 runs
// after 2 warm-ups (full table in deadLetterOrderColumn):
//
//	                       bare ORDER BY   julianday() ORDER BY
//	list, queue = ?               279us              154.606ms   554x
//	list, status = ?              531us              258.767ms   487x
//	list, no filter            55.513ms               68.864ms   1.2x
//
//	plan queue = ?   bare: SEARCH … idx_jobs_queue_created
//	                         | USE TEMP B-TREE FOR RIGHT PART OF ORDER BY
//	                 jd:   SEARCH … idx_jobs_queue_created
//	                         | USE TEMP B-TREE FOR ORDER BY
//
// The unfiltered list already scans and sorts, so it barely moves; the FILTERED
// shapes — which is what the dashboard's queue and status pickers produce — are
// the ones that turn into a full read plus a sort of every matching row.
//
// The other route, a single stored clock face, is what dead_lettered_at took (see
// deadLetterOrderColumn) and what run_at already had (normalizeRunAtZone). It is
// NOT available to created_at: created_at is half of the dequeue correctness
// fence, `COALESCE(run_at, created_at) <= <process-local bind>` (gorm.go, and see
// claimableCandidates on why that gate and not dq_ready is the fence). Storing it
// on UTC while the bind stays local would read every freshly created job as due
// hours in the future in a positive-offset zone; moving the bind too breaks every
// ALREADY-STORED row, including firing scheduled jobs early. That is the v5 change
// normalizeRunAtZone's residual (3) already describes, not a late edit to a
// release branch. Rewriting stored text is not on the table either — a migration
// that did exactly that corrupted ordering on every SQLite database and was
// deleted.
//
// The route that would close it without either cost is an indexed generated
// column (SQLite VIRTUAL generated columns can be indexed, so no stored text
// changes and no plan loss) — a schema change, deliberately not made here.
//
// Pinned by TestR29_AcceptedResidual_SearchJobsSortsCreatedAtByWallFace and
// TestR29_AcceptedResidual_SearchJobsSortInvertsAcrossADSTFallBack, which fail if
// the residual is ever closed so this godoc cannot go stale, and by
// TestR29_JobsListOrderKeepsTheIndex, which fails if the normalized form is
// merged without the measurement being redone.
func jobSortOrder(filter core.JobFilter) string {
	col, ok := jobSortColumns[filter.SortKey]
	if !ok {
		col = "created_at"
	}
	dir := "DESC"
	if strings.EqualFold(filter.SortDir, "asc") {
		dir = "ASC"
	}
	if col == "created_at" {
		return "created_at " + dir + ", id DESC"
	}
	return col + " " + dir + ", created_at DESC, id DESC"
}

// applyJobSearch applies the dashboard/dead-letter search term to a jobs query.
// Job IDs are stored as binary uuid columns (v3), so a substring LIKE against id
// no longer works on Postgres/MySQL and is meaningless for random UUIDv7s.
// Instead an exact id match is added when the term parses as a UUID, alongside a
// substring search over the args text (which still supports free-text lookups).
func applyJobSearch(s *GormStorage, q *gorm.DB, rawSearch string) *gorm.DB {
	if rawSearch == "" {
		return q
	}
	searchTerm := rawSearch
	if len(searchTerm) > maxUISearchLength {
		searchTerm = searchTerm[:maxUISearchLength]
	}
	id, idErr := core.ParseUUID(strings.TrimSpace(searchTerm))
	isUUID := idErr == nil && id != core.NilUUID

	// Free-text args search only works on plaintext payloads. Under an encrypting
	// codec the args column holds non-UTF8 ciphertext: convert_from(args,'UTF8')
	// ERRORS on Postgres (22021) and CONVERT(args USING utf8mb4) SILENTLY corrupts
	// on MySQL. So the args-LIKE branch is gated to the identity codec. Under
	// encryption the search is limited to an exact job-ID match; a non-UUID term
	// matches nothing (returning unfiltered rows would misrepresent an explicit
	// search as "no filter applied" — exact-ID-only is the honest contract).
	if !s.argsSearchable() {
		if isUUID {
			return q.Where("id = ?", id)
		}
		return q.Where("1 = 0")
	}

	argsLike := "%" + escapeLikePattern(searchTerm) + "%"
	if isUUID {
		return q.Where("id = ? OR "+argsTextExpression(s)+" LIKE ? ESCAPE ?", id, argsLike, `\`)
	}
	return q.Where(argsTextExpression(s)+" LIKE ? ESCAPE ?", argsLike, `\`)
}

// argsSearchable reports whether the stored args column holds plaintext that can
// be safely LIKE-searched. True only for the identity (non-encrypting) codec; an
// encrypting codec stores non-UTF8 ciphertext that breaks text search.
func (s *GormStorage) argsSearchable() bool {
	return s.codecIsIdentity()
}

func argsTextExpression(s *GormStorage) string {
	switch s.dialect() {
	case dialectPostgres:
		return "convert_from(args,'UTF8')"
	case dialectMySQL:
		return "CONVERT(args USING utf8mb4)"
	default:
		return "CAST(args AS TEXT)"
	}
}

func metadataPairFragment(key, value string) string {
	pair, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return fmt.Sprintf(`"%s":"%s"`, key, value)
	}
	return strings.TrimSuffix(strings.TrimPrefix(string(pair), "{"), "}")
}

func clampUIPagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 50
	} else if limit > maxUIQueryLimit {
		limit = maxUIQueryLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func escapeLikePattern(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// RetryJob resets a failed or cancelled job back to pending for re-execution.
func (s *GormStorage) RetryJob(ctx context.Context, jobID core.UUID) (*core.Job, error) {
	var out core.Job
	err := s.withSerializationRetry(ctx, func() error {
		// Reset per attempt: a serialization retry must scan into a zero-value
		// struct so a NULLed column (result/run_at/completed_at) can't be masked by
		// a prior attempt's non-nil pointer. now is likewise refreshed per attempt.
		out = core.Job{}
		now := time.Now()
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var job core.Job
			if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
				return err
			}
			if job.Status != core.StatusFailed && job.Status != core.StatusCancelled {
				return fmt.Errorf("jobs: cannot retry job with status %q", job.Status)
			}
			// Route the dashboard "Retry" through the same replay-from-scratch reset
			// as storage Requeue: clear result/run_at, delete checkpoints, and delete
			// the fan-out subtree so a retried workflow parent re-dispatches a fresh
			// fan-out instead of replaying stale checkpoints/results. Retrying a
			// sub-job is rejected (ErrCannotRequeueSubJob) — replay via the parent.
			if err := s.applyRequeueResetTx(tx, &job, now); err != nil {
				return err
			}
			// Re-read the reset row (into a fresh struct so NULLed columns are not
			// masked by the pre-reset values) to return the persisted state.
			return tx.First(&out, "id = ?", jobID).Error
		})
	})
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobPayloads(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteJob permanently removes a single job from the database. It refuses to
// delete a fan-out parent (a job that has sub-jobs), returning
// core.ErrJobHasChildren — deleting a parent directly would strand its children
// with dangling parent_job_id/root_job_id/fan_out_id references (the FK cascade
// only covers fan_outs.parent_job_id, not the job self-references, and SQLite has
// no FKs at all). Use DeleteWorkflowSubtree to remove a whole workflow.
func (s *GormStorage) DeleteJob(ctx context.Context, jobID core.UUID) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var childFanOuts int64
		if err := tx.Model(&core.FanOut{}).Where("parent_job_id = ?", jobID).Count(&childFanOuts).Error; err != nil {
			return err
		}
		if childFanOuts > 0 {
			return fmt.Errorf("%w (job %s)", core.ErrJobHasChildren, jobID)
		}

		// Refuse to delete a fan-out sub-job whose PARENT is not yet terminal: the
		// parent rebuilds its results from surviving sub-job rows on resume, so
		// deleting a (succeeded) child first silently corrupts the aggregation — the
		// same hazard PurgeJobs and the retention path guard against. Allowed once
		// the parent reaches a terminal status (or via DeleteWorkflowSubtree).
		var livingParent int64
		if err := tx.Raw("SELECT COUNT(*) FROM jobs c JOIN fan_outs f ON f.id = c.fan_out_id "+
			"JOIN jobs pp ON pp.id = f.parent_job_id "+
			"WHERE c.id = ? AND pp.status NOT IN ("+quotedTerminalJobStatuses()+")", jobID).
			Scan(&livingParent).Error; err != nil {
			return err
		}
		if livingParent > 0 {
			return fmt.Errorf("%w (job %s)", core.ErrJobHasPendingParent, jobID)
		}

		// Delete checkpoints and any buffered signals first
		if err := tx.Where("job_id = ?", jobID).Delete(&core.Checkpoint{}).Error; err != nil {
			return err
		}
		if err := tx.Where("job_id = ?", jobID).Delete(&core.Signal{}).Error; err != nil {
			return err
		}
		// Release any windowed dedup lock (IdempotencyKey/UniqueFor) the job holds,
		// as PurgeJobs does. unique_locks.job_id has no FK cascade, so skipping this
		// would strand a live window pointing at a row that no longer exists — and
		// since a missing job row is deliberately NOT a steal trigger (see
		// stealTerminalUniqueLock), that window would keep blocking re-enqueue for
		// the remainder of its TTL. Deleting a job is an explicit operator act, so
		// releasing its window with it is the intended meaning.
		if err := tx.Where("job_id = ?", jobID).Delete(&core.UniqueLock{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", jobID).Delete(&core.Job{}).Error
	})
}

// DeleteWorkflowSubtree permanently removes the job at rootJobID together with
// its entire fan-out subtree: every fan-out it spawned at any depth, all of those
// sub-jobs, and all of their checkpoints/signals. This is the explicit,
// workflow-aware counterpart to DeleteJob (which refuses to delete a parent), so
// an operator can remove a whole workflow without orphaning rows. rootJobID may
// be any node in a workflow; everything at or below it is removed.
func (s *GormStorage) DeleteWorkflowSubtree(ctx context.Context, rootJobID core.UUID) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// deleteFanOutSubtree removes every descendant (sub-jobs + their
		// checkpoints/signals) and every fan_outs row whose parent is at or below
		// rootJobID — including rootJobID's own fan_outs — but it does NOT touch
		// the root job row itself or the root's own checkpoints/signals.
		if err := s.deleteFanOutSubtree(tx, rootJobID); err != nil {
			return err
		}
		if err := tx.Where("job_id = ?", rootJobID).Delete(&core.Checkpoint{}).Error; err != nil {
			return err
		}
		if err := tx.Where("job_id = ?", rootJobID).Delete(&core.Signal{}).Error; err != nil {
			return err
		}
		// Release the root's own windowed dedup lock; deleteFanOutSubtree released
		// the descendants'. See DeleteJob for why this must be explicit.
		if err := tx.Where("job_id = ?", rootJobID).Delete(&core.UniqueLock{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", rootJobID).Delete(&core.Job{}).Error
	})
}

// PurgeJobs deletes all jobs in a queue matching the given status, EXCLUDING
// fan-out parents. A parent is skipped (not deleted) so a bulk purge can never
// strand its children — a paused/terminal parent may still have children in
// other states, and the FK cascade does not reach the sub-jobs. Parents must be
// removed via DeleteWorkflowSubtree. Checkpoints, signals, AND unique_locks for
// the purged (leaf) jobs are deleted too; the returned count is the number of job
// rows actually removed. Rows a worker is actively holding (row-locked) are
// SKIP-LOCKED and left un-purged — so on a busy queue the count can be less than
// the total matching rows, and those rows sweep on a later call; this prevents
// deleting a job out from under a running worker.
func (s *GormStorage) PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error) {
	var deleted int64
	// Never purge a completed leaf sub-job whose owning fan-out's PARENT job is not
	// yet terminal: the parent rebuilds its result slice from surviving sub-job rows
	// when it resumes (pkg/fanout CollectResults), so deleting a succeeded child
	// before then silently turns it into ErrSubJobIncomplete with no top-level error.
	// Guarding on the fan_out's own status is insufficient — it flips 'completed' the
	// moment the last child finishes, BEFORE a stranded/paused/backlogged parent
	// resumes — so we key on the parent job's terminal status. This mirrors the
	// automatic retention path's fanOutParentGuard (retention.go). It lives in the
	// id-SELECT only (never the DELETE) so MySQL's "can't self-reference the delete
	// target" rule is not triggered.
	fanOutParentGuard := "NOT EXISTS (SELECT 1 FROM fan_outs f JOIN jobs pp ON pp.id = f.parent_job_id " +
		"WHERE f.id = jobs.fan_out_id AND pp.status NOT IN (" + quotedTerminalJobStatuses() + "))"
	const purgeBatchSize = 1000
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Uncorrelated set of all fan-out parent job ids. Matched jobs whose id is
		// in this set are skipped to avoid orphaning their children.
		parentIDs := tx.Model(&core.FanOut{}).
			Distinct("parent_job_id").
			Where("parent_job_id IS NOT NULL")

		// Resolve purgeable ids in LOCKED batches, then delete child rows and job
		// rows by that id list. Materializing the ids (rather than a jobs-referencing
		// subquery inside the DELETE) keeps the guard's self-reference out of the
		// DELETE, which MySQL rejects (err 1093). FOR UPDATE SKIP LOCKED on the pluck
		// is load-bearing: (a) it pins each batch so a row cannot transition OUT of
		// the target status between pluck and delete — without it a job a worker just
		// claimed (pending->running) or one retried (failed->pending via RetryJob)
		// mid-purge would be deleted WHILE LIVE, silently losing an in-flight job —
		// and (b) it skips rows a worker is actively running, so those are never
		// purged. Batching bounds the literal IN-list so a large backlog cannot
		// exceed the driver's bind-parameter ceiling (SQLite ~32k, Postgres 65535).
		// Locks the pluck identically to the retention path (retention.go); retention
		// batches across separate GC transactions, whereas PurgeJobs batches within
		// this one atomic transaction.
		for {
			idQuery := tx.Model(&core.Job{}).Select("id").
				Where("status = ?", status).
				Where("id NOT IN (?)", parentIDs).
				Where(fanOutParentGuard).
				Limit(purgeBatchSize)
			if queue != "" {
				idQuery = idQuery.Where("queue = ?", queue)
			}
			idQuery = s.lockForUpdate(idQuery, true)
			var ids []core.UUID
			if err := idQuery.Pluck("id", &ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}

			if err := tx.Where("job_id IN ?", ids).Delete(&core.Checkpoint{}).Error; err != nil {
				return err
			}
			if err := tx.Where("job_id IN ?", ids).Delete(&core.Signal{}).Error; err != nil {
				return err
			}
			// unique_locks.job_id has no FK cascade, so a purge that skipped them would
			// strand a dangling lock — a still-live one keeps blocking re-enqueue of a
			// dedup scope whose job is gone, and expired ones accumulate unbounded.
			if err := tx.Where("job_id IN ?", ids).Delete(&core.UniqueLock{}).Error; err != nil {
				return err
			}

			// Re-assert status on the job DELETE as belt-and-suspenders (the held
			// FOR UPDATE lock already pins it, and matches the retention path); a
			// literal id list, so still 1093-safe.
			result := tx.Where("id IN ?", ids).Where("status = ?", status).Delete(&core.Job{})
			if result.Error != nil {
				return result.Error
			}
			deleted += result.RowsAffected
		}
	})
	return deleted, err
}

// GetWorkflowRoots returns root workflow jobs (jobs with children but no parent).
func (s *GormStorage) GetWorkflowRoots(ctx context.Context, status string, limit, offset int) ([]*core.Job, int64, error) {
	q := s.db.WithContext(ctx).Model(&core.Job{}).
		Where("parent_job_id IS NULL").
		Where("id IN (SELECT DISTINCT parent_job_id FROM jobs WHERE parent_job_id IS NOT NULL)")

	if status != "" {
		statuses := strings.Split(status, ",")
		q = q.Where("status IN ?", statuses)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	limit, offset = clampUIPagination(limit, offset)

	var jobs []*core.Job
	// Bare created_at, for the same measured reason as jobSortOrder — and carrying
	// the same accepted residual on SQLite (mixed clock faces sort by wall face).
	err := q.Order("created_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, 0, err
	}

	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

// GetFanOutsByParents retrieves fan-outs for multiple parent jobs in one query.
func (s *GormStorage) GetFanOutsByParents(ctx context.Context, parentJobIDs []core.UUID) ([]*core.FanOut, error) {
	if len(parentJobIDs) == 0 {
		return nil, nil
	}

	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id IN ?", parentJobIDs).
		Order("parent_job_id ASC, created_at ASC").
		Find(&fanOuts).Error
	if err != nil {
		return nil, err
	}
	if err := overlayLiveFanOutCountsBatch(s.db.WithContext(ctx), fanOuts); err != nil {
		return nil, err
	}
	return fanOuts, nil
}

// GetSubJobsByFanOuts retrieves sub-jobs for multiple fan-outs in one query.
func (s *GormStorage) GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []core.UUID) ([]*core.Job, error) {
	if len(fanOutIDs) == 0 {
		return nil, nil
	}

	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id IN ?", fanOutIDs).
		Order("fan_out_id ASC, fan_out_index ASC").
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
