package storage

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// storedClockFaces returns the clock faces a jobs.created_at / dead_lettered_at
// value can actually wear on SQLite.
//
// Both columns are written from time.Now() (GORM autoCreateTime for created_at,
// Storage.Fail for dead_lettered_at), so a row wears the face of whichever
// PROCESS wrote it. The repo ships `sdj --driver sqlite --dsn ./jobs.db` and two
// standalone UI binaries as documented second processes against one SQLite file,
// and nothing makes them share a TZ with the worker — so one database routinely
// holds several faces. Bounds arrive on their own face too: timestamppb.AsTime is
// unconditionally UTC.
//
// A row seeded with an explicit CreatedAt on face F is byte-identical to a row
// autoCreateTime wrote in a process whose TZ is F (verified below by
// TestStoredTimestampWearsTheSuppliedClockFace), which is what lets a
// single-process test cover the whole writer-zone x reader-zone matrix.
//
// time.Local is included so the table still says something in the zone CI runs
// in; the fixed zones are what make it say something under TZ=UTC too. +14:00 and
// -12:00 are the widest offsets tzdata produces, so they also pin that the
// index-friendly loose bound in timeBoundPredicate is wide enough.
//
// +05:30 and -05:30 are a deliberate SIGN PAIR: same magnitude, opposite sign.
// The predicate's same-face test is substr(text, -6), which includes the sign
// character, and without a pair like this no fixture can tell substr(-6) from
// substr(-5) — the shorter form collapses "+05:30" and "-05:30" onto one key and
// silently compares two rows 11 hours apart as raw wall text. Asia/Colombo and a
// fixed -05:30 are both realistic writer faces.
func storedClockFaces() []*time.Location {
	return []*time.Location{
		time.UTC,
		time.Local,
		time.FixedZone("plus0530", 5*3600+1800),
		time.FixedZone("minus0530", -(5*3600 + 1800)),
		time.FixedZone("minus0700", -7*3600),
		time.FixedZone("plus1400", 14*3600),
		time.FixedZone("minus1200", -12*3600),
	}
}

// unixToInternalSeconds is time's own offset from the Unix epoch to its internal
// year-1 epoch. It is unexported there, so it is restated here purely to build a
// bound whose +26h shift genuinely overflows int64.
const unixToInternalSeconds = (1969*365 + 1969/4 - 1969/100 + 1969/400) * 24 * 60 * 60

// seedJobCreatedAt inserts a job whose created_at is stored on exactly the clock
// face of createdAt. GORM's autoCreateTime only fills the field when it is the
// zero value, so an explicit value is written through verbatim.
func seedJobCreatedAt(t *testing.T, ctx context.Context, s *GormStorage, queue, typ string, createdAt time.Time) core.UUID {
	t.Helper()
	job := &core.Job{Type: typ, Queue: queue, CreatedAt: createdAt}
	require.NoError(t, s.Enqueue(ctx, job))
	return job.ID
}

// TestStoredTimestampWearsTheSuppliedClockFace is the load-bearing premise of
// every table below: that seeding an explicit CreatedAt really does reproduce
// what a differently-zoned WRITER process stores. If GORM ever started
// normalizing the value, the matrix tests would still pass while covering only
// one face, so this asserts the raw bytes rather than the parsed instant.
func TestStoredTimestampWearsTheSuppliedClockFace(t *testing.T) {
	if !newTestStorage(t).isSQLite {
		t.Skip("stored-text assertion is SQLite-specific")
	}
	ctx := context.Background()
	s := newTestStorage(t)

	instant := time.Date(2026, 3, 4, 5, 6, 7, 890000000, time.UTC)
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("plus0530", 5*3600+1800)} {
		id := seedJobCreatedAt(t, ctx, s, "faceq", "t-"+loc.String(), instant.In(loc))
		var raw string
		require.NoError(t, s.db.WithContext(ctx).Raw(
			"SELECT CAST(created_at AS TEXT) FROM jobs WHERE id = ?", id).Scan(&raw).Error)
		assert.Equal(t, instant.In(loc).Format("2006-01-02 15:04:05.999999999-07:00"), raw,
			"created_at must be stored on the clock face it was supplied on")
	}
}

// TestSearchJobs_CreatedAtWindowSelectsByInstantOnEveryStoredFace is the
// user-facing invariant: a [since, until] window selects the jobs whose
// created_at INSTANT falls inside it, whatever face the row was stored on and
// whatever face the caller expressed the bound on.
//
// On SQLite a timestamp is TEXT with a trailing offset compared LEXICALLY, so a
// bare `created_at >= ?` compares two wall-clock faces and is wrong by the delta
// between them. Fixing that by re-facing the BIND is not enough — it only makes
// reader-face == writer-face work, and measurably breaks writer=UTC/reader!=UTC,
// which the unfixed code got right. Hence the full cross product below: every
// cell must hold, not a diagonal.
func TestSearchJobs_CreatedAtWindowSelectsByInstantOnEveryStoredFace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	anchor := time.Now()
	faces := storedClockFaces()
	for i, writerFace := range faces {
		queue := fmt.Sprintf("wq%d", i)
		// Two jobs inside the window and one 48h-old bystander, all written by the
		// same "writer process" (= same stored face). The bystander is what stops
		// "drop the predicate and return everything" from passing.
		seedJobCreatedAt(t, ctx, s, queue, "fresh-a", anchor.In(writerFace))
		seedJobCreatedAt(t, ctx, s, queue, "fresh-b", anchor.Add(-time.Minute).In(writerFace))
		seedJobCreatedAt(t, ctx, s, queue, "ancient", anchor.Add(-48*time.Hour).In(writerFace))
	}

	for i, writerFace := range faces {
		queue := fmt.Sprintf("wq%d", i)
		for _, readerFace := range faces {
			name := fmt.Sprintf("writer=%s/reader=%s", writerFace, readerFace)
			t.Run(name, func(t *testing.T) {
				count := func(f core.JobFilter) int64 {
					t.Helper()
					f.Queue = queue
					f.Limit = 50
					jobs, total, err := s.SearchJobs(ctx, f)
					require.NoError(t, err)
					assert.Len(t, jobs, int(total), "page and count must agree")
					return total
				}

				assert.Equal(t, int64(2), count(core.JobFilter{
					Since: anchor.Add(-time.Hour).In(readerFace),
					Until: anchor.Add(time.Hour).In(readerFace),
				}), "[now-1h, now+1h] must bracket exactly the two fresh jobs")

				assert.Equal(t, int64(0), count(core.JobFilter{
					Since: anchor.Add(time.Hour).In(readerFace),
				}), "since=now+1h must exclude every job")

				assert.Equal(t, int64(1), count(core.JobFilter{
					Until: anchor.Add(-time.Hour).In(readerFace),
				}), "until=now-1h must keep only the 48h-old bystander")

				assert.Equal(t, int64(3), count(core.JobFilter{}),
					"an absent window must not narrow the result")
			})
		}
	}
}

// TestSearchJobs_CreatedAtWindowIsCorrectAcrossDSTFallBack covers the case that
// needs no second process at all: ONE zone writes TWO clock faces across a DST
// transition, so a bound converted with the offset in force at the BOUND's
// instant is wrong for a row wearing the offset in force at the ROW's instant.
// Inside the fall-back hour that is a full hour of mis-ordering.
func TestSearchJobs_CreatedAtWindowIsCorrectAcrossDSTFallBack(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err, "test needs tzdata")

	// 2026-11-01: America/Los_Angeles leaves DST at 09:00 UTC. The row is stored
	// 30 minutes BEFORE the change (face -07:00), the bound falls 10 minutes AFTER
	// it (face -08:00), so the row's instant is 40 minutes before the bound.
	rowInstant := time.Date(2026, 11, 1, 8, 30, 0, 0, time.UTC)
	boundInstant := time.Date(2026, 11, 1, 9, 10, 0, 0, time.UTC)
	_, rowOffset := rowInstant.In(la).Zone()
	_, boundOffset := boundInstant.In(la).Zone()
	require.NotEqual(t, rowOffset, boundOffset,
		"fixture must straddle the DST transition, or it proves nothing")

	seedJobCreatedAt(t, ctx, s, "dstq", "straddler", rowInstant.In(la))

	count := func(f core.JobFilter) int64 {
		t.Helper()
		f.Queue = "dstq"
		f.Limit = 50
		_, total, err := s.SearchJobs(ctx, f)
		require.NoError(t, err)
		return total
	}

	// Control: an unambiguous bound an hour before the row still includes it, so a
	// failure below is the DST bound and not a broken fixture.
	require.Equal(t, int64(1), count(core.JobFilter{Since: rowInstant.Add(-time.Hour).In(la)}),
		"control: a bound an hour before the row must include it")

	assert.Equal(t, int64(0), count(core.JobFilter{Since: boundInstant.In(la)}),
		"the row's instant is 40 minutes BEFORE since, so it must be excluded")
	assert.Equal(t, int64(1), count(core.JobFilter{Until: boundInstant.In(la)}),
		"the row's instant is 40 minutes BEFORE until, so it must be included")
}

// TestSearchJobs_CreatedAtWindowBoundsAreInclusive pins the documented
// inclusivity of both bounds — on the same-face fast path AND on the cross-face
// path, which are separate branches of the predicate and can rot independently.
func TestSearchJobs_CreatedAtWindowBoundsAreInclusive(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Millisecond-aligned: the cross-face branch normalizes through strftime('%f'),
	// which is millisecond-resolution, so an equal-instant assertion has to be
	// expressible at that resolution to mean anything.
	exact := time.Now().Truncate(time.Millisecond)
	foreign := time.FixedZone("foreign", 5*3600+1800)

	for _, tc := range []struct {
		name string
		face *time.Location
	}{
		{"same face as the bound", time.Local},
		{"different face from the bound", foreign},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue := "incl-" + tc.name
			seedJobCreatedAt(t, ctx, s, queue, "onthedot", exact.In(tc.face))

			count := func(f core.JobFilter) int64 {
				t.Helper()
				f.Queue = queue
				f.Limit = 50
				_, total, err := s.SearchJobs(ctx, f)
				require.NoError(t, err)
				return total
			}

			assert.Equal(t, int64(1), count(core.JobFilter{Since: exact}),
				"Since is documented INCLUSIVE: a job created exactly at the bound is in")
			assert.Equal(t, int64(1), count(core.JobFilter{Until: exact}),
				"Until is documented INCLUSIVE: a job created exactly at the bound is in")
			assert.Equal(t, int64(0), count(core.JobFilter{Since: exact.Add(time.Millisecond)}),
				"a bound past the job must still exclude it")
			assert.Equal(t, int64(0), count(core.JobFilter{Until: exact.Add(-time.Millisecond)}),
				"a bound before the job must still exclude it")
		})
	}
}

// TestSearchJobs_SameFaceWindowKeepsNanosecondPrecision pins the reason the
// predicate keeps a raw-text fast path instead of normalizing every comparison
// through strftime().
//
// strftime('%f') is MILLISECOND resolution, so an unconditional normalization
// collapses any two values less than 1ms apart — the same truncation that
// measurably stalled sub-millisecond schedules when an earlier scheduleCursorLess
// did it (see gorm.go). When the row and the bound already share a clock face the
// raw driver text compares exactly, to the full nanosecond the driver wrote, and
// this asserts that rather than leaving it as a claim in a comment.
func TestSearchJobs_SameFaceWindowKeepsNanosecondPrecision(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("nanosecond text precision is a SQLite storage property")
	}

	// Same face as the bound below, so the fast path is the branch under test.
	exact := time.Date(2026, 5, 4, 3, 2, 1, 123456789, time.Local)
	seedJobCreatedAt(t, ctx, s, "nanoq", "nanojob", exact)

	count := func(f core.JobFilter) int64 {
		t.Helper()
		f.Queue = "nanoq"
		f.Limit = 50
		_, total, err := s.SearchJobs(ctx, f)
		require.NoError(t, err)
		return total
	}

	require.Equal(t, int64(1), count(core.JobFilter{Since: exact}),
		"control: the bound at the exact instant includes the row")
	assert.Equal(t, int64(0), count(core.JobFilter{Since: exact.Add(time.Nanosecond)}),
		"a bound ONE NANOSECOND after the row must exclude it on the same-face path")
	assert.Equal(t, int64(0), count(core.JobFilter{Until: exact.Add(-time.Nanosecond)}),
		"a bound ONE NANOSECOND before the row must exclude it on the same-face path")
}

// TestTimeBoundPredicate_NonSQLiteKeepsThePlainForm pins the branch a SQLite-only
// test suite can otherwise never reach. Postgres and MySQL store a real instant
// and already compare instants, so they must get a bare `column >= ?` with the
// caller's time.Time passed through UNCHANGED — same instant AND same Location,
// because re-facing it there would be a pointless behaviour change and any
// function wrapping would cost those backends their index range.
//
// No server needed: the predicate is a pure function of s.isSQLite.
func TestTimeBoundPredicate_NonSQLiteKeepsThePlainForm(t *testing.T) {
	bound := time.Date(2026, 8, 1, 12, 0, 0, 123456789, time.FixedZone("probe", -7*3600))

	for _, tc := range []struct {
		name   string
		dir    timeBoundDirection
		column string
		want   string
	}{
		{"lower bound on created_at", boundAtOrAfter, "created_at", "created_at >= ?"},
		{"upper bound on created_at", boundAtOrBefore, "created_at", "created_at <= ?"},
		{"lower bound on dead_lettered_at", boundAtOrAfter, "dead_lettered_at", "dead_lettered_at >= ?"},
		{"upper bound on dead_lettered_at", boundAtOrBefore, "dead_lettered_at", "dead_lettered_at <= ?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &GormStorage{isSQLite: false}
			pred, args := s.timeBoundPredicate(tc.column, tc.dir, bound)
			assert.Equal(t, tc.want, pred, "non-SQLite backends must keep the bare, index-usable form")
			require.Len(t, args, 1)
			got, ok := args[0].(time.Time)
			require.True(t, ok, "the bind must stay a time.Time, not a rendered string")
			assert.True(t, got.Equal(bound), "the instant must be passed through unchanged")
			assert.Equal(t, bound.Location(), got.Location(),
				"the clock face must be passed through unchanged on backends that store an instant")

			// And the guard must be load-bearing in the other direction: the SQLite
			// form is a genuinely different predicate, not the same string.
			sqliteStore := &GormStorage{isSQLite: true}
			sqlitePred, sqliteArgs := sqliteStore.timeBoundPredicate(tc.column, tc.dir, bound)
			assert.NotEqual(t, tc.want, sqlitePred)
			assert.Contains(t, sqlitePred, "strftime", "SQLite needs the face-independent comparison")
			assert.Len(t, sqliteArgs, 4)
		})
	}
}

// deadLetter drives the real terminal-failure path so dead_lettered_at is written
// by production code, then re-points the two timestamps at chosen instants and
// clock faces — which is exactly what a differently-zoned worker process would
// have stored.
func deadLetter(t *testing.T, ctx context.Context, s *GormStorage, queue, typ string, createdAt, diedAt time.Time) core.UUID {
	t.Helper()
	job := &core.Job{Type: typ, Queue: queue, MaxRetries: 1}
	require.NoError(t, s.Enqueue(ctx, job))
	claimed, err := s.Dequeue(ctx, []string{queue}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.NoError(t, s.Fail(ctx, claimed.ID, "worker-1", "boom", nil))
	dead, err := s.GetJob(ctx, claimed.ID)
	require.NoError(t, err)
	require.NotNil(t, dead.DeadLetteredAt, "fixture must actually be dead-lettered")

	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", claimed.ID).
		Updates(map[string]any{"created_at": createdAt, "dead_lettered_at": diedAt}).Error)
	return claimed.ID
}

// TestDeadLetterQueries_WindowBoundsDeathNotBirth pins the column choice, which
// is a deliberate decision and not a side effect: on the dead-letter branch the
// window means dead_lettered_at.
//
// "What died in the last hour" is the DLQ triage query, and it must return a job
// that was born two days ago and died a second ago. Bounding created_at instead
// would hide exactly that job — and the view is ordered dead_lettered_at DESC, so
// it would also be filtering one axis while sorting another.
func TestDeadLetterQueries_WindowBoundsDeathNotBirth(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	now := time.Now()
	deadLetter(t, ctx, s, "dlqcol", "old-job-just-died", now.Add(-48*time.Hour), now)
	deadLetter(t, ctx, s, "dlqcol", "new-job-died-long-ago", now, now.Add(-48*time.Hour))

	filter := core.DeadLetterFilter{
		Queue:             "dlqcol",
		DeadLetteredSince: now.Add(-time.Hour),
		DeadLetteredUntil: now.Add(time.Hour),
		Limit:             50,
	}
	jobs, err := s.ListDeadLettered(ctx, filter)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, "old-job-just-died", jobs[0].Type,
		"a job created 48h ago but dead-lettered a moment ago is what a last-hour DLQ query is for")

	total, err := s.CountDeadLettered(ctx, filter)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "count must honour the same window as the list")

	unbounded, err := s.CountDeadLettered(ctx, core.DeadLetterFilter{Queue: "dlqcol", Limit: 50})
	require.NoError(t, err)
	assert.Equal(t, int64(2), unbounded, "an absent window must not narrow the result")
}

// TestDeadLetterQueries_WindowSelectsByInstantOnEveryStoredFace is the
// dead-letter mirror of the SearchJobs matrix: dead_lettered_at is written from
// time.Now() in Storage.Fail, so it wears the writing process's face and needs
// the same face-independent comparison.
func TestDeadLetterQueries_WindowSelectsByInstantOnEveryStoredFace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	anchor := time.Now()
	faces := storedClockFaces()
	for i, writerFace := range faces {
		queue := fmt.Sprintf("dlq%d", i)
		deadLetter(t, ctx, s, queue, "fresh-a", anchor, anchor.In(writerFace))
		deadLetter(t, ctx, s, queue, "fresh-b", anchor, anchor.Add(-time.Minute).In(writerFace))
		deadLetter(t, ctx, s, queue, "ancient", anchor, anchor.Add(-48*time.Hour).In(writerFace))
	}

	for i, writerFace := range faces {
		queue := fmt.Sprintf("dlq%d", i)
		for _, readerFace := range faces {
			t.Run(fmt.Sprintf("writer=%s/reader=%s", writerFace, readerFace), func(t *testing.T) {
				count := func(f core.DeadLetterFilter) int64 {
					t.Helper()
					f.Queue = queue
					f.Limit = 50
					jobs, err := s.ListDeadLettered(ctx, f)
					require.NoError(t, err)
					total, err := s.CountDeadLettered(ctx, f)
					require.NoError(t, err)
					assert.Len(t, jobs, int(total), "page and count must agree")
					return total
				}

				assert.Equal(t, int64(2), count(core.DeadLetterFilter{
					DeadLetteredSince: anchor.Add(-time.Hour).In(readerFace),
					DeadLetteredUntil: anchor.Add(time.Hour).In(readerFace),
				}), "[now-1h, now+1h] must bracket exactly the two freshly dead jobs")

				assert.Equal(t, int64(0), count(core.DeadLetterFilter{
					DeadLetteredSince: anchor.Add(time.Hour).In(readerFace),
				}), "since=now+1h must exclude every dead-lettered job")

				assert.Equal(t, int64(1), count(core.DeadLetterFilter{
					DeadLetteredUntil: anchor.Add(-time.Hour).In(readerFace),
				}), "until=now-1h must keep only the 48h-old bystander")

				assert.Equal(t, int64(3), count(core.DeadLetterFilter{}),
					"an absent window must not narrow the result")
			})
		}
	}
}

// TestDeadLetterFilter_StaysComparable is a compile-time probe: DeadLetterFilter
// is an exported concrete struct and the release-gating api-compat job treats
// losing comparability as an incompatible change (a map key type MUST be
// comparable, so this fails to build rather than to run). Adding time.Time fields
// must not break it.
func TestDeadLetterFilter_StaysComparable(t *testing.T) {
	set := map[core.DeadLetterFilter]struct{}{}
	set[core.DeadLetterFilter{Queue: "a"}] = struct{}{}
	set[core.DeadLetterFilter{Queue: "a"}] = struct{}{}
	assert.Len(t, set, 1)
}

// TestSearchJobs_SameFaceFastPathDistinguishesTheOffsetSign pins the WIDTH of the
// same-face test, which decides which of the two comparison arms runs.
//
// The predicate takes the raw-text fast path only when the row and the bound wear
// the SAME trailing offset, and it decides that with substr(text, -6) — six
// characters, "+05:30" / "-05:30", SIGN INCLUDED. Narrowing it to substr(-5)
// drops the sign, so two faces 11 hours apart hash to the same key, the raw-text
// arm runs on values that do NOT sort by instant, and the comparison silently
// reverts to comparing wall faces: exactly the bug this predicate exists to fix.
//
// This needs a sign PAIR to see, which is why storedClockFaces carries one. The
// matrix tests above would catch it too; this states it directly so the reason the
// fixture contains ±05:30 cannot be lost to a future tidy-up.
func TestSearchJobs_SameFaceFastPathDistinguishesTheOffsetSign(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("the raw-text fast path is a SQLite storage property")
	}

	plus := time.FixedZone("plus0530", 5*3600+1800)
	minus := time.FixedZone("minus0530", -(5*3600 + 1800))
	require.Equal(t, plus.String()[len(plus.String())-4:], minus.String()[len(minus.String())-4:],
		"fixture must be a same-magnitude opposite-sign pair, or it proves nothing")

	// One instant, stored on +05:30. Its wall text reads 11 hours LATER than the
	// same instant written on -05:30, so a sign-blind comparison against a -05:30
	// bound gets the direction wrong by 11 hours in both directions.
	instant := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	seedJobCreatedAt(t, ctx, s, "signq", "plusrow", instant.In(plus))

	count := func(f core.JobFilter) int64 {
		t.Helper()
		f.Queue = "signq"
		f.Limit = 50
		_, total, err := s.SearchJobs(ctx, f)
		require.NoError(t, err)
		return total
	}

	require.Equal(t, int64(1), count(core.JobFilter{}), "control: the row exists")
	// The bound wears the MIRROR face and sits one minute after the row, so the
	// row must be excluded from [since] and included in [until]. Under substr(-5)
	// the raw-text arm compares "…17:30…" against "…06:31…" and answers both the
	// wrong way round.
	after := instant.Add(time.Minute).In(minus)
	assert.Equal(t, int64(0), count(core.JobFilter{Since: after}),
		"a bound one minute AFTER the row, on the mirrored face, must exclude it")
	assert.Equal(t, int64(1), count(core.JobFilter{Until: after}),
		"a bound one minute AFTER the row, on the mirrored face, must include it")

	before := instant.Add(-time.Minute).In(minus)
	assert.Equal(t, int64(1), count(core.JobFilter{Since: before}),
		"a bound one minute BEFORE the row, on the mirrored face, must include it")
	assert.Equal(t, int64(0), count(core.JobFilter{Until: before}),
		"a bound one minute BEFORE the row, on the mirrored face, must exclude it")
}

// TestSearchJobs_OpenEndedFarFutureUntilKeepsEveryRow is the user-facing form of
// the range check in sqliteComparableBound.
//
// timestamppb.IsValid accepts 9999-12-31T23:59:59.999999999Z, and it is the
// natural "no upper bound" sentinel for a programmatic Connect client. Two
// independent mechanisms turn it into a silent EMPTY PAGE if the bound is not
// range-checked: strftime() rounds it past the end of SQLite's supported range and
// returns NULL (so the cross-face arm drops every row — reachable whenever the
// reader's zone differs from the stored face, e.g. TZ=America/Los_Angeles), and
// adding the ±26h prefilter slop rolls it into year 10000, whose text sorts BELOW
// every real row (so the loose prefilter drops every row in ANY zone). Unpatched
// v4.6 returns the rows; a naive fix returns none.
//
// The far-past `since` leg is the mirror, and the year-10000 legs go past what
// timestamppb itself can hold, because core.JobFilter is exported and takes a
// plain time.Time.
func TestSearchJobs_OpenEndedFarFutureUntilKeepsEveryRow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	anchor := time.Now()
	// Stored on a foreign face so the CROSS-FACE arm is the one under test; a
	// same-face row would take the raw-text arm and hide the strftime NULL.
	foreign := time.FixedZone("plus0530", 5*3600+1800)
	seedJobCreatedAt(t, ctx, s, "openq", "a", anchor.In(foreign))
	seedJobCreatedAt(t, ctx, s, "openq", "b", anchor.Add(-48*time.Hour).In(foreign))

	count := func(f core.JobFilter) int64 {
		t.Helper()
		f.Queue = "openq"
		f.Limit = 50
		jobs, total, err := s.SearchJobs(ctx, f)
		require.NoError(t, err)
		assert.Len(t, jobs, int(total), "page and count must agree")
		return total
	}

	require.Equal(t, int64(2), count(core.JobFilter{}), "control: both rows exist")

	protoMax := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	// timestamppb's MINIMUM, 0001-01-01T00:00:00Z, is exactly Go's zero time.Time,
	// which JobFilter documents as "no bound" — so it can never reach the predicate
	// and is not what this test is about. One nanosecond later is the smallest
	// value that does reach it.
	protoMin := time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC)
	require.False(t, protoMin.IsZero(), "the far-past leg must not be read as an absent bound")
	require.True(t, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).IsZero(),
		"and the exact protobuf minimum must still be read as one")
	beyondLexical := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	beforeLexical := time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name   string
		filter core.JobFilter
		want   int64
	}{
		{"until = protobuf max", core.JobFilter{Until: protoMax}, 2},
		{"until = year 10000", core.JobFilter{Until: beyondLexical}, 2},
		{"since = protobuf min + 1ns", core.JobFilter{Since: protoMin}, 2},
		{"since = year -1", core.JobFilter{Since: beforeLexical}, 2},
		{"open window, both ends", core.JobFilter{Since: protoMin, Until: protoMax}, 2},
		// The mirrored directions still EXCLUDE: a lower bound at the end of time
		// and an upper bound at the dawn of it must not turn into "no bound".
		{"since = protobuf max", core.JobFilter{Since: protoMax}, 0},
		{"since = year 10000", core.JobFilter{Since: beyondLexical}, 0},
		{"until = protobuf min + 1ns", core.JobFilter{Until: protoMin}, 0},
		{"until = year -1", core.JobFilter{Until: beforeLexical}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, count(tc.filter))
		})
	}
}

// TestSearchJobs_BoundOnAnUnparsableFaceStillCompares covers the other value
// SQLite's date parser refuses: a timezone suffix beyond ±14:00. No tzdata zone
// produces one, but core.JobFilter takes a plain time.Time and time.FixedZone
// will happily build one, and strftime() returns NULL for it — which silently
// drops every row on the cross-face arm rather than raising an error.
//
// The instant is unchanged by re-facing, so the answer must be identical to the
// same bound expressed in UTC.
func TestSearchJobs_BoundOnAnUnparsableFaceStillCompares(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("SQLite's date-parser offset cap is a SQLite property")
	}

	anchor := time.Now()
	seedJobCreatedAt(t, ctx, s, "farfaceq", "fresh", anchor.In(time.FixedZone("plus0530", 5*3600+1800)))
	seedJobCreatedAt(t, ctx, s, "farfaceq", "ancient", anchor.Add(-48*time.Hour).In(time.UTC))

	count := func(f core.JobFilter) int64 {
		t.Helper()
		f.Queue = "farfaceq"
		f.Limit = 50
		_, total, err := s.SearchJobs(ctx, f)
		require.NoError(t, err)
		return total
	}

	require.Equal(t, int64(2), count(core.JobFilter{}), "control: both rows exist")

	// BOTH SIGNS. sqliteComparableBound decides via absDuration, so a positive-only
	// fixture leaves the `d < 0` branch deletable with the suite green — the whole
	// guard exists to stop a silent empty page, and half of it would be untested.
	for _, unparsable := range []*time.Location{
		time.FixedZone("plus1600", 16*3600),
		time.FixedZone("minus1600", -16*3600),
	} {
		t.Run(unparsable.String(), func(t *testing.T) {
			assert.Equal(t, int64(1), count(core.JobFilter{Since: anchor.Add(-time.Hour).In(unparsable)}),
				"a bound on a face SQLite cannot parse must still select by instant")
			assert.Equal(t, int64(1), count(core.JobFilter{Until: anchor.Add(-time.Hour).In(unparsable)}),
				"and in the other direction")
		})
	}
}

// TestSearchJobs_SubMillisecondCrossFaceNeverDropsAnInsideRow pins the invariant
// that timeBoundPredicate's godoc, ui.JobFilter's godoc and core.DeadLetterFilter's
// godoc all state absolutely: the cross-face branch's millisecond resolution errs
// ONLY towards admitting extra rows, and never drops a row that belongs in the
// window.
//
// It exists because a reviewer read that claim, argued a row sitting exactly on an
// inclusive bound could be DROPPED on a half-millisecond rounding tie, and observed
// correctly that nothing here could have caught it: every other cross-face fixture
// is millisecond-ALIGNED (.500000000, or Truncate(time.Millisecond)), so by
// construction none of them can reach a tie at all.
//
// The claim was then measured rather than argued. strftime('%f') was found to render
// a given instant identically on every face (36,015 comparisons at 1ns granularity
// around five half-millisecond tie points, offsets from -12:00 to +14:00), so both
// sides of the comparison round the same way, monotonicity survives, and only
// collapse-to-equal — over-inclusion — is reachable. This test is the durable form
// of that measurement: it sweeps genuinely UN-aligned sub-millisecond offsets across
// the full writer-face x reader-face matrix and asserts no inside row is ever lost.
//
// If a future change makes the two sides round differently, this reds.
func TestSearchJobs_SubMillisecondCrossFaceNeverDropsAnInsideRow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("strftime resolution is a SQLite storage property")
	}

	faces := storedClockFaces()
	// Deliberately NOT millisecond-aligned, and straddling the .5ms tie in both
	// directions — the exact values an aligned fixture cannot produce.
	offsets := []int{1, 999, 250_000, 499_999, 500_000, 500_001, 750_000, 999_999}

	for _, ns := range offsets {
		for fi, rowFace := range faces {
			instant := time.Date(2026, 5, 4, 3, 2, 1, ns, time.UTC)
			queue := fmt.Sprintf("subms_%d_%d", ns, fi)
			seedJobCreatedAt(t, ctx, s, queue, "row", instant.In(rowFace))

			for _, boundFace := range faces {
				// The window strictly CONTAINS the row by 5ms on both sides, so the
				// row is unambiguously inside no matter how the ends round.
				_, total, err := s.SearchJobs(ctx, core.JobFilter{
					Queue: queue,
					Limit: 10,
					Since: instant.Add(-5 * time.Millisecond).In(boundFace),
					Until: instant.Add(5 * time.Millisecond).In(boundFace),
				})
				require.NoError(t, err)
				require.Equal(t, int64(1), total,
					"row at +%dns on %s, bounds on %s: a row inside the window must never be dropped",
					ns, rowFace, boundFace)
			}
		}
	}
}

// TestTimeBoundPredicate_BoundsOutsideSQLitesRangeDegradeSafely states the
// contract sqliteComparableBound implements, at the level where a reader can
// check it against the SQL: which bounds produce NO predicate (because they
// exclude nothing storable), which are clamped, and which are re-faced.
func TestTimeBoundPredicate_BoundsOutsideSQLitesRangeDegradeSafely(t *testing.T) {
	s := &GormStorage{isSQLite: true}
	protoMax := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	protoMin := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("an open-ended upper bound emits no predicate", func(t *testing.T) {
		pred, args := s.timeBoundPredicate("created_at", boundAtOrBefore, protoMax)
		assert.Empty(t, pred, "a bound past everything storable must not restrict anything")
		assert.Empty(t, args)
	})

	t.Run("an open-ended lower bound emits no predicate", func(t *testing.T) {
		pred, args := s.timeBoundPredicate("created_at", boundAtOrAfter,
			protoMin.Add(-24*time.Hour))
		assert.Empty(t, pred)
		assert.Empty(t, args)
	})

	t.Run("the mirrored directions still restrict", func(t *testing.T) {
		pred, args := s.timeBoundPredicate("created_at", boundAtOrAfter, protoMax)
		require.NotEmpty(t, pred, "since=end-of-time must exclude, not admit everything")
		require.NotEmpty(t, args)
		pred, args = s.timeBoundPredicate("created_at", boundAtOrBefore, protoMin.Add(-24*time.Hour))
		require.NotEmpty(t, pred, "until=dawn-of-time must exclude, not admit everything")
		require.NotEmpty(t, args)
	})

	t.Run("a clamped bound stays inside the four-digit year", func(t *testing.T) {
		_, args := s.timeBoundPredicate("created_at", boundAtOrAfter,
			time.Date(10000, 6, 1, 0, 0, 0, 0, time.UTC))
		require.NotEmpty(t, args)
		for i, arg := range args {
			bound, ok := arg.(time.Time)
			require.True(t, ok, "bind %d must be a time.Time", i)
			assert.LessOrEqual(t, bound.Year(), 9999,
				"a five-digit year renders text that sorts BELOW every real row")
			assert.GreaterOrEqual(t, bound.Year(), 1)
		}
	})

	t.Run("a face SQLite cannot parse is re-faced to UTC", func(t *testing.T) {
		bound := time.Date(2026, 8, 1, 12, 0, 0, 0, time.FixedZone("plus1600", 16*3600))
		_, args := s.timeBoundPredicate("created_at", boundAtOrAfter, bound)
		require.Len(t, args, 4)
		for i, arg := range args {
			got, ok := arg.(time.Time)
			require.True(t, ok, "bind %d must be a time.Time", i)
			_, offset := got.Zone()
			assert.LessOrEqual(t, absDuration(time.Duration(offset)*time.Second),
				sqliteMaxParsableFaceOffset,
				"strftime() returns NULL for a suffix past ±14:00, which drops every row")
		}
		// Instant-preserving: the exact binds still name the same moment.
		assert.True(t, args[1].(time.Time).Equal(bound))
	})

	t.Run("an in-range bound is passed through untouched", func(t *testing.T) {
		bound := time.Date(2026, 8, 1, 12, 0, 0, 123456789, time.FixedZone("minus0700", -7*3600))
		_, args := s.timeBoundPredicate("created_at", boundAtOrAfter, bound)
		require.Len(t, args, 4)
		for _, i := range []int{1, 2, 3} {
			assert.Equal(t, bound, args[i],
				"the exact binds must keep the caller's instant AND face")
		}
	})
}

// TestTimeBoundPredicate_LoosePrefilterIsUTCFacedAndClamped pins the index-
// preserving bare-column prefilter itself — the only clause in this predicate that
// can EXCLUDE a row the exact comparison would keep, and therefore the one that
// most needs an assertion rather than a comment.
//
// Two properties, both load-bearing:
//
//   - The bind is UTC-FACED. The ±26h headroom argument is "wall = instant +
//     offset, |offset| ≤ 14h", and it only closes when the BOUND's own offset is
//     zero. Left on the caller's face the requirement becomes offsetBound −
//     offsetRow ≤ 26h, which at tzdata's extremes (+14:00 bound, −12:00 row) is
//     26h exactly — no headroom at all, and negative headroom for a FixedZone.
//   - The shift is RANGE-CHECKED. bound.UTC().Add(+26h) on a bound near the end of
//     the four-digit year band rolls into year 10000, whose text sorts BELOW every
//     real row, so the prefilter would reject everything.
func TestTimeBoundPredicate_LoosePrefilterIsUTCFacedAndClamped(t *testing.T) {
	s := &GormStorage{isSQLite: true}
	const driverLayout = "2006-01-02 15:04:05.999999999-07:00"

	t.Run("the loose bind wears UTC whatever face the caller used", func(t *testing.T) {
		for _, face := range []*time.Location{
			time.UTC,
			time.FixedZone("plus1400", 14*3600),
			time.FixedZone("minus1200", -12*3600),
		} {
			bound := time.Date(2026, 8, 1, 12, 0, 0, 0, face)
			for _, tc := range []struct {
				dir    timeBoundDirection
				slop   time.Duration
				prefix string
			}{
				{boundAtOrAfter, -maxStoredClockFaceOffset, "(created_at >= ? AND "},
				{boundAtOrBefore, maxStoredClockFaceOffset, "(created_at <= ? AND "},
			} {
				pred, args := s.timeBoundPredicate("created_at", tc.dir, bound)
				require.True(t, strings.HasPrefix(pred, tc.prefix),
					"the bare prefilter must be the FIRST clause so SQLite can use it as an "+
						"index range; got %q", pred)
				require.Len(t, args, 4)
				loose, ok := args[0].(time.Time)
				require.True(t, ok, "the loose bind must stay a time.Time")
				assert.True(t, strings.HasSuffix(loose.Format(driverLayout), "+00:00"),
					"the loose bind must RENDER on the UTC face, or the ±26h headroom is not there")
				assert.True(t, loose.Equal(bound.Add(tc.slop)),
					"and it must still be the caller's instant shifted by exactly the slop")
			}
		}
	})

	t.Run("the prefilter is dropped when the shift leaves the year band", func(t *testing.T) {
		// In range on its own, out of range once +26h is added.
		nearTheEnd := time.Date(9999, 12, 31, 12, 0, 0, 0, time.UTC)
		pred, args := s.timeBoundPredicate("created_at", boundAtOrBefore, nearTheEnd)
		require.NotEmpty(t, pred, "the bound itself is still storable, so it must restrict")
		assert.NotContains(t, pred, "created_at <= ? AND",
			"a five-digit prefilter bind would reject every row; the clause must be omitted")
		assert.Len(t, args, 3, "only the three exact binds remain")

		// The mirror: in range on its own, out of range once -26h is subtracted.
		nearTheStart := time.Date(1, 1, 1, 12, 0, 0, 0, time.UTC)
		pred, args = s.timeBoundPredicate("created_at", boundAtOrAfter, nearTheStart)
		require.NotEmpty(t, pred)
		assert.NotContains(t, pred, "created_at >= ? AND")
		assert.Len(t, args, 3)
	})

	t.Run("lexicalPrefilterBound refuses both failure modes", func(t *testing.T) {
		// Year check: far out, but Add still moves it the right way.
		farOut := time.Unix(1<<62, 0)
		require.True(t, farOut.UTC().Add(maxStoredClockFaceOffset).After(farOut),
			"fixture must exercise the YEAR check, not the wrap check")
		_, ok := lexicalPrefilterBound(farOut, maxStoredClockFaceOffset)
		assert.False(t, ok, "a five-digit-plus year cannot be compared lexically")

		// Wrap check: Add overflows the internal int64 second count and moves the
		// bound BACKWARDS, which would turn an upper bound into a lower one.
		wraps := time.Unix(math.MaxInt64-unixToInternalSeconds, 0)
		require.False(t, wraps.UTC().Add(maxStoredClockFaceOffset).After(wraps),
			"fixture must actually overflow, or the wrap check is untested")
		_, ok = lexicalPrefilterBound(wraps, maxStoredClockFaceOffset)
		assert.False(t, ok, "an overflowing Add moves the bound the WRONG way")
	})
}

// explainQueryPlan runs EXPLAIN QUERY PLAN over a DryRun-built statement and
// returns the plan rows joined into one string.
func explainQueryPlan(t *testing.T, s *GormStorage, stmt *gorm.Statement) string {
	t.Helper()
	rows, err := s.db.Raw("EXPLAIN QUERY PLAN "+stmt.SQL.String(), stmt.Vars...).Rows()
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

// TestSearchJobs_WindowKeepsTheIndexRangeRestriction is the reason the loose
// bare-column prefilter exists, asserted instead of asserted-about.
//
// Wrapping created_at in strftime()/CASE makes it a computed value, and SQLite
// then cannot use it to restrict an index RANGE — it falls back to the equality
// prefix alone and filters the rest row by row. Measured on the real migrated
// schema at 200k rows over ~139 days with a one-day window (mean of 5 runs after
// 2 warm-ups):
//
//	                        bare (pristine)   CASE only    loose+CASE
//	count  queue=?+window        17us          4.443ms       133us
//	list   queue=?+window        22us          2.890ms        96us
//	count  status=?+window       54us         63.062ms       905us
//	list   status=?+window       28us         35.963ms       416us
//	count  window only           55us         56.889ms       959us
//
// That gap grows with the TABLE, while the loose form's cost grows with the
// WINDOW, so the clause is not a micro-optimisation. Deleting it changes no
// answer, which is exactly why the plan needs a test: nothing else in this suite
// can tell.
func TestSearchJobs_WindowKeepsTheIndexRangeRestriction(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("EXPLAIN QUERY PLAN output is SQLite-specific")
	}

	anchor := time.Now()
	for i := 0; i < 40; i++ {
		seedJobCreatedAt(t, ctx, s, "planq", fmt.Sprintf("j%d", i),
			anchor.Add(-time.Duration(i)*time.Hour))
	}
	require.NoError(t, s.db.WithContext(ctx).Exec("ANALYZE").Error)

	since := anchor.Add(-6 * time.Hour)
	until := anchor.Add(time.Hour)

	for _, tc := range []struct {
		name     string
		equality func(*gorm.DB) *gorm.DB
		index    string
	}{
		{"queue equality", func(q *gorm.DB) *gorm.DB { return q.Where("queue = ?", "planq") }, "idx_jobs_queue_created"},
		{"status equality", func(q *gorm.DB) *gorm.DB { return q.Where("status = ?", core.StatusPending) }, "idx_jobs_status_created"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			build := func(withWindow bool) *gorm.Statement {
				var jobs []*core.Job
				q := tc.equality(s.db.Session(&gorm.Session{DryRun: true}).Model(&core.Job{}))
				if withWindow {
					q = applyTimeWindow(s, q, "created_at", since, until)
				}
				return q.Order("created_at DESC").Limit(20).Find(&jobs).Statement
			}

			windowed := explainQueryPlan(t, s, build(true))
			t.Logf("plan with window: %s", windowed)
			assert.Contains(t, windowed, tc.index,
				"the window must not cost the equality its index")
			assert.Contains(t, windowed, "created_at>",
				"the loose bare clause must keep the created_at RANGE restriction; "+
					"without it SQLite reads the whole equality prefix and filters row by row")

			// The no-window plan proves the assertion above is about the window and
			// not about something the index does anyway.
			bare := explainQueryPlan(t, s, build(false))
			t.Logf("plan without window: %s", bare)
			assert.NotContains(t, bare, "created_at>",
				"control: with no window there is no range restriction to find")
		})
	}
}

// TestSearchJobs_CrossFaceWindowResolvesToMilliseconds pins the ONE accepted
// inaccuracy in this predicate, so it is a decision on record rather than a thing
// that could silently widen.
//
// The cross-face arm normalizes both sides through strftime('%f'), which renders
// MILLISECONDS. Two values less than 1ms apart that wear different offsets
// therefore collapse and compare EQUAL, so an inclusive bound can admit a row up
// to 1ms outside the window. The accepted shape is precise:
//
//   - the error is bounded by 1ms;
//   - it is OVER-inclusion in every case — the predicate never drops a row that
//     belongs in the window, which is the property callers depend on;
//   - it does not exist on the same-face path, which is exact to the nanosecond
//     and is what a single-zone deployment always takes.
//
// If a future change makes this coarser than 1ms, or makes it lose rows instead of
// admitting them, this test fails.
func TestSearchJobs_CrossFaceWindowResolvesToMilliseconds(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("strftime resolution is a SQLite storage property")
	}

	// Mid-millisecond so both a sub-ms nudge and a 1ms nudge stay inside the same
	// second, and neither rounds across a boundary by accident.
	instant := time.Date(2026, 5, 4, 3, 2, 1, 500000000, time.UTC)
	rowFace := time.FixedZone("plus0530", 5*3600+1800)
	boundFace := time.UTC
	seedJobCreatedAt(t, ctx, s, "msq", "row", instant.In(rowFace))

	count := func(f core.JobFilter) int64 {
		t.Helper()
		f.Queue = "msq"
		f.Limit = 50
		_, total, err := s.SearchJobs(ctx, f)
		require.NoError(t, err)
		return total
	}
	require.Equal(t, int64(1), count(core.JobFilter{}), "control: the row exists")

	for _, d := range []time.Duration{time.Nanosecond, time.Microsecond, 400 * time.Microsecond} {
		t.Run("accepted over-inclusion at "+d.String(), func(t *testing.T) {
			assert.Equal(t, int64(1), count(core.JobFilter{Since: instant.Add(d).In(boundFace)}),
				"ACCEPTED: cross-face comparisons resolve to 1ms, so a bound this "+
					"close after the row still admits it")
			assert.Equal(t, int64(1), count(core.JobFilter{Until: instant.Add(-d).In(boundFace)}),
				"ACCEPTED: and symmetrically on the upper bound")
		})
	}

	t.Run("exact at millisecond granularity", func(t *testing.T) {
		assert.Equal(t, int64(0), count(core.JobFilter{Since: instant.Add(time.Millisecond).In(boundFace)}),
			"1ms is the documented resolution: past it the bound must exclude")
		assert.Equal(t, int64(0), count(core.JobFilter{Until: instant.Add(-time.Millisecond).In(boundFace)}),
			"and symmetrically on the upper bound")
	})

	t.Run("never loses a row inside the window", func(t *testing.T) {
		for _, d := range []time.Duration{time.Nanosecond, time.Microsecond, 400 * time.Microsecond, time.Millisecond} {
			assert.Equal(t, int64(1), count(core.JobFilter{
				Since: instant.Add(-d).In(boundFace),
				Until: instant.Add(d).In(boundFace),
			}), "a window that brackets the row by %s must contain it", d)
		}
	})

	t.Run("the same-face path is unaffected and exact", func(t *testing.T) {
		seedJobCreatedAt(t, ctx, s, "msq2", "row", instant.In(boundFace))
		sameFace := func(f core.JobFilter) int64 {
			t.Helper()
			f.Queue = "msq2"
			f.Limit = 50
			_, total, err := s.SearchJobs(ctx, f)
			require.NoError(t, err)
			return total
		}
		require.Equal(t, int64(1), sameFace(core.JobFilter{Since: instant.In(boundFace)}))
		assert.Equal(t, int64(0), sameFace(core.JobFilter{Since: instant.Add(time.Nanosecond).In(boundFace)}),
			"one nanosecond is enough to exclude when the faces match")
	})
}
