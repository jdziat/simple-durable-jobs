package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// failViaProduction drives the real terminal path — Enqueue, Dequeue, Fail with
// MaxRetries exhausted — so dead_lettered_at is written by production code and
// not by a test fixture. Nothing here chooses a clock face; that is the point.
func failViaProduction(t *testing.T, ctx context.Context, s *GormStorage, queue, typ string) core.UUID {
	t.Helper()
	job := &core.Job{Type: typ, Queue: queue, MaxRetries: 1}
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{queue}, "worker-r29")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, job.ID, got.ID)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-r29", "boom", nil))
	return job.ID
}

// storedText returns the raw bytes SQLite holds for one timestamp column.
func storedText(t *testing.T, ctx context.Context, s *GormStorage, column string, id core.UUID) string {
	t.Helper()
	var raw string
	require.NoError(t, s.db.WithContext(ctx).Raw(
		"SELECT CAST("+column+" AS TEXT) FROM jobs WHERE id = ?", id).Scan(&raw).Error)
	return raw
}

// seedDeadLetteredAt writes a dead-lettered row whose dead_lettered_at wears
// exactly the clock face of the supplied time — what a SECOND process (or an
// earlier release) stores.
func seedDeadLetteredAt(t *testing.T, ctx context.Context, s *GormStorage, queue, typ string, at time.Time) core.UUID {
	t.Helper()
	job := &core.Job{Type: typ, Queue: queue, CreatedAt: at}
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", job.ID).
		Updates(map[string]any{
			"status":             core.StatusFailed,
			"dead_lettered_at":   at,
			"dead_letter_reason": "seeded",
		}).Error)
	return job.ID
}

func dlqTypes(t *testing.T, ctx context.Context, s *GormStorage, filter core.DeadLetterFilter) []string {
	t.Helper()
	jobs, err := s.ListDeadLettered(ctx, filter)
	require.NoError(t, err)
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Type)
	}
	return out
}

func searchTypes(t *testing.T, ctx context.Context, s *GormStorage, filter core.JobFilter) []string {
	t.Helper()
	jobs, _, err := s.SearchJobs(ctx, filter)
	require.NoError(t, err)
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Type)
	}
	return out
}

// FINDING 1, the root of it. ListDeadLettered orders by dead_lettered_at, and on
// SQLite that ORDER BY is a lexical compare of offset-suffixed TEXT — so the sort
// is only an ordering of INSTANTS if every row shares one face. Storage.Fail used
// to write a bare time.Now(), which wears the writing process's offset: two faces
// across a DST fall-back in ONE worker, and one per zone across a fleet.
//
// This asserts the property that makes the bare ORDER BY sound rather than any
// particular pair of rows: the stored face does not depend on the host zone. It
// is RED under any non-UTC TZ and vacuously green under TZ=UTC, which is exactly
// the shape of the defect — there was nothing to fix for a UTC writer.
func TestR29_FailWritesAFaceIndependentDeadLetteredAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}

	id := failViaProduction(t, ctx, s, "faceq", "dlq.face")
	raw := storedText(t, ctx, s, "dead_lettered_at", id)
	_, hostOffset := time.Now().Zone()
	t.Logf("TZ offset %ds -> stored dead_lettered_at TEXT = %q", hostOffset, raw)

	assert.True(t, strings.HasSuffix(raw, "+00:00"),
		"dead_lettered_at must wear ONE face regardless of the writer's zone; "+
			"the DLQ's ORDER BY compares this text lexically")

	// completed_at is written in the SAME terminal UPDATE and already went through
	// nowWriteValue. If the two ever disagree again, the DLQ view is sorting on a
	// different axis than the column beside it.
	//
	// Compare the FACE SUFFIX of each string on its own. An earlier draft sliced
	// one column's text by the OTHER column's length —
	// `storedText(…,"completed_at",…)[len(raw)-6:]` — which is only the offset when
	// the two happen to be the same length. mattn/go-sqlite3 renders with
	// "2006-01-02 15:04:05.999999999-07:00", and the `.999999999` form drops
	// trailing zeros, so two timestamps taken microseconds apart routinely differ
	// in width: the slice then lands mid-timestamp and the assertion passes or
	// fails on the value of a digit.
	completedAt := storedText(t, ctx, s, "completed_at", id)
	assert.Equal(t, faceSuffix(t, raw), faceSuffix(t, completedAt),
		"dead_lettered_at (%q) and completed_at (%q) are written together and must share a face",
		raw, completedAt)
}

// The SECOND writer of dead_lettered_at. Fail's retry-exhausted branch and
// FailTerminalWithResult's fan-out-accounting transaction are different code with
// the same obligation, and one test that only reaches Fail would leave the other
// free to drift back onto the writer's local clock.
func TestR29_FailTerminalWithResultWritesTheSameFace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}

	job := &core.Job{Type: "dlq.terminal", Queue: "faceq2", MaxRetries: 5}
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{"faceq2"}, "worker-r29")
	require.NoError(t, err)
	require.NotNil(t, got)
	_, err = s.FailTerminalWithResult(ctx, got.ID, "worker-r29", "terminal boom")
	require.NoError(t, err)

	raw := storedText(t, ctx, s, "dead_lettered_at", job.ID)
	_, hostOffset := time.Now().Zone()
	t.Logf("TZ offset %ds -> stored dead_lettered_at TEXT = %q", hostOffset, raw)
	assert.True(t, strings.HasSuffix(raw, "+00:00"),
		"the terminal/fan-out writer must store the same single face as Fail")
}

// FINDING 1, the user-visible failure: a job that died LATER shows up BELOW one
// that died earlier, so the DLQ's whole reason for existing (what just died)
// lands on page 2.
//
// The "other worker" row is seeded on UTC because that is what another process
// running THIS code stores. Under a non-UTC TZ the production row used to wear
// the host offset and sorted below a row an hour older; now both are UTC.
func TestR29_DeadLetterNewestDeadFirstAgainstAnotherWorker(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}
	if _, off := time.Now().Zone(); off == 0 {
		t.Skip("needs a non-UTC host zone to have two faces in play")
	}

	// Another worker dead-lettered this one an hour ago.
	seedDeadLetteredAt(t, ctx, s, "dlqmix", "older-by-1h", time.Now().UTC().Add(-time.Hour))
	// This worker dead-letters one now, through production code.
	failViaProduction(t, ctx, s, "dlqmix", "newest")

	got := dlqTypes(t, ctx, s, core.DeadLetterFilter{Queue: "dlqmix"})
	require.Len(t, got, 2)
	assert.Equal(t, []string{"newest", "older-by-1h"}, got,
		"ListDeadLettered documents newest-dead-first; the job that died an hour LATER must lead")
}

// FINDING 1, severity: with the shipped Limit the newest dead job is pushed off
// page 1 of the triage view entirely.
func TestR29_NewestDeadJobStaysOnPageOne(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}
	if _, off := time.Now().Zone(); off == 0 {
		t.Skip("needs a non-UTC host zone to have two faces in play")
	}

	for i := 0; i < 2; i++ {
		seedDeadLetteredAt(t, ctx, s, "dlqpage", fmt.Sprintf("old-%d", i),
			time.Now().UTC().Add(-time.Duration(i+1)*time.Hour))
	}
	failViaProduction(t, ctx, s, "dlqpage", "NEWEST")

	page1 := dlqTypes(t, ctx, s, core.DeadLetterFilter{Queue: "dlqpage", Limit: 2})
	assert.Contains(t, page1, "NEWEST",
		"page 1 of the DLQ must hold the most recently dead job")
}

// A single worker's whole DLQ must read newest-first under any host zone. This is
// the invariant the write-side face buys; it is a guard, not a reproduction.
func TestR29_DeadLetterOrderIsMonotoneForOneWorker(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}

	const n = 5
	for i := 0; i < n; i++ {
		failViaProduction(t, ctx, s, "dlqmono", fmt.Sprintf("j%d", i))
	}
	got := dlqTypes(t, ctx, s, core.DeadLetterFilter{Queue: "dlqmono"})
	require.Len(t, got, n)
	assert.Equal(t, []string{"j4", "j3", "j2", "j1", "j0"}, got,
		"newest dead first, in the order they actually died")
}

// ACCEPTED RESIDUAL, stated so it cannot widen silently: rows dead-lettered by an
// EARLIER release keep the local face they were written on, and the bare ORDER BY
// still sorts those by wall face. They are not rewritten — a migration that
// rewrote stored timestamp text was tried in this repo and corrupted ordering on
// every SQLite database. They drain with retention.
func TestR29_LegacyMixedFaceDeadLetterRowsStillSortByWallFace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}
	plus7 := time.FixedZone("plus0700", 7*3600)

	seedDeadLetteredAt(t, ctx, s, "dlqlegacy", "legacy-older-by-5h",
		time.Date(2026, 8, 1, 7, 0, 0, 0, time.UTC).In(plus7))
	seedDeadLetteredAt(t, ctx, s, "dlqlegacy", "legacy-newest",
		time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))

	assert.Equal(t, []string{"legacy-older-by-5h", "legacy-newest"},
		dlqTypes(t, ctx, s, core.DeadLetterFilter{Queue: "dlqlegacy"}),
		"ACCEPTED: pre-existing mixed-face rows keep sorting by wall face. If this "+
			"ever reads newest-first, the ordering was made instant-correct and the "+
			"residual documented on deadLetterOrderColumn is stale")

	// The WINDOW over the same rows is instant-correct, and stays that way.
	total, err := s.CountDeadLettered(ctx, core.DeadLetterFilter{
		Queue:             "dlqlegacy",
		DeadLetteredSince: time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total,
		"the window selects by instant even for legacy rows — only the sort does not")
}

// FINDING 2, NOT FIXED. Pinned as an accepted residual with the reason, so the
// next reader finds the measurement instead of rediscovering the defect.
// See jobSortOrder for why the read-side normalization was rejected.
func TestR29_AcceptedResidual_SearchJobsSortsCreatedAtByWallFace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}
	plus7 := time.FixedZone("plus0700", 7*3600)

	seedJobCreatedAt(t, ctx, s, "sortface", "older-by-5h",
		time.Date(2026, 8, 1, 7, 0, 0, 0, time.UTC).In(plus7))
	seedJobCreatedAt(t, ctx, s, "sortface", "newest",
		time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))

	assert.Equal(t, []string{"older-by-5h", "newest"},
		searchTypes(t, ctx, s, core.JobFilter{Queue: "sortface", Limit: 50}),
		"ACCEPTED: created_at is compared as wall text. created_at cannot join the "+
			"single-face convention because it is the dequeue correctness fence "+
			"(COALESCE(run_at, created_at) <= <local bind>), and normalizing the "+
			"ORDER BY costs 554x — see jobSortOrder. If this ever reads newest-first, "+
			"that residual was closed and the godoc is stale")
}

// The same residual, reached by ONE worker in a DST zone rather than by two
// processes — which is what makes it more than a multi-process curiosity.
func TestR29_AcceptedResidual_SearchJobsSortInvertsAcrossADSTFallBack(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	// 2026-11-01 09:00Z is the PDT->PST fall-back; both faces are what ONE
	// America/Los_Angeles process's own time.Now() produces across it.
	earlier := time.Date(2026, 11, 1, 8, 59, 0, 0, time.UTC).In(la) // 01:59:00-07:00
	later := time.Date(2026, 11, 1, 9, 30, 0, 0, time.UTC).In(la)   // 01:30:00-08:00
	t.Logf("earlier wall face: %s", earlier.Format("2006-01-02 15:04:05-07:00"))
	t.Logf("later   wall face: %s", later.Format("2006-01-02 15:04:05-07:00"))

	seedJobCreatedAt(t, ctx, s, "sortdst", "earlier", earlier)
	seedJobCreatedAt(t, ctx, s, "sortdst", "later", later)

	assert.Equal(t, []string{"earlier", "later"},
		searchTypes(t, ctx, s, core.JobFilter{Queue: "sortdst", Limit: 50}),
		"ACCEPTED: inside the fall-back hour the two local offsets invert the sort")

	// The window over the same two rows is instant-correct. That asymmetry is the
	// whole finding: fixing one layer left the other behind.
	_, total, err := s.SearchJobs(ctx, core.JobFilter{
		Queue: "sortdst", Limit: 50,
		Since: time.Date(2026, 11, 1, 9, 15, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total,
		"the window already selects by instant across the fold; only the sort does not")
}

// FINDING 3 — representableBound range-checks the INSTANT but every comparison it
// guards is against the bound's RENDERED WALL text. A bound at the ceiling on a
// positive-offset face renders a five-digit year and hid every row.
func TestR29_UntilAtEndOfTimeOnAPositiveFaceKeepsEveryRow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("rendered-wall-text bounds are a SQLite storage property")
	}
	plus0530 := time.FixedZone("plus0530", 5*3600+1800)

	seedJobCreatedAt(t, ctx, s, "ceilq", "a", time.Date(2026, 5, 4, 3, 2, 1, 0, time.UTC))
	seedJobCreatedAt(t, ctx, s, "ceilq", "b", time.Date(2026, 5, 5, 3, 2, 1, 0, time.UTC))

	ceilOnFace := representableBoundCeil.In(plus0530)
	t.Logf("bound instant   = %s", ceilOnFace.UTC().Format(time.RFC3339Nano))
	t.Logf("bound renders as %q", ceilOnFace.Format("2006-01-02 15:04:05.999999999-07:00"))
	t.Logf("bound.After(representableBoundCeil) = %v", ceilOnFace.After(representableBoundCeil))

	_, total, err := s.SearchJobs(ctx, core.JobFilter{Queue: "ceilq", Limit: 50, Until: ceilOnFace})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total, "an upper bound at the end of time must not hide every row")

	// The mirrored lower bound is safe (year "0000" is still four digits), which is
	// why the symmetric-looking instant guard hid the upper-bound hole.
	floorOnFace := representableBoundFloor.In(time.FixedZone("minus0530", -(5*3600 + 1800)))
	t.Logf("floor bound renders as %q", floorOnFace.Format("2006-01-02 15:04:05.999999999-07:00"))
	_, total, err = s.SearchJobs(ctx, core.JobFilter{Queue: "ceilq", Limit: 50, Since: floorOnFace})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total, "control: the mirrored lower bound already keeps every row")
}

// The same hole through the dead-letter view, which takes its own window.
func TestR29_DeadLetterUntilAtEndOfTimeOnAPositiveFaceKeepsEveryRow(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("rendered-wall-text bounds are a SQLite storage property")
	}
	plus0530 := time.FixedZone("plus0530", 5*3600+1800)

	seedDeadLetteredAt(t, ctx, s, "ceildlq", "a", time.Date(2026, 5, 4, 3, 2, 1, 0, time.UTC))
	seedDeadLetteredAt(t, ctx, s, "ceildlq", "b", time.Date(2026, 5, 5, 3, 2, 1, 0, time.UTC))

	total, err := s.CountDeadLettered(ctx, core.DeadLetterFilter{
		Queue:             "ceildlq",
		DeadLetteredUntil: representableBoundCeil.In(plus0530),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total, "an upper bound at the end of time must not hide every dead row")
}

// The whole band, on REAL tzdata zones rather than a hand-built FixedZone: any
// positive face pushes an instant inside the last 14 hours of year 9999 out of
// the four-digit band. The control keeps the guard honest — a bound whose wall is
// still in 9999 was never broken, so a fix that "passes" by dropping every upper
// bound would fail here.
func TestR29_EndOfTimeBandOnRealZones(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("rendered-wall-text bounds are a SQLite storage property")
	}
	seedJobCreatedAt(t, ctx, s, "bandq", "a", time.Date(2026, 5, 4, 3, 2, 1, 0, time.UTC))
	seedJobCreatedAt(t, ctx, s, "bandq", "b", time.Date(2026, 5, 5, 3, 2, 1, 0, time.UTC))

	count := func(until time.Time) int64 {
		t.Helper()
		_, total, err := s.SearchJobs(ctx, core.JobFilter{Queue: "bandq", Limit: 50, Until: until})
		require.NoError(t, err)
		return total
	}

	for _, name := range []string{"Asia/Kolkata", "Pacific/Kiritimati", "Asia/Tokyo"} {
		loc, err := time.LoadLocation(name)
		require.NoError(t, err)
		bound := representableBoundCeil.In(loc)
		t.Logf("%-20s renders %q", name, bound.Format("2006-01-02 15:04:05.999-07:00"))
		assert.Equal(t, int64(2), count(bound), "%s: an end-of-time upper bound must keep every row", name)
	}

	// CONTROL: still inside year 9999, so it was correct before and must stay so —
	// and it must still EXCLUDE nothing while a genuinely restricting bound does.
	inBand := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC).In(time.FixedZone("plus0530", 5*3600+1800))
	t.Logf("control (wall still 9999) renders %q", inBand.Format("2006-01-02 15:04:05.999-07:00"))
	assert.Equal(t, int64(2), count(inBand), "control: a four-digit-year bound was never broken")
	assert.Equal(t, int64(1), count(time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)),
		"control: a real upper bound must still restrict — the fix must not simply drop every bound")
}

// faceSuffix returns the trailing "+HH:MM" / "-HH:MM" the SQLite driver always
// writes, taken from the END of the string rather than at a position computed
// from some other string's length. It fails loudly on anything that does not end
// in a face, so a layout change surfaces as a clear failure instead of a silent
// comparison of two mid-timestamp digits.
func faceSuffix(t *testing.T, stored string) string {
	t.Helper()
	if len(stored) < 6 {
		t.Fatalf("stored timestamp %q is too short to carry a clock face", stored)
	}
	face := stored[len(stored)-6:]
	if face[0] != '+' && face[0] != '-' {
		t.Fatalf("stored timestamp %q does not end in a +HH:MM / -HH:MM face", stored)
	}
	return face
}
