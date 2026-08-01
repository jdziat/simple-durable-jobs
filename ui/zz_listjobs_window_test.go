package ui

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	storagepackage "github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
)

// setupServiceWithGormStorage returns a service backed by a REAL GormStorage on
// SQLite, so ListJobs exercises the actual SQL predicate rather than a mock that
// would happily ignore the filter it is handed.
func setupServiceWithGormStorage(t *testing.T) (*jobsService, *storagepackage.GormStorage, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	// One connection: with ":memory:" every pooled connection is a SEPARATE
	// database, so a second connection would silently see an empty schema.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)

	store := storagepackage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return newJobsService(store, nil, nil), store, db
}

// writerFaces returns the clock faces the fixtures write rows on: this process's
// own, plus two that no process zone can collapse onto it or onto the UTC face
// every timestamppb bound wears.
//
// Fixed absolute offsets, NOT time.Local plus a delta: a delta can land outside
// ±14:00, which is both wider than any tzdata zone and outside what SQLite's own
// date parser accepts, so the fixture would be testing a face no writer can
// produce. It is also not a hardcoded UTC-vs-local pair, because CI runs TZ=UTC
// where local already IS UTC and the fixture would quietly stop being cross-face.
func writerFaces(t *testing.T) map[string]*time.Location {
	t.Helper()
	faces := map[string]*time.Location{
		"local":     time.Local,
		"plus0530":  time.FixedZone("plus0530", 5*3600+1800),
		"minus1200": time.FixedZone("minus1200", -12*3600),
	}
	// At least one face must differ from the UTC face every bound wears, or the
	// table degenerates into a single-face fixture that passes on broken code.
	foreign := 0
	for _, face := range faces {
		if _, off := time.Now().In(face).Zone(); off != 0 {
			foreign++
		}
	}
	require.GreaterOrEqual(t, foreign, 2, "fixture must span faces other than UTC")
	return faces
}

// foreignFace returns one clock face that is guaranteed to differ from both this
// process's zone and the UTC face a timestamppb bound arrives on.
func foreignFace(t *testing.T) *time.Location {
	t.Helper()
	_, localOffset := time.Now().Zone()
	face := time.FixedZone("plus0530", 5*3600+1800)
	if localOffset == 5*3600+1800 {
		face = time.FixedZone("minus1200", -12*3600)
	}
	_, faceOffset := time.Now().In(face).Zone()
	require.NotEqual(t, localOffset, faceOffset, "fixture face must differ from the process zone")
	require.NotZero(t, faceOffset, "fixture face must differ from the UTC face bounds arrive on")
	return face
}

// TestListJobs_SinceUntilWindowSelectsByInstantAcrossWriterZones is the
// user-facing invariant for a window sent through the public Connect handler.
//
// timestamppb.AsTime is UNCONDITIONALLY UTC-faced. On SQLite created_at is stored
// as offset-suffixed TEXT wearing the face of the process that WROTE it, and
// compared LEXICALLY — so a bare comparison is wrong by the delta between the two
// faces, in whichever direction. Rows below are seeded at the SAME INSTANT on
// three different faces — this process's own, +05:30 and -12:00 — which is
// byte-for-byte what three differently-zoned writer processes would have stored.
// Every one of them must fall in the same window. Re-facing the bind instead
// satisfies at most one of the three.
func TestListJobs_SinceUntilWindowSelectsByInstantAcrossWriterZones(t *testing.T) {
	ctx := context.Background()
	svc, store, _ := setupServiceWithGormStorage(t)

	anchor := time.Now()
	faces := writerFaces(t)
	for name, face := range faces {
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			Type: "fresh-" + name, Queue: "windowq", CreatedAt: anchor.In(face),
		}))
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			Type: "ancient-" + name, Queue: "windowq", CreatedAt: anchor.Add(-48 * time.Hour).In(face),
		}))
	}

	total := func(req *jobsv1.ListJobsRequest) int64 {
		t.Helper()
		req.Queue = "windowq"
		resp, err := svc.ListJobs(ctx, connect.NewRequest(req))
		require.NoError(t, err)
		assert.Len(t, resp.Msg.Jobs, int(resp.Msg.Total), "page and total must agree")
		return resp.Msg.Total
	}

	assert.Equal(t, int64(3), total(&jobsv1.ListJobsRequest{
		Since: timestamppb.New(anchor.Add(-time.Hour)),
		Until: timestamppb.New(anchor.Add(time.Hour)),
	}), "every fresh job falls in [now-1h, now+1h] regardless of the face it was written on")

	assert.Equal(t, int64(0), total(&jobsv1.ListJobsRequest{
		Since: timestamppb.New(anchor.Add(time.Hour)),
	}), "since=now+1h must exclude every job")

	assert.Equal(t, int64(3), total(&jobsv1.ListJobsRequest{
		Until: timestamppb.New(anchor.Add(-time.Hour)),
	}), "until=now-1h must keep exactly the three 48h-old jobs")

	assert.Equal(t, int64(6), total(&jobsv1.ListJobsRequest{}),
		"an absent window must not narrow the result")
}

// TestListJobs_DeadLetteredWindowIsHonouredAndBoundsDeath pins two things about
// the branch status="dead-lettered" short-circuits into.
//
// First, the window must reach it at all: every other field on ListJobsRequest is
// forwarded to core.DeadLetterFilter, and since/until used to be silently dropped
// — eight status values narrowed the query and the ninth returned the whole
// table, with no way for the caller to tell.
//
// Second, it bounds dead_lettered_at, not created_at. This view is ordered
// dead_lettered_at DESC and answers "what died in this window", so the job born
// two days ago and killed a second ago is precisely the one it must return.
//
// The shipped dashboard sends no since/until on any request, so this is reached
// today only by programmatic Connect clients — which is why it is asserted
// through the real handler rather than at the storage layer alone.
func TestListJobs_DeadLetteredWindowIsHonouredAndBoundsDeath(t *testing.T) {
	ctx := context.Background()
	svc, store, db := setupServiceWithGormStorage(t)

	now := time.Now()
	die := func(typ string, createdAt, diedAt time.Time) {
		t.Helper()
		job := &core.Job{Type: typ, Queue: "dlqwindow", MaxRetries: 1}
		require.NoError(t, store.Enqueue(ctx, job))
		claimed, err := store.Dequeue(ctx, []string{"dlqwindow"}, "worker-1")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.NoError(t, store.Fail(ctx, claimed.ID, "worker-1", "boom", nil))
		dead, err := store.GetJob(ctx, claimed.ID)
		require.NoError(t, err)
		require.NotNil(t, dead.DeadLetteredAt, "fixture must actually be dead-lettered")
		require.NoError(t, db.Model(&core.Job{}).Where("id = ?", claimed.ID).
			Updates(map[string]any{"created_at": createdAt, "dead_lettered_at": diedAt}).Error)
	}

	// Written on a foreign clock face so the assertion is about instants, not
	// about the process happening to run in the zone the rows were written in.
	face := foreignFace(t)
	die("old-job-just-died", now.Add(-48*time.Hour).In(face), now.In(face))
	die("new-job-died-long-ago", now.In(face), now.Add(-48*time.Hour).In(face))

	list := func(req *jobsv1.ListJobsRequest) []*jobsv1.Job {
		t.Helper()
		req.Queue = "dlqwindow"
		req.Status = statusDeadLetteredUI
		resp, err := svc.ListJobs(ctx, connect.NewRequest(req))
		require.NoError(t, err)
		assert.Len(t, resp.Msg.Jobs, int(resp.Msg.Total), "page and total must agree")
		return resp.Msg.Jobs
	}

	jobs := list(&jobsv1.ListJobsRequest{
		Since: timestamppb.New(now.Add(-time.Hour)),
		Until: timestamppb.New(now.Add(time.Hour)),
	})
	require.Len(t, jobs, 1, "the dead-lettered branch must honour since/until")
	assert.Equal(t, "old-job-just-died", jobs[0].Type,
		"the window bounds when the job DIED, so a 48h-old job killed a moment ago is in")

	assert.Empty(t, list(&jobsv1.ListJobsRequest{Since: timestamppb.New(now.Add(time.Hour))}),
		"since=now+1h must exclude every dead-lettered job")
	assert.Len(t, list(&jobsv1.ListJobsRequest{Until: timestamppb.New(now.Add(-time.Hour))}), 1,
		"until=now-1h must keep only the job that died 48h ago")
	assert.Len(t, list(&jobsv1.ListJobsRequest{}), 2,
		"an absent window must not narrow the dead-letter result")
}

// TestListJobs_OpenEndedProtobufMaxUntilReturnsEveryRow is the public-API form of
// the bound range check: what a programmatic Connect client gets when it uses the
// natural "no upper bound" sentinel.
//
// timestamppb.IsValid accepts 9999-12-31T23:59:59.999999999Z, so it is a legal
// value on ListJobsRequest.until, and two separate mechanisms in the SQLite
// predicate turn it into a silent EMPTY PAGE if the bound is not range-checked
// first: strftime() rounds it past the end of its supported range and returns NULL
// (dropping every row on the cross-face arm — which is the arm a reader whose zone
// differs from the stored face always takes, so this one is zone-dependent), and
// adding the ±26h index-prefilter slop rolls the year to five digits, whose text
// sorts BELOW every real row (which drops every row in EVERY zone).
//
// Unpatched v4.6 answers this request correctly, so getting it wrong would be a
// regression, and a silent empty page is precisely the failure the window fix
// exists to remove.
func TestListJobs_OpenEndedProtobufMaxUntilReturnsEveryRow(t *testing.T) {
	ctx := context.Background()
	svc, store, _ := setupServiceWithGormStorage(t)

	anchor := time.Now()
	// Written on a foreign face so the CROSS-FACE arm is exercised whatever zone
	// the test process runs in; a same-face row hides the strftime NULL under
	// TZ=UTC.
	face := foreignFace(t)
	for _, typ := range []string{"a", "b"} {
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			Type: typ, Queue: "openq", CreatedAt: anchor.In(face),
		}))
	}

	total := func(req *jobsv1.ListJobsRequest) int64 {
		t.Helper()
		req.Queue = "openq"
		resp, err := svc.ListJobs(ctx, connect.NewRequest(req))
		require.NoError(t, err)
		assert.Len(t, resp.Msg.Jobs, int(resp.Msg.Total), "page and total must agree")
		return resp.Msg.Total
	}

	protoMax := timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC))
	require.True(t, protoMax.IsValid(), "the sentinel must be a value the API accepts")

	require.Equal(t, int64(2), total(&jobsv1.ListJobsRequest{}), "control: both jobs exist")
	assert.Equal(t, int64(2), total(&jobsv1.ListJobsRequest{Until: protoMax}),
		"an open-ended upper bound must return every row, not an empty page")
	assert.Equal(t, int64(2), total(&jobsv1.ListJobsRequest{
		Since: timestamppb.New(anchor.Add(-time.Hour)),
		Until: protoMax,
	}), "and it must not poison a real lower bound either")

	// The mirror must still EXCLUDE: an open-ended bound is not a licence to ignore
	// the direction it was given in.
	assert.Equal(t, int64(0), total(&jobsv1.ListJobsRequest{Since: protoMax}),
		"since=end-of-time must exclude every row")
}

// TestListJobs_DeadLetteredOpenEndedUntilReturnsEveryRow is the same check on the
// branch status="dead-lettered" short-circuits into, because that branch builds a
// different filter over a different column and could regress independently.
func TestListJobs_DeadLetteredOpenEndedUntilReturnsEveryRow(t *testing.T) {
	ctx := context.Background()
	svc, store, db := setupServiceWithGormStorage(t)

	now := time.Now()
	face := foreignFace(t)
	for _, typ := range []string{"a", "b"} {
		job := &core.Job{Type: typ, Queue: "dlqopen", MaxRetries: 1}
		require.NoError(t, store.Enqueue(ctx, job))
		claimed, err := store.Dequeue(ctx, []string{"dlqopen"}, "worker-1")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.NoError(t, store.Fail(ctx, claimed.ID, "worker-1", "boom", nil))
		require.NoError(t, db.Model(&core.Job{}).Where("id = ?", claimed.ID).
			Update("dead_lettered_at", now.In(face)).Error)
	}

	list := func(req *jobsv1.ListJobsRequest) int {
		t.Helper()
		req.Queue = "dlqopen"
		req.Status = statusDeadLetteredUI
		resp, err := svc.ListJobs(ctx, connect.NewRequest(req))
		require.NoError(t, err)
		return len(resp.Msg.Jobs)
	}

	protoMax := timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC))
	require.Equal(t, 2, list(&jobsv1.ListJobsRequest{}), "control: both jobs are dead-lettered")
	assert.Equal(t, 2, list(&jobsv1.ListJobsRequest{Until: protoMax}),
		"an open-ended upper bound must not empty the dead-letter page")
	assert.Equal(t, 0, list(&jobsv1.ListJobsRequest{Since: protoMax}),
		"and the mirrored direction must still exclude")
}
