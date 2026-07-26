package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SQLite has no datetime type: mattn/go-sqlite3 binds every time.Time as TEXT
// with a trailing offset ("...+00:00" for UTC — never "Z"), and SQLite compares
// those strings LEXICALLY. Every due-ness predicate binds this process's wall
// clock, carrying the LOCAL offset, so a run_at handed in on a different clock
// face mis-orders by the full delta between the zones.
//
// FALSE-GREEN TRAP: asserting the stored instant is equal passes with the bug
// present — the INSTANT was always right; it is the rendered CLOCK FACE that
// differed, and only the lexical comparison could see it. So the discriminating
// assertion is on the zone the value is written in.
func TestEnqueue_NormalizesRunAtToOneClockFace(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// A caller handing in UTC while the process runs in some other zone — the
	// common shape: queue.At(t) with a UTC t, or a parsed RFC 3339 "...Z".
	utcRunAt := time.Now().Add(2 * time.Hour).UTC()
	job := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", RunAt: &utcRunAt}
	require.NoError(t, s.Enqueue(ctx, job))

	require.NotNil(t, job.RunAt)
	assert.Equal(t, time.Local, job.RunAt.Location(),
		"run_at must be written on the same clock face as the due-ness binds, or SQLite's "+
			"lexical comparison mis-orders it by the delta between the zones")
	assert.True(t, job.RunAt.Equal(utcRunAt),
		"normalization must change the CLOCK FACE only — never the instant")
}

// The caller's Option must not be corrupted. job.RunAt aliases the *time.Time
// inside queue.At's Option, so writing through the pointer would mutate an
// Option the caller may reuse across enqueues.
func TestEnqueue_DoesNotMutateTheCallersRunAtValue(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	shared := time.Now().Add(time.Hour).UTC()
	before := shared

	for range 2 {
		job := &core.Job{ID: core.NewID(), Type: "t", Queue: "default", RunAt: &shared}
		require.NoError(t, s.Enqueue(ctx, job))
	}

	assert.Equal(t, before, shared,
		"normalization must REPOINT job.RunAt, not write through the pointer — the caller's "+
			"Option value is shared across enqueues")
	assert.Equal(t, time.UTC, shared.Location())
}

// offsetZone returns a fixed zone whose UTC offset is delta away from THIS
// process's local offset.
//
// Deriving the probe zones RELATIVE to time.Local is what makes these tests mean
// anything on CI. The obvious version hardcodes UTC as the "foreign" zone — and
// CI containers run TZ=UTC, where local already IS UTC, so the normalization is a
// no-op and the test passes against unfixed code. Anchoring to time.Local keeps
// the two clock faces genuinely different in every timezone.
func offsetZone(t *testing.T, delta time.Duration) *time.Location {
	t.Helper()
	_, localOff := time.Now().Zone()
	return time.FixedZone("probe", localOff+int(delta/time.Second))
}

func claimedTypes(t *testing.T, ctx context.Context, s *GormStorage, queue string) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for range 4 {
		j, err := s.Dequeue(ctx, []string{queue}, "w1")
		require.NoError(t, err)
		if j == nil {
			break
		}
		got[j.Type] = true
	}
	return got
}

// TestEnqueue_RunAtZoneDoesNotShiftEligibility pins the USER-FACING invariant: a
// job runs when it is due, and the timezone the caller happened to express
// run_at in does not move when that is.
func TestEnqueue_RunAtZoneDoesNotShiftEligibility(t *testing.T) {
	t.Run("past run_at in a zone ahead of local is still claimable", func(t *testing.T) {
		s := newTestStorage(t)
		ctx := context.Background()
		due := time.Now().Add(-time.Minute).In(offsetZone(t, 8*time.Hour))
		j := newTestJob("q", "due.job")
		j.RunAt = &due
		require.NoError(t, s.Enqueue(ctx, j))
		require.True(t, claimedTypes(t, ctx, s, "q")["due.job"],
			"run_at already passed -> claimable regardless of the caller's zone (else it fires ~8h LATE)")
	})

	t.Run("future run_at in a zone behind local is not claimable", func(t *testing.T) {
		s := newTestStorage(t)
		ctx := context.Background()
		future := time.Now().Add(time.Hour).In(offsetZone(t, -8*time.Hour))
		j := newTestJob("q", "future.job")
		j.RunAt = &future
		require.NoError(t, s.Enqueue(ctx, j))
		require.False(t, claimedTypes(t, ctx, s, "q")["future.job"],
			"run_at is an hour away -> NOT claimable regardless of the caller's zone (else it fires ~8h EARLY)")
	})
}

// TestFail_RetryAtZoneDoesNotShiftEligibility covers the second caller-supplied
// run_at entry point: core.Storage.Fail's retryAt.
func TestFail_RetryAtZoneDoesNotShiftEligibility(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	j := newTestJob("q", "retry.job")
	j.MaxRetries = 3
	require.NoError(t, s.Enqueue(ctx, j))
	claimed, err := s.Dequeue(ctx, []string{"q"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, claimed)

	// Reschedule an hour out, expressed 8h BEHIND local. It must not come back now.
	retryAt := time.Now().Add(time.Hour).In(offsetZone(t, -8*time.Hour))
	require.NoError(t, s.Fail(ctx, claimed.ID, "w1", "boom", &retryAt))

	require.False(t, claimedTypes(t, ctx, s, "q")["retry.job"],
		"a retry scheduled an hour out must not be re-claimable now regardless of the caller's zone")
}
