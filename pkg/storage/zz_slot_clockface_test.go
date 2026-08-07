package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slotExpiresText reads concurrency_slots.expires_at as raw TEXT — the bytes SQLite
// will actually compare, not what Go parses them back into. Parsing normalizes the
// face and would hide the whole defect.
func slotExpiresText(t *testing.T, ctx context.Context, s *GormStorage, slotName string, jobID core.UUID) string {
	t.Helper()
	var raw string
	require.NoError(t, s.db.WithContext(ctx).Raw(
		"SELECT CAST(expires_at AS TEXT) FROM concurrency_slots WHERE slot_name = ? AND job_id = ?",
		slotName, jobID).Scan(&raw).Error)
	return raw
}

// concurrency_slots.expires_at must wear ONE clock face regardless of the writing
// process's timezone, because the expiry sweep compares it lexically.
//
// TryAcquireConcurrencySlot wrote it from a bare time.Now() — the process-LOCAL face
// — while the worker's hourly sweep passes time.Now().UTC(). On SQLite there is no
// datetime type and timestamps are TEXT, so the comparison is a string compare:
//
//	stored under TZ=America/Los_Angeles   "2026-08-06 20:46:04.884334676-07:00"
//	sweep cutoff (UTC)                    "2026-08-07 03:01:04.885+00:00"
//
// The same instant, and the row is 45 minutes from expiring — but "2026-08-06…"
// sorts before "2026-08-07…", so the DELETE takes it and the cap it was enforcing
// silently stops capping.
//
// clock.go's nowWriteValue() godoc already describes this failure verbatim: "a
// local-offset string (e.g. '...-07:00') would mis-sort against a UTC ('...Z')
// cutoff across the offset and delete recent rows / keep stale ones." The convention
// existed and this path did not use it.
//
// Like the other clock-face guards in this package, this asserts the STORED FACE
// rather than any particular pair of rows, and like them it is RED under any
// negative-offset TZ and VACUOUSLY GREEN under TZ=UTC — which is what CI runners
// use. A CI leg with a non-UTC TZ is what turns this from decoration into a gate.
func TestConcurrencySlotWritesAFaceIndependentExpiresAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property; DB-clock backends route both ends to the server clock")
	}

	const slot = "scarce-external-resource"
	jobA := core.NewID()
	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobA, "worker-a", 1, 45*time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "the first job must get the only slot")

	raw := slotExpiresText(t, ctx, s, slot, jobA)
	_, hostOffset := time.Now().Zone()
	t.Logf("TZ offset %ds -> stored expires_at TEXT = %q", hostOffset, raw)

	assert.True(t, strings.HasSuffix(raw, "+00:00"),
		"expires_at must wear one face regardless of the writer's zone (got %q); the expiry "+
			"sweep compares this text lexically against a UTC cutoff", raw)

	// Renewal writes the same column on the same path and must agree with it — a
	// renew that re-introduces the local face would resurrect the bug on the hour.
	renewed, err := s.RenewConcurrencySlot(ctx, slot, jobA, 45*time.Minute)
	require.NoError(t, err)
	require.True(t, renewed, "the live slot must renew")
	rawRenewed := slotExpiresText(t, ctx, s, slot, jobA)
	assert.True(t, strings.HasSuffix(rawRenewed, "+00:00"),
		"RenewConcurrencySlot must write the same face as the acquire (got %q)", rawRenewed)
}

// The behavioural consequence, asserted separately from the face so a failure says
// which layer broke: a live slot must survive the routine sweep, and the cap must
// still cap afterwards.
//
// This is the user-visible invariant. ConcurrencyCap exists to protect a scarce
// external resource; if the sweep deletes the row, the cap admits unboundedly many
// jobs while the original holder is still running, and nothing reports it — the
// sweep returns err=nil, and the holder's renew returns renewed=false to a caller
// that discards the bool.
func TestConcurrencySweepDoesNotDestroyALiveSlot(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	const slot = "scarce-external-resource"
	const ttl = 45 * time.Minute
	jobA, jobB := core.NewID(), core.NewID()

	okA, err := s.TryAcquireConcurrencySlot(ctx, slot, jobA, "worker-a", 1, ttl)
	require.NoError(t, err)
	require.True(t, okA)

	// The cap holds BEFORE the sweep — without this the assertion below could pass
	// on a fixture where the cap never worked at all.
	okB, err := s.TryAcquireConcurrencySlot(ctx, slot, jobB, "worker-b", 1, ttl)
	require.NoError(t, err)
	require.False(t, okB, "cap=1 must reject a second job while the first holds the slot")

	// Exactly how pkg/worker's hourly GC calls it.
	swept, err := s.DeleteExpiredConcurrencySlots(ctx, time.Now().UTC())
	require.NoError(t, err, "the sweep reports success even when it destroys a live row")
	assert.Zero(t, swept,
		"the sweep deleted %d live slot row(s) with %s of TTL remaining", swept, ttl)

	okAfter, err := s.TryAcquireConcurrencySlot(ctx, slot, core.NewID(), "worker-c", 1, ttl)
	require.NoError(t, err)
	assert.False(t, okAfter,
		"ConcurrencyCap(1) admitted another job after the sweep — the cap has silently "+
			"stopped capping while the original holder is still running")
}
