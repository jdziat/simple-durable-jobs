package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedSlotOnLocalFace writes a concurrency_slots row EXACTLY as every release
// before this one wrote it: expires_at bearing the writing process's local UTC
// offset rather than a normalized one.
//
// This is the fixture the clock-face fixes were missing. Normalizing a write to
// UTC does nothing for rows already in the table, and every test in this suite
// starts from an EMPTY database — so a write-side-only fix is guaranteed to look
// correct in tests and be wrong in production, which is the one place the table is
// not empty. Seeding the old shape is the only way a test can tell the difference.
//
// GORM would normalize a time.Time through the driver, so the row is written as
// raw text to pin the exact bytes an older binary left behind.
func seedSlotOnLocalFace(t *testing.T, ctx context.Context, s *GormStorage, slot string, jobID core.UUID, expiresAt time.Time) {
	t.Helper()
	require.True(t, s.isSQLite, "the legacy-face fixture is a SQLite storage concern")
	// The driver's own rendering, offset suffix included — "…-07:00", "…+05:30".
	text := expiresAt.Format("2006-01-02 15:04:05.999999999-07:00")
	require.NoError(t, s.db.WithContext(ctx).Exec(
		`INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at) VALUES (?, ?, ?, ?)`,
		slot, jobID, "legacy-worker", text).Error)
}

// An upgraded database holds BOTH clock faces at once, and both directions of
// mis-compare are live defects.
//
// Rows written by any earlier release wear the writer's local offset; rows written
// by this one wear UTC; and during a rolling deploy both are being written
// concurrently, so no migration can fully close the window either. The comparison
// therefore has to be face-aware, which is what timeBoundPredicate provides.
//
// Without that, normalizing only the write side produces two distinct failures:
//
//   - WEST of UTC a LIVE legacy row sorts BELOW a UTC cutoff, so the sweep deletes
//     a slot that is still held and the cap silently stops capping. That is the
//     original defect — unfixed on every existing deployment.
//   - EAST of UTC an EXPIRED legacy row sorts ABOVE it, so it can never be
//     collected and blocks the cap permanently. A limit-1 cap admits ZERO jobs
//     until real time overtakes the offset — up to 14 hours at +14:00. That one is
//     a STALL the normalization INTRODUCED, and it is arguably worse than the
//     over-admission it replaced.
//
// Both are asserted here against a real legacy row, under whatever TZ the run is
// in. Vacuously green under TZ=UTC, like every clock-face guard in this package;
// the non-UTC CI legs are what make it a gate.
func TestLegacyLocalFacedSlotRowsAreHandledCorrectly(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("DB-clock backends compare native timestamps against the server clock; no face to disagree about")
	}

	t.Run("a LIVE legacy row still holds its slot and survives the sweep", func(t *testing.T) {
		slot := uniqueSlotName(t)
		holder := core.NewID()
		// Written by an older binary an instant ago, 45 minutes from expiring.
		seedSlotOnLocalFace(t, ctx, s, slot, holder, time.Now().Add(45*time.Minute))

		ok, err := s.TryAcquireConcurrencySlot(ctx, slot, core.NewID(), "worker-new", 1, time.Hour)
		require.NoError(t, err)
		assert.False(t, ok,
			"a LIVE legacy row must still count against the cap. Admitting here means "+
				"ConcurrencyCap silently stops capping on every database that existed "+
				"before this upgrade — the exact defect the write-side fix claims to close")

		swept, err := s.DeleteExpiredConcurrencySlots(ctx, time.Now().UTC())
		require.NoError(t, err)
		assert.Zero(t, swept,
			"the sweep deleted %d LIVE legacy row(s); west of UTC these sort below a UTC "+
				"cutoff and are collected while still held", swept)
	})

	t.Run("an EXPIRED legacy row frees its slot and can be collected", func(t *testing.T) {
		slot := uniqueSlotName(t)
		// Written by an older binary, expired an hour ago.
		seedSlotOnLocalFace(t, ctx, s, slot, core.NewID(), time.Now().Add(-time.Hour))

		ok, err := s.TryAcquireConcurrencySlot(ctx, slot, core.NewID(), "worker-new", 1, time.Hour)
		require.NoError(t, err)
		assert.True(t, ok,
			"an EXPIRED legacy row must not block the cap. Denying here is a STALL: east "+
				"of UTC the row sorts above the cutoff, so a limit-1 cap admits ZERO jobs "+
				"until real time overtakes the offset — up to 14h at +14:00")

		swept, err := s.DeleteExpiredConcurrencySlots(ctx, time.Now().UTC())
		require.NoError(t, err)
		assert.Positive(t, swept,
			"the expired legacy row must be collectable; if the sweep cannot see it, it "+
				"is uncollectable garbage that blocks its slot forever")
	})
}
