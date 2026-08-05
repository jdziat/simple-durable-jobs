package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

// TestTryAcquireConcurrencySlot_TakesTheSentinelRowLock covers the sentinel
// `FOR UPDATE` in TryAcquireConcurrencySlot, which had NO effective coverage:
// removing it left ./pkg/storage green on SQLite AND on live Postgres.
//
// FALSE-GREEN TRAP, and TestConcurrencySlotConcurrentLastSlotRace is it. That
// test exists precisely to guard this lock — cap 2, one slot pre-taken, two
// racers for the last one, "exactly one racer should acquire the final slot".
// It cannot detect the lock's removal. Measured on live Postgres with the
// clause deleted:
//
//	2 racers,  20 consecutive runs  -> PASS every time
//	8 racers,  25 rounds            -> PASS every time, worst case 1 winner
//
// The natural window between the live-holder COUNT and the INSERT is a few
// hundred microseconds, so the racers essentially never straddle it. The lock
// is nonetheless load-bearing; widening that window to 60ms proves it:
//
//	lock REMOVED  + 60ms window -> OVER-ADMISSION in 25 of 25 rounds (2 winners for 1 slot)
//	lock RESTORED + 60ms window -> 1 winner in 25 of 25 rounds
//
// So the protection is real and the pre-existing race test is simply too weak
// to lose the race. Rather than hammer-and-hope with more racers — which would
// be flaky and, on MySQL, vacuous (see below) — this asserts the lock is taken
// at all, deterministically: hold the sentinel row lock from the test's own
// transaction and require that admission BLOCKS behind it. cap=2 with a single
// holder leaves a free slot, so a block can only be the lock and never the cap.
//
// WHERE THIS TEST DISCRIMINATES, MEASURED, because "it passes on both dialects"
// is not the same as "it covers both dialects":
//
//	Postgres — DISCRIMINATING. Drop the clause and this test fails: admission
//	  sails through in ~13ms and returns acquired=true, because ON CONFLICT DO
//	  NOTHING does not wait on the conflicting row's lock and the follower's own
//	  INSERT uses a different job_id, so nothing else contends.
//	MySQL — NOT DISCRIMINATING, and the test still passes with the clause
//	  removed (blocks the full 900ms). InnoDB locks rows it SCANS rather than
//	  only rows it modifies, so the sentinel upsert contends on the index
//	  regardless of the explicit clause. On MySQL this clause is belt-and-braces;
//	  the Postgres leg is what pins it. Same asymmetry as
//	  TestEnqueueScheduledFire_PriorFireReadTakesTheRowLock.
//	SQLite — skipped. lockForUpdate returns the query unchanged there, so the
//	  mechanism does not exist to observe. SQLite's own writer serialization
//	  (WAL + txlock=immediate) substitutes for it, which is why
//	  newConcurrentTestStorage's race test passes on SQLite either way.
//
// The property under test is what the comment in concurrency.go claims: every
// contender for one slotName locks a concrete shared row BEFORE it counts live
// holders, so two contenders cannot both observe the same free slot and both
// insert. Without it the cap silently over-admits on Postgres.
func TestTryAcquireConcurrencySlot_TakesTheSentinelRowLock(t *testing.T) {
	s := newTestStorage(t)
	if s.isSQLite {
		t.Skip("lockForUpdate is a no-op on SQLite, so the sentinel lock cannot be observed there")
	}
	ctx := context.Background()
	slot := uniqueSlotName(t)

	// cap=2 with one holder leaves a slot free, so whatever blocks the second
	// acquire below is the LOCK and not the cap check.
	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, core.NewID(), "holder", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	tx := s.db.Begin()
	require.NoError(t, tx.Error)
	defer func() { _ = tx.Rollback() }()
	var held core.ConcurrencySlot
	require.NoError(t, tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("slot_name = ? AND job_id = ?", slot, core.NilUUID).
		First(&held).Error, "hold the sentinel row lock")

	const budget = 900 * time.Millisecond
	tctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	start := time.Now()
	acquired, aerr := s.TryAcquireConcurrencySlot(tctx, slot, core.NewID(), "contender", 2, time.Hour)
	elapsed := time.Since(start)

	require.Error(t, aerr,
		"admission must block on the held sentinel lock (it returned in %s); sailing through means the FOR UPDATE is gone and two contenders can both count the same free slot and both insert, over-admitting past the cap",
		elapsed.Round(time.Millisecond))
	require.False(t, acquired, "a contender that never took the lock must not have been admitted")
	require.GreaterOrEqual(t, elapsed, budget-100*time.Millisecond,
		"it must actually have waited on the lock rather than failing fast for some unrelated reason")
}
