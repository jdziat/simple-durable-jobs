package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
)

// The chaos harness itself needs several worker PROCESSES against one database
// and cannot run on SQLite. Its INVARIANT CHECKS are a different thing: each is a
// SQL assertion over the ledger and the library's own schema, and until now not
// one of them was executed by any test. That is how INV-EXACTLY-ONCE came to be
// asserting over data its own fixture made impossible, how four checks lost their
// population guards, and how INV-SCHED passed on zero ticks.
//
// These tests run the REAL check functions against the REAL migrated schema and
// the REAL ensureLedger DDL on SQLite, and each one is written so that breaking
// what it pins turns it red.

func newHarnessDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/chaos.db?_journal_mode=WAL&_busy_timeout=10000&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	ctx := context.Background()
	store := jobs.NewGormStorage(db)
	require.NoError(t, store.Migrate(ctx), "migrate the library schema")
	require.NoError(t, ensureLedger(ctx, db, dialectSQLite), "create the chaos ledger")
	return db
}

// TestLedgerCanRecordADuplicateEffect is the A1 proof at the fixture level.
//
// The ledger used to be UNIQUE(job_id, marker). That did not merely make
// duplicate_effect_groups dead — it made the fixture MASK the defect the check
// exists to find: a job that really ran twice had its second effect INSERT
// rejected, so a genuine duplicate-execution bug surfaced as a handler error and
// the count still read 0.
func TestLedgerCanRecordADuplicateEffect(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	jobID := jobs.NewID()

	// A tx-paired effect, executed twice — two invocations, two nonces.
	require.NoError(t, insertEffectAttempt(ctx, db, jobID, "phase:extract", newAttemptNonce()))
	require.NoError(t, insertEffectAttempt(ctx, db, jobID, "phase:extract", newAttemptNonce()),
		"a re-executed exactly-once effect must be RECORDABLE; if the ledger rejects it "+
			"the duplicate can never be counted and INV-EXACTLY-ONCE is asserting over data "+
			"its own fixture forbids")

	var n int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM chaos_effects WHERE job_id = ? AND marker = 'phase:extract'`,
		string(jobID)).Scan(&n).Error)
	assert.EqualValues(t, 2, n)

	// And the at-least-once half is unchanged: a replay of a NON-tx-paired effect
	// still collides, exactly as before attempt_nonce existed. This is what keeps
	// duplicate_effect_groups from firing on the documented crash window.
	other := jobs.NewID()
	require.NoError(t, insertEffect(ctx, db, other, "done"))
	require.Error(t, insertEffect(ctx, db, other, "done"),
		"an at-least-once effect must still collide; if it did not, every SIGKILL replay "+
			"of chaos.unit/chaos.sub/pipeline_window would form a duplicate group and fail "+
			"INV-EXACTLY-ONCE on a healthy run")
}

// TestExactlyOnceRedensOnASeededDuplicate is the A1 proof at the CHECK level: the
// harness must go red when a real duplicate execution is present. This exact
// mutation survived the old harness.
func TestExactlyOnceRedensOnASeededDuplicate(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	jobID := jobs.NewID()

	require.NoError(t, insertEffectAttempt(ctx, db, jobID, "phase:extract", newAttemptNonce()))
	clean := checkExactlyOnce(ctx, db, dialectSQLite)
	require.True(t, clean.pass, "a single tx-paired effect is a clean run: %s", clean.detail)

	// The job ran its checkpointed phase a second time.
	require.NoError(t, insertEffectAttempt(ctx, db, jobID, "phase:extract", newAttemptNonce()))
	dirty := checkExactlyOnce(ctx, db, dialectSQLite)
	assert.False(t, dirty.pass,
		"a duplicate tx-paired effect must fail INV-EXACTLY-ONCE: %s", dirty.detail)
	assert.Contains(t, dirty.detail, "duplicate_effect_groups=1")
}

// TestExactlyOnceIgnoresTheDocumentedAtLeastOnceWindow is the false-fire guard.
// chaos.pipeline_window re-executes by design under SIGKILL; that must not read as
// an exactly-once violation.
func TestExactlyOnceIgnoresTheDocumentedAtLeastOnceWindow(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)

	// One healthy exactly-once effect, so the population guard is satisfied.
	require.NoError(t, insertEffectAttempt(ctx, db, jobs.NewID(), "phase:extract", newAttemptNonce()))

	// A window job that re-executed several times under crashes. Every one of these
	// is a documented at-least-once replay.
	windowJob := jobs.NewID()
	for i := 0; i < 4; i++ {
		_ = insertEffect(ctx, db, windowJob, "phase:extract")
		require.NoError(t, insertEffectIgnoreDuplicate(ctx, db, dialectSQLite, windowJob, "window-reexec:extract"))
	}
	inv := checkExactlyOnce(ctx, db, dialectSQLite)
	assert.True(t, inv.pass,
		"documented at-least-once re-execution must not fail the exactly-once invariant: %s", inv.detail)
}

// TestExactlyOnceFailsOnAnEmptyPopulation is the A2 guard for INV-EXACTLY-ONCE: a
// regression that stops the exactly-once handlers running leaves nothing to
// duplicate, and a check with no population must read as broken, not clean.
func TestExactlyOnceFailsOnAnEmptyPopulation(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	inv := checkExactlyOnce(ctx, db, dialectSQLite)
	assert.False(t, inv.pass, "an empty ledger must not report a clean exactly-once run: %s", inv.detail)
	assert.Contains(t, inv.detail, "atomic_effects=0")
}

// TestPopulationGuards covers A2 for the other three checks that lacked one: an
// EMPTIED population currently reads as PASS, which is how a regression that
// stops producing the data disappears from the gate.
func TestPopulationGuards(t *testing.T) {
	ctx := context.Background()

	t.Run("INV-FANOUT-COUNTS", func(t *testing.T) {
		db := newHarnessDB(t)
		empty := checkFanOutCounts(ctx, db)
		assert.False(t, empty.pass, "no fan-outs at all must not read as clean: %s", empty.detail)

		require.NoError(t, db.Exec(
			`INSERT INTO fan_outs (id, parent_job_id, total_count, completed_count, failed_count, cancelled_count, status, created_at, updated_at)
			 VALUES (?, ?, 2, 2, 0, 0, 'completed', ?, ?)`,
			jobs.NewID(), jobs.NewID(), time.Now(), time.Now()).Error)
		populated := checkFanOutCounts(ctx, db)
		assert.True(t, populated.pass, "a balanced fan-out is clean: %s", populated.detail)

		require.NoError(t, db.Exec(
			`INSERT INTO fan_outs (id, parent_job_id, total_count, completed_count, failed_count, cancelled_count, status, created_at, updated_at)
			 VALUES (?, ?, 5, 1, 0, 0, 'pending', ?, ?)`,
			jobs.NewID(), jobs.NewID(), time.Now(), time.Now()).Error)
		mismatched := checkFanOutCounts(ctx, db)
		assert.False(t, mismatched.pass, "a mismatched fan-out must fail: %s", mismatched.detail)
	})

	t.Run("INV-SLOT-NO-LEAK", func(t *testing.T) {
		db := newHarnessDB(t)
		empty := checkSlotNoLeak(ctx, db, dialectSQLite)
		assert.False(t, empty.pass,
			"an empty concurrency_slots table means the cap machinery never ran; that must "+
				"not be indistinguishable from a clean run: %s", empty.detail)

		// The per-key sentinel, which release never deletes.
		require.NoError(t, db.Exec(
			`INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at) VALUES ('chaos', ?, '', ?)`,
			jobs.NilUUID, time.Now().Add(-time.Hour).UTC()).Error)
		clean := checkSlotNoLeak(ctx, db, dialectSQLite)
		assert.True(t, clean.pass, "sentinel only is a clean run: %s", clean.detail)

		// A live slot still held by a job that is long gone.
		require.NoError(t, db.Exec(
			`INSERT INTO concurrency_slots (slot_name, job_id, worker_id, expires_at) VALUES ('chaos', ?, 'w1', ?)`,
			jobs.NewID(), time.Now().Add(time.Hour).UTC()).Error)
		leaked := checkSlotNoLeak(ctx, db, dialectSQLite)
		assert.False(t, leaked.pass, "a live non-sentinel slot is a leak: %s", leaked.detail)
	})

	t.Run("INV-RATE-WELLFORMED", func(t *testing.T) {
		db := newHarnessDB(t)
		empty := checkRateWellFormed(ctx, db, dialectSQLite)
		assert.False(t, empty.pass,
			"no rate-limit windows means the limiter never ran: %s", empty.detail)

		require.NoError(t, db.Exec(
			`INSERT INTO rate_limit_windows (limit_name, window_start, "count") VALUES ('chaos', ?, 7)`,
			time.Now().UTC()).Error)
		clean := checkRateWellFormed(ctx, db, dialectSQLite)
		assert.True(t, clean.pass, "a positive window is clean: %s", clean.detail)

		require.NoError(t, db.Exec(
			`INSERT INTO rate_limit_windows (limit_name, window_start, "count") VALUES ('chaos', ?, -1)`,
			time.Now().Add(time.Minute).UTC()).Error)
		negative := checkRateWellFormed(ctx, db, dialectSQLite)
		assert.False(t, negative.pass, "a negative window must fail: %s", negative.detail)
	})
}

// TestScheduleLowerBound is A3. The bound was one-sided, so a scheduler that fired
// ZERO times passed. checkSchedule measures a live window, so the window is driven
// here by inserting ticks concurrently rather than by waiting on a real scheduler.
func TestScheduleLowerBound(t *testing.T) {
	ctx := context.Background()

	t.Run("zero ticks fails", func(t *testing.T) {
		db := newHarnessDB(t)
		inv := checkScheduleWindow(ctx, db, 50*time.Millisecond, 5*time.Second)
		assert.False(t, inv.pass,
			"a scheduler that fires zero times in the window is dead, and this is the only "+
				"check watching for it: %s", inv.detail)
		assert.Contains(t, inv.detail, "ticks_in_50ms_window=0")
	})

	t.Run("one tick passes", func(t *testing.T) {
		db := newHarnessDB(t)
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = db.Exec(`INSERT INTO chaos_ticks DEFAULT VALUES`).Error
		}()
		inv := checkScheduleWindow(ctx, db, 200*time.Millisecond, 5*time.Second)
		assert.True(t, inv.pass, "a single fire in the window is a live scheduler: %s", inv.detail)
	})

	t.Run("too many ticks fails", func(t *testing.T) {
		db := newHarnessDB(t)
		go func() {
			time.Sleep(20 * time.Millisecond)
			for i := 0; i < 20; i++ {
				_ = db.Exec(`INSERT INTO chaos_ticks DEFAULT VALUES`).Error
			}
		}()
		inv := checkScheduleWindow(ctx, db, 300*time.Millisecond, 5*time.Second)
		assert.False(t, inv.pass, "N replicas each firing must still fail: %s", inv.detail)
	})
}

// TestWedgeInvariantsReadTheDrainObservation is A4. Both checks used to run over
// sets waitForDrain had already proven empty, so neither could fail at the point
// it ran.
func TestWedgeInvariantsReadTheDrainObservation(t *testing.T) {
	t.Run("drained is clean", func(t *testing.T) {
		obs := drainObservation{drained: true, polls: 12}
		assert.True(t, checkNoWedge(obs).pass)
		assert.True(t, checkReadyNoStuck(obs).pass)
	})

	t.Run("a workload that never quiesced fails", func(t *testing.T) {
		obs := drainObservation{drained: false, waiting: 3, running: 1, polls: 120}
		inv := checkNoWedge(obs)
		assert.False(t, inv.pass, "a drain that timed out with work in flight is the wedge: %s", inv.detail)
		assert.Contains(t, inv.detail, "last_waiting=3")
	})

	t.Run("a transient unready row does not fire", func(t *testing.T) {
		obs := drainObservation{drained: true, stuckStreak: stuckStreakLimit - 1, maxStuck: 4, polls: 60}
		assert.True(t, checkReadyNoStuck(obs).pass,
			"the promoter heals within a poll; a brief streak is normal and must not fail")
	})

	t.Run("a persistently unready row fails", func(t *testing.T) {
		obs := drainObservation{drained: true, stuckStreak: stuckStreakLimit, maxStuck: 1, polls: 60}
		inv := checkReadyNoStuck(obs)
		assert.False(t, inv.pass, "a row nothing heals is a latent wedge: %s", inv.detail)
	})
}

// TestStuckStreakCountsRowsNotPolls is the FALSE-FIRE guard on INV-READY-NO-STUCK,
// and the reason the streak is per row rather than per poll.
//
// On a busy chaos run, retried jobs are constantly becoming eligible and being
// promoted ~50ms later, so "at least one row was eligible-but-unready at this
// instant" is true on nearly EVERY 1-second poll of a perfectly healthy system. A
// poll-count streak would therefore run to the length of the whole drain and fail
// the release gate on exactly the workload it exists to run. Only the SAME row
// surviving poll after poll means anything.
func TestStuckStreakCountsRowsNotPolls(t *testing.T) {
	t.Run("churning rows never accumulate", func(t *testing.T) {
		streaks := map[string]int{}
		longest := 0
		// 100 polls, each seeing a DIFFERENT freshly-eligible row — the healthy
		// busy-run shape.
		for i := 0; i < 100; i++ {
			var got int
			streaks, got = foldStuckStreaks(streaks, []jobs.UUID{jobs.UUID(fmt.Sprintf("transient-%d", i))})
			if got > longest {
				longest = got
			}
		}
		assert.Equal(t, 1, longest,
			"a stream of different rows must never build a streak; if it does, every busy "+
				"chaos run fails INV-READY-NO-STUCK")
		assert.Less(t, longest, stuckStreakLimit)
	})

	t.Run("one persistent row accumulates", func(t *testing.T) {
		streaks := map[string]int{}
		longest := 0
		stuck := jobs.UUID("never-promoted")
		for i := 0; i < stuckStreakLimit; i++ {
			// The persistent row alongside churning company, which is what a real
			// poll returns.
			var got int
			streaks, got = foldStuckStreaks(streaks, []jobs.UUID{stuck, jobs.UUID(fmt.Sprintf("transient-%d", i))})
			if got > longest {
				longest = got
			}
		}
		assert.Equal(t, stuckStreakLimit, longest,
			"a row nothing heals must accumulate a streak even amid churn")
		assert.False(t, checkReadyNoStuck(drainObservation{stuckStreak: longest}).pass)
	})

	t.Run("a row that is finally promoted stops accumulating", func(t *testing.T) {
		streaks, _ := foldStuckStreaks(map[string]int{}, []jobs.UUID{"row"})
		streaks, _ = foldStuckStreaks(streaks, []jobs.UUID{"row"})
		require.Equal(t, 2, streaks["row"])
		streaks, longest := foldStuckStreaks(streaks, nil) // promoted: absent from the poll
		assert.Empty(t, streaks)
		assert.Zero(t, longest)
		// And it starts from scratch if it ever comes back.
		_, longest = foldStuckStreaks(streaks, []jobs.UUID{"row"})
		assert.Equal(t, 1, longest)
	})
}

// TestWaitForDrainRecordsWhatItSaw pins that the observation the wedge invariants
// read is actually populated by the drain loop — a struct the loop never fills
// would make both checks vacuous again, this time silently.
func TestWaitForDrainRecordsWhatItSaw(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)

	// A pending job that is eligible now but flagged unready: invisible to Dequeue.
	require.NoError(t, db.Exec(
		`INSERT INTO jobs (id, type, queue, status, args, attempt, max_retries, created_at, updated_at, priority, dq_ready)
		 VALUES (?, 'stuck', 'default', 'pending', x'7b7d', 0, 3, ?, ?, 0, 0)`,
		jobs.NewID(), time.Now().UTC(), time.Now().UTC()).Error)

	// A short drain: enough polls to prove the SAME row's streak is carried
	// forward by the loop, without paying stuckStreakLimit seconds of wall clock.
	// The threshold itself is exercised by TestStuckStreakCountsRowsNotPolls and
	// TestWedgeInvariantsReadTheDrainObservation.
	obs, err := waitForDrain(ctx, db, 6*time.Second, time.Second)
	require.NoError(t, err)
	assert.False(t, obs.drained, "a pending job that never runs must not read as drained")
	assert.GreaterOrEqual(t, obs.stuckStreak, 4,
		"the loop must carry one row's consecutive-poll streak forward; a streak that "+
			"resets every poll makes INV-READY-NO-STUCK unable to fail")
	assert.EqualValues(t, 1, obs.maxStuck)
	assert.False(t, checkNoWedge(obs).pass)
}

// TestReportingOnlyCheckIsNotNamedLikeAnInvariant is A5. The check hardcodes
// pass:true, so an INV- name put a permanent PASS in the release gate's report for
// something that asserts nothing.
func TestReportingOnlyCheckIsNotNamedLikeAnInvariant(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	inv := checkAtLeastOnceWindow(ctx, db)
	assert.True(t, inv.pass, "this check is reporting-only by design")
	assert.NotContains(t, inv.name, "INV-",
		"a check that can never fail must not be named like an invariant in the gate's report")
	assert.Contains(t, inv.detail, "reporting only")
}

// TestEveryHardInvariantCanFail is the meta-guard: it runs the whole report over a
// completely empty database and requires that EVERY HARD check reports FAIL.
//
// An empty database is the strongest possible regression — nothing ran at all —
// and before this campaign four HARD checks reported PASS on exactly that input.
// A new HARD check that cannot fail on an empty database is almost certainly
// missing its population guard, and this catches it at the moment it is added
// rather than at the release it was needed.
func TestEveryHardInvariantCanFail(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	obs := drainObservation{drained: false, stuckStreak: stuckStreakLimit}

	// The list production runs, not a copy — a check added to runAllChecks and not
	// to a hand-maintained list here would slip past this guard entirely.
	results := runAllChecks(ctx, db, dialectSQLite, obs, 50*time.Millisecond, 5*time.Second)

	hard := 0
	for _, inv := range results {
		if inv.level != "HARD" {
			continue
		}
		hard++
		assert.False(t, inv.pass,
			"%s reports PASS on a database where nothing ran; it is missing a population "+
				"guard and cannot detect a regression that empties its input (detail: %s)",
			inv.name, inv.detail)
	}
	assert.Positive(t, hard, "runAllChecks returned no HARD checks at all")
	t.Logf("%d HARD checks, all failing on an empty database", hard)
}

// TestCheckReportNamesAreStable keeps the printed report width honest: the report
// pads names to a fixed column, and a name longer than the column silently ruins
// the alignment operators read the gate through.
func TestCheckReportNamesAreStable(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	obs := drainObservation{drained: true}
	for _, inv := range []invariant{
		checkExactlyOnce(ctx, db, dialectSQLite),
		checkAtLeastOnceWindow(ctx, db),
		checkNoWedge(obs),
		checkReadyNoStuck(obs),
	} {
		assert.LessOrEqual(t, len(inv.name), 28, "%s exceeds the report's name column", inv.name)
	}
}

func TestEnsureLedgerIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	require.NoError(t, ensureLedger(ctx, db, dialectSQLite), "ensureLedger runs in every process")
	require.NoError(t, insertEffect(ctx, db, jobs.NewID(), "done"))
}

func TestResetHarnessDataClearsTheLedger(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)
	for i := 0; i < 3; i++ {
		require.NoError(t, insertEffect(ctx, db, jobs.NewID(), fmt.Sprintf("m%d", i)))
	}
	require.NoError(t, resetHarnessData(ctx, db, dialectSQLite))
	var n int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM chaos_effects`).Scan(&n).Error)
	assert.Zero(t, n)
}

// TestPopulationGuardsDoNotRedenAHealthyRun is the FALSE-FIRE guard on the guards.
//
// A population guard turns "the data is absent" from PASS into FAIL, which is
// only an improvement if the data is genuinely present on a healthy run. Two of
// them read tables whose rows are DELETED on release — concurrency_slots and
// rate_limit_windows — so "it must be non-empty after the workload drains" is a
// claim about the library, not about the harness, and it has to be checked
// against a real worker rather than reasoned about.
//
// This runs a worker configured exactly as runWorker configures the chaos
// workers (ConcurrencyCap("chaos", 64), RateLimit("chaos", 1000)), drains a small
// workload, and requires both checks to PASS. If a future change stops the
// per-slot sentinel from persisting, or GCs the last rate window, this fails here
// instead of turning the whole release gate red on a healthy chaos run.
func TestPopulationGuardsDoNotRedenAHealthyRun(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)

	q := jobs.New(jobs.NewGormStorage(db))
	done := make(chan struct{}, 8)
	q.Register("pop.job", func(context.Context, struct{}) error {
		done <- struct{}{}
		return nil
	})
	const n = 3
	for i := 0; i < n; i++ {
		_, err := q.Enqueue(ctx, "pop.job", struct{}{}, jobs.Retries(0))
		require.NoError(t, err)
	}

	w := jobs.NewWorker(q,
		jobs.Concurrency(8),
		jobs.ConcurrencyCap("chaos", 64),
		jobs.RateLimit("chaos", 1000),
		jobs.WithPollInterval(50*time.Millisecond),
	)
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	go func() { _ = w.Start(runCtx) }()
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("workload did not drain")
		}
	}
	// Let the deferred slot release land before sampling.
	require.Eventually(t, func() bool {
		var live int64
		return db.Raw(`SELECT count(*) FROM concurrency_slots WHERE job_id <> ?`, jobs.NilUUID).
			Scan(&live).Error == nil && live == 0
	}, 8*time.Second, 100*time.Millisecond, "slots should all be released after the drain")
	cancel()

	slotInv := checkSlotNoLeak(ctx, db, dialectSQLite)
	assert.True(t, slotInv.pass,
		"the slot population guard must not fire on a healthy drained run: %s", slotInv.detail)
	rateInv := checkRateWellFormed(ctx, db, dialectSQLite)
	assert.True(t, rateInv.pass,
		"the rate population guard must not fire on a healthy drained run: %s", rateInv.detail)
}

// TestWindowCheckpointJoinActuallyMatches guards the third sub-check of
// INV-EXACTLY-ONCE against being silently dead.
//
// That sub-check is a cross-type join: chaos_effects.job_id is canonical UUID
// TEXT while checkpoints.job_id is the native UUID column (uuid on Postgres,
// binary(16) on MySQL, a blob on SQLite). It was already dead once — from the v3
// binary-UUID migration until the collation/cast fix — because a join that never
// matches reports zero, which is indistinguishable from a clean run.
//
// So it is not enough for the query to RUN. It has to match when the defect is
// present: a window re-exec marker whose phase checkpoint exists means the
// checkpoint committed and the phase re-executed anyway.
func TestWindowCheckpointJoinActuallyMatches(t *testing.T) {
	ctx := context.Background()
	db := newHarnessDB(t)

	jobID := jobs.NewID()
	// One atomic effect so the population guard is satisfied either way.
	require.NoError(t, insertEffectAttempt(ctx, db, jobs.NewID(), "phase:x", newAttemptNonce()))
	require.NoError(t, insertEffect(ctx, db, jobID, "window-reexec:extract"))

	clean := checkExactlyOnce(ctx, db, dialectSQLite)
	require.True(t, clean.pass,
		"a re-exec marker with NO checkpoint is the documented at-least-once window: %s", clean.detail)

	// The same phase's checkpoint (call_index -1 is the phase-checkpoint key,
	// call_type is the bare phase name).
	require.NoError(t, db.Exec(
		`INSERT INTO checkpoints (id, job_id, call_index, call_type, result, created_at)
		 VALUES (?, ?, -1, 'extract', x'226f6b22', CURRENT_TIMESTAMP)`,
		jobs.NewID(), jobID).Error)

	dirty := checkExactlyOnce(ctx, db, dialectSQLite)
	assert.False(t, dirty.pass,
		"a re-exec whose checkpoint HAD committed must fail; if this passes the join is "+
			"not matching and the sub-check is dead: %s", dirty.detail)
	assert.Contains(t, dirty.detail, "checkpointed_reexec_markers=1")
}
