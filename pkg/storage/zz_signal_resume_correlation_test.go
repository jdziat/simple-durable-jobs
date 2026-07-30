package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// The signal-resume poll used to wake a waiting job whenever ANY unconsumed
// signal existed for it, with no correlation to the signal the handler is
// actually parked on. A pending signal the handler will never consume therefore
// made the poll resume the job on every tick (default 5s) forever: the job is
// re-dispatched, the handler replays from the top, its wait finds nothing and
// re-suspends, and the surplus signal is still pending for the next tick.
//
// Reachable without a typo: at-least-once producers are the documented contract,
// so a retried caller can deliver "a" twice while the handler has moved on to
// "b"; or a signal is sent early for a later phase. Each replay re-runs handler
// code that is not behind a Call or phase checkpoint and burns a dispatch plus a
// fleet rate-limit token.
//
// The fix records the name a job suspended on and correlates the poll against it.
// An EMPTY recorded name keeps the old permissive behaviour on purpose: fan-out
// suspends go through plain MarkWaiting, and a third-party core.Storage that does
// not implement the capability records nothing — in both cases waking on any
// pending signal is what preserves liveness.
func TestGetSignalWaitingJobsToResume_CorrelatesOnTheAwaitedName(t *testing.T) {
	ctx := context.Background()

	t.Run("a signal the job is not waiting for does not wake it", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, "b")
		seedPendingSignal(t, s, job, "a")

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.NotContains(t, resumeIDs(got), job,
			"the job is parked on \"b\" and only \"a\" is pending, so waking it just replays the handler and re-suspends — every poll tick, for the whole life of the wait")
	})

	t.Run("the awaited signal does wake it", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, "b")
		seedPendingSignal(t, s, job, "b")

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job,
			"correlating on the name must not cost liveness: the signal the handler is parked on has to wake it")
	})

	t.Run("a surplus signal alongside the awaited one still wakes it", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, "b")
		seedPendingSignal(t, s, job, "a")
		seedPendingSignal(t, s, job, "b")

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job)
	})

	t.Run("an already-consumed signal never wakes it", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, "b")
		consumed := time.Now()
		require.NoError(t, s.db.Create(&core.Signal{
			ID: core.NewID(), JobID: job, Name: "b", Payload: []byte(`"p"`),
			ConsumedAt: &consumed, CreatedAt: time.Now(),
		}).Error)

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.NotContains(t, resumeIDs(got), job)
	})

	// A durable sleep records the reserved sleep type, which validateName forbids as
	// a user signal name, so nothing can correlate with it and only run_at wakes it.
	t.Run("a sleeping job is not woken by a buffered signal", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, core.CheckpointTypeSleep)
		seedPendingSignal(t, s, job, "anything")

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.NotContains(t, resumeIDs(got), job,
			"GetSignalWaitingJobsToResumeAfter's own doc comment says durable timers with buffered signals must not resume before run_at; a buffered signal used to replay a sleeping job on every tick for the whole sleep")
	})

	// ...but its deadline still wakes it, so the sentinel costs no liveness.
	t.Run("a sleeping job is still woken by its deadline", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, core.CheckpointTypeSleep)
		past := time.Now().Add(-time.Minute)
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", job).
			Update("run_at", past).Error)

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job,
			"the run_at branch must still fire, or a durable sleep never wakes at all")
	})

	// Liveness for waits whose name we do not record: fan-out suspends use plain
	// MarkWaiting, and a custom core.Storage need not implement the capability.
	t.Run("no recorded name keeps the permissive behaviour", func(t *testing.T) {
		s := newTestStorage(t)
		job := seedWaitingOnSignal(t, ctx, s, "")
		seedPendingSignal(t, s, job, "anything")

		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job,
			"with no recorded name the poll must still wake on any pending signal, or an unrecorded wait never resumes")
	})
}

func seedWaitingOnSignal(t *testing.T, ctx context.Context, s *GormStorage, awaited string) core.UUID {
	t.Helper()
	id := core.NewID()
	require.NoError(t, s.db.Create(&core.Job{
		ID: id, Type: "wf", Queue: "default", Status: core.StatusWaiting,
		WaitingSignalName: awaited,
	}).Error)
	return id
}

func seedPendingSignal(t *testing.T, s *GormStorage, jobID core.UUID, name string) {
	t.Helper()
	require.NoError(t, s.db.Create(&core.Signal{
		ID: core.NewID(), JobID: jobID, Name: name, Payload: []byte(`"p"`),
		CreatedAt: time.Now(),
	}).Error)
}

func resumeIDs(jobs []*core.Job) []core.UUID {
	out := make([]core.UUID, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}

// TestMigrateJobsWaitingSignalName_UpgradePathKeepsParkedJobsWakeable models the
// upgrade rather than a fresh install: a job is already parked in `waiting` when
// the column does not exist yet.
//
// UPGRADE.md claims there is no window in which a waiting job becomes unwakeable.
// That rests entirely on a NOT NULL empty-string default backfilling every row to
// the empty string, which the resume poll reads as "not recorded" and handles
// permissively. If the column were ever added as NULL-able, or with a non-empty
// default, every job parked across the upgrade would stop being resumable by the
// signal it was waiting for — a silent, permanent wedge. This asserts the claim
// instead of trusting the DDL.
//
// WHAT ACTUALLY SHAPES THE COLUMN, measured, because it is not the obvious thing:
//
//	migration DDL made nullable          -> this test still PASSES
//	core.Job struct tag made nullable    -> this test FAILS
//
// Migrate runs db.AutoMigrate before the versioned migrations, so the column is
// created from the not-null/empty-default gorm tag on core.Job and
// migrateJobsWaitingSignalName's HasColumn check then no-ops. The versioned
// migration is the house belt-and-braces pattern (see migrateCheckpointsSpanEnd,
// which likewise "guarantees" a column AutoMigrate normally adds) and matters only
// where AutoMigrate is bypassed. Change the TAG and this test is what catches you.
func TestMigrateJobsWaitingSignalName_UpgradePathKeepsParkedJobsWakeable(t *testing.T) {
	// Two independent paths can supply this column, and BOTH must land it correctly
	// (on MySQL, with the right collation — see requireMySQLWaitingSignalNameCollation
	// for why a mismatch breaks every signal resume). Which one runs depends only on
	// what a given database has already recorded, so each is exercised.
	//
	//	pre-migration path — a database that has the jobs table but not the column
	//	                     and has not recorded the pre-migration. Adds it with the
	//	                     correct collation before AutoMigrate can get it wrong,
	//	                     avoiding a MySQL table rebuild.
	//	v39 repair path    — a database that already has the column with the wrong
	//	                     collation, because it reached the current schema before
	//	                     the pre-migration existed. MODIFYs it.
	//
	// What this does and does not prove: the two are a deliberately redundant pair,
	// so neither subtest fails if only ONE of them is broken — v39 alone still
	// lands the right column, and that is what makes the pre-migration safe to add.
	// The pre-migration's actual benefit is skipping a MySQL table rebuild, which is
	// a cost, not an observable, and is therefore not asserted here. What IS pinned
	// is that both orderings end with a correctly-shaped column and a wakeable
	// parked job — so a future change to either one cannot quietly produce a
	// database where the resume poll errors.
	for _, tc := range []struct {
		name           string
		clearPreLedger bool
	}{
		{name: "v39 repair path", clearPreLedger: false},
		{name: "pre-migration path", clearPreLedger: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := newTestStorage(t)

			// Return to the pre-v39 shape: drop the column AND its ledger row.
			// Dropping only the column does not model an upgrade — with v39 still
			// recorded as applied, Migrate skips it, AutoMigrate re-adds the column
			// with the jobs table default collation, and nothing repairs it. That is
			// how this test first failed on MySQL, and the lesson is that the
			// collation repair is one-shot: it corrects a column exactly once, on a
			// database that has not yet recorded v39.
			require.NoError(t, s.db.Migrator().DropColumn(&core.Job{}, "waiting_signal_name"),
				"drop the column to model a pre-v39 database")
			require.False(t, s.db.Migrator().HasColumn(&core.Job{}, "waiting_signal_name"))
			require.NoError(t, s.db.Where("version = ?", 39).
				Delete(&core.SchemaMigration{}).Error, "un-record v39 so the migration re-runs")
			if tc.clearPreLedger {
				require.NoError(t, s.db.Where("name = ?", "jobs_waiting_signal_name_collation").
					Delete(&core.PreMigration{}).Error, "un-record the pre-migration so it re-runs")
			}

			parked := core.NewID()
			require.NoError(t, s.db.Exec(
				"INSERT INTO jobs (id, type, queue, status) VALUES (?, ?, ?, ?)",
				parked, "wf", "default", core.StatusWaiting).Error)
			seedPendingSignal(t, s, parked, "approval")

			// Upgrade.
			require.NoError(t, s.Migrate(ctx))
			require.True(t, s.db.Migrator().HasColumn(&core.Job{}, "waiting_signal_name"),
				"the column must exist after the upgrade")

			var got string
			require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", parked).
				Pluck("waiting_signal_name", &got).Error)
			require.Equal(t, "", got,
				"a row that predates the column must backfill to the empty string, which is what keeps it on the permissive resume path")

			if !s.isSQLite && s.dialect() == dialectMySQL {
				requireMySQLWaitingSignalNameCollation(t, s.db)
			}

			resume, err := s.GetSignalWaitingJobsToResume(ctx)
			require.NoError(t, err)
			require.Contains(t, resumeIDs(resume), parked,
				"a job parked across the upgrade must still be resumed by its pending signal; if this fails the upgrade wedges every waiting job in the database")
		})
	}
}
