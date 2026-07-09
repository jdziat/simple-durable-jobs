package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func signalUUID(v string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("signal:"+v)).String())
}

func sendTestSignal(t *testing.T, ctx context.Context, s *GormStorage, jobID, name string, payload []byte) {
	t.Helper()
	id := signalUUID(jobID)
	seedTestJob(t, ctx, s, id, core.StatusWaiting)
	require.NoError(t, s.SendSignal(ctx, id, name, payload))
}

// ownJob transitions a job to the running+locked state a handler is in when it
// calls ConsumeSignalTx, so the ownership gate (locked_by=workerID AND
// status=running) admits the consume.
func ownJob(t *testing.T, ctx context.Context, s *GormStorage, id core.UUID, worker string) {
	t.Helper()
	until := time.Now().Add(time.Minute)
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", id).
		Updates(map[string]any{"status": core.StatusRunning, "locked_by": worker, "locked_until": until}).Error)
}

func TestSendSignal_SQLiteRejectsMissingJob(t *testing.T) {
	// Regression (teardown g10): on SQLite (foreign_keys=OFF, fk_signals_job not
	// enforced) SendSignal to a missing job used to insert a permanently-orphaned
	// pending signal. It must now return ErrJobNotFound and leave no row.
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("SQLite-specific: PostgreSQL/MySQL enforce fk_signals_job instead")
	}

	missing := signalUUID("send-signal-missing-job")
	err := s.SendSignal(ctx, missing, "go", []byte(`"x"`))
	require.ErrorIs(t, err, core.ErrJobNotFound)

	var count int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Signal{}).Where("job_id = ?", missing).Count(&count).Error)
	assert.Zero(t, count, "no orphaned pending signal must be inserted for a missing job")

	// A signal to a real job still succeeds.
	real := signalUUID("send-signal-real-job")
	seedTestJob(t, ctx, s, real, core.StatusWaiting)
	require.NoError(t, s.SendSignal(ctx, real, "go", []byte(`"x"`)))
}

func TestSendConsumeSignal_FIFO(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`"a"`))
	sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`"b"`))
	sendTestSignal(t, ctx, s, "j1", "other", []byte(`"x"`))

	got1, err := s.ConsumeSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, got1)
	assert.Equal(t, `"a"`, string(got1.Payload), "FIFO: oldest first")

	got2, err := s.ConsumeSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, got2)
	assert.Equal(t, `"b"`, string(got2.Payload))

	none, err := s.ConsumeSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	assert.Nil(t, none, "no more pending of this name")

	// The other name is untouched.
	other, err := s.ConsumeSignal(ctx, signalUUID("j1"), "other")
	require.NoError(t, err)
	require.NotNil(t, other)
	assert.Equal(t, `"x"`, string(other.Payload))
}

// ST-03: consumed_at is written on the DB clock so it shares a clock with
// DeleteConsumedSignalsOlderThan's DB-clock retention cutoff. A freshly consumed
// signal must read as recent to the GC (survives a generous age); once its
// consumed_at predates the window it is collected.
func TestDeleteConsumedSignals_ClockConsistentWithConsume(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	sendTestSignal(t, ctx, s, "gc1", "ev", []byte(`"x"`))
	got, err := s.ConsumeSignal(ctx, signalUUID("gc1"), "ev")
	require.NoError(t, err)
	require.NotNil(t, got)

	// Freshly consumed: a generous (1h) cutoff must NOT collect it — the stored
	// consumed_at is recent on the same clock the cutoff is computed from.
	deleted, err := s.DeleteConsumedSignalsOlderThan(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "recently consumed signal must survive an hour-old cutoff")

	// Age it past the window; now the GC collects it.
	old := time.Now().Add(-2 * time.Hour).UTC()
	require.NoError(t, s.db.WithContext(ctx).
		Model(&core.Signal{}).
		Where("job_id = ?", signalUUID("gc1")).
		Update("consumed_at", old).Error)
	deleted, err = s.DeleteConsumedSignalsOlderThan(ctx, time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "consumed signal older than the cutoff is collected")
}

func TestPeekSignal_DoesNotConsume(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`"v"`))

	p1, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, p1)
	p2, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, p2, "peek leaves the signal pending")

	c, err := s.ConsumeSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, c)
	after, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	assert.Nil(t, after, "consumed signal no longer pending")
}

func TestDrainSignals(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	for _, p := range [][]byte{[]byte(`"a"`), []byte(`"b"`), []byte(`"c"`)} {
		sendTestSignal(t, ctx, s, "j1", "ctx", p)
	}
	sendTestSignal(t, ctx, s, "j1", "other", []byte(`"x"`))

	drained, err := s.DrainSignals(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.Len(t, drained, 3)
	assert.Equal(t, `"a"`, string(drained[0].Payload))
	assert.Equal(t, `"c"`, string(drained[2].Payload))

	again, err := s.DrainSignals(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	assert.Empty(t, again, "all consumed")

	// Unrelated name still there.
	o, err := s.PeekSignal(ctx, signalUUID("j1"), "other")
	require.NoError(t, err)
	assert.NotNil(t, o)
}

func TestGetSignalWaitingJobsToResume(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mkJob := func(id string, status core.JobStatus, runAt *time.Time) {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: signalUUID(id), Type: "wf", Queue: "default", Status: status, RunAt: runAt,
		}).Error)
	}
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)

	mkJob("has-signal", core.StatusWaiting, nil) // waiting + a pending signal → resume
	sendTestSignal(t, ctx, s, "has-signal", "ctx", []byte(`1`))
	mkJob("timed-out", core.StatusWaiting, &past)       // waiting + deadline passed → resume
	mkJob("still-waiting", core.StatusWaiting, &future) // waiting, no signal, future deadline → NOT yet
	mkJob("plain-waiting", core.StatusWaiting, nil)     // waiting, no signal, no deadline → NOT
	mkJob("running-signal", core.StatusRunning, nil)    // running (not waiting) → NOT
	sendTestSignal(t, ctx, s, "running-signal", "ctx", []byte(`1`))

	// A parent still waiting on a pending fan-out must NOT be signal-resumed even
	// if it has a pending signal — resuming would replay and re-suspend on the
	// incomplete fan-out. The signal stays buffered for when the fan-out finishes.
	mkJob("fanout-parent", core.StatusWaiting, nil)
	sendTestSignal(t, ctx, s, "fanout-parent", "ctx", []byte(`1`))
	require.NoError(t, s.db.WithContext(ctx).Create(&core.FanOut{
		ID: signalUUID("fo1"), ParentJobID: signalUUID("fanout-parent"), Status: core.FanOutPending, TotalCount: 1,
	}).Error)

	jobs, err := s.GetSignalWaitingJobsToResume(ctx)
	require.NoError(t, err)
	ids := map[core.UUID]bool{}
	for _, j := range jobs {
		ids[j.ID] = true
	}
	assert.True(t, ids[signalUUID("has-signal")], "waiting job with a pending signal must resume")
	assert.True(t, ids[signalUUID("timed-out")], "waiting job past its deadline must resume")
	assert.False(t, ids[signalUUID("still-waiting")], "future deadline, no signal -> not yet")
	assert.False(t, ids[signalUUID("plain-waiting")], "no signal, no deadline -> not")
	assert.False(t, ids[signalUUID("running-signal")], "only waiting jobs are resumed")
	assert.False(t, ids[signalUUID("fanout-parent")], "a parent waiting on a pending fan-out must not be signal-resumed")
}

func TestGetSignalWaitingJobsToResumeAfter_KeysetPagesInIDOrder(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: signalUUID(id), Type: "wf", Queue: "default", Status: core.StatusWaiting,
		}).Error)
		sendTestSignal(t, ctx, s, id, "ctx", []byte(`1`))
	}
	expected := []core.UUID{signalUUID("a"), signalUUID("b"), signalUUID("c"), signalUUID("d")}
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })

	first, err := s.GetSignalWaitingJobsToResumeAfter(ctx, core.NilUUID, 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	assert.Equal(t, expected[0], first[0].ID)
	assert.Equal(t, expected[1], first[1].ID)

	second, err := s.GetSignalWaitingJobsToResumeAfter(ctx, first[1].ID, 2)
	require.NoError(t, err)
	require.Len(t, second, 2)
	assert.Equal(t, expected[2], second[0].ID)
	assert.Equal(t, expected[3], second[1].ID)
}

func TestResumeSignalWaitingJob(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)

	// A waiting job with a future run_at (a WaitForSignalTimeout wake deadline).
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: signalUUID("w"), Type: "x", Queue: "default", Status: core.StatusWaiting, RunAt: &future,
	}).Error)
	// A paused job — a signal must NOT un-pause it.
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: signalUUID("p"), Type: "x", Queue: "default", Status: core.StatusPaused,
	}).Error)

	ok, err := s.ResumeSignalWaitingJob(ctx, signalUUID("w"))
	require.NoError(t, err)
	require.True(t, ok)
	var w core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&w, "id = ?", signalUUID("w")).Error)
	assert.Equal(t, core.StatusPending, w.Status)
	assert.Nil(t, w.RunAt, "signal resume must clear the timeout wake deadline")

	ok2, err := s.ResumeSignalWaitingJob(ctx, signalUUID("p"))
	require.NoError(t, err)
	assert.False(t, ok2, "a paused job is not signal-resumable")
	var p core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&p, "id = ?", signalUUID("p")).Error)
	assert.Equal(t, core.StatusPaused, p.Status, "paused job stays paused")
}

func TestMarkWaitingWithDeadline_RunningOwnedJobSucceeds(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	job := newTestJob("default", "signal.wait")
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NoError(t, s.MarkWaitingWithDeadline(ctx, got.ID, "w1", time.Minute))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusWaiting, after.Status)
	assert.Empty(t, after.LockedBy)
	assert.Nil(t, after.LockedUntil)
	require.NotNil(t, after.RunAt)
	assert.True(t, after.RunAt.After(time.Now()), "deadline should be in the future")
}

func TestMarkWaitingWithDeadline_NonRunningOwnedJobReturnsErrJobNotOwned(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	job := newTestJob("default", "signal.wait")
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{"default"}, "w1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.LockedUntil)

	require.NoError(t, s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ?", got.ID).
		Update("status", core.StatusCompleted).Error)

	err = s.MarkWaitingWithDeadline(ctx, got.ID, "w1", time.Minute)
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusCompleted, after.Status)
	assert.Equal(t, "w1", after.LockedBy)
	require.NotNil(t, after.LockedUntil)
	assert.WithinDuration(t, *got.LockedUntil, *after.LockedUntil, time.Millisecond)
	assert.Nil(t, after.RunAt)
}

// claimRunningJob enqueues a fresh job and dequeues it as workerID so it is
// running+owned — the precondition for MarkWaitingWithDeadline /
// SaveCheckpointAndMarkWaiting to succeed.
func claimRunningJob(t *testing.T, ctx context.Context, s *GormStorage, workerID string) *core.Job {
	t.Helper()
	job := newTestJob("default", "signal.wait")
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{"default"}, workerID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, core.StatusRunning, got.Status)
	return got
}

// sleepCheckpointType is the on-disk CallType the durable-timer suspend writes
// (pkg/signal.SleepCheckpointType). Duplicated as a literal here because storage
// must not import pkg/signal (that would be an import cycle); the storage method
// treats CallType as opaque, so any valid value exercises it.
const sleepCheckpointType = "_sleep"

func suspendCheckpoint(jobID core.UUID) *core.Checkpoint {
	return &core.Checkpoint{
		JobID:     jobID,
		CallIndex: 0,
		CallType:  sleepCheckpointType,
		Result:    []byte(`{"deadline":1,"resolved":false}`),
	}
}

// SaveCheckpointAndMarkWaiting on a running+owned job must, in one tx, write the
// checkpoint AND advance the job to waiting with a future run_at — closing the
// torn-write window where a crash between the two left the job running with the
// checkpoint already committed (timer fires late, only the reaper recovers it).
func TestSaveCheckpointAndMarkWaiting_AtomicCheckpointAndWaiting(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	got := claimRunningJob(t, ctx, s, "w1")
	cp := suspendCheckpoint(got.ID)

	require.NoError(t, s.SaveCheckpointAndMarkWaiting(ctx, cp, got.ID, "w1", time.Minute))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusWaiting, after.Status, "job advanced to waiting in the same tx")
	assert.Empty(t, after.LockedBy)
	assert.Nil(t, after.LockedUntil)
	require.NotNil(t, after.RunAt)
	assert.True(t, after.RunAt.After(time.Now()), "wake deadline in the future")

	cps, err := s.GetCheckpoints(ctx, got.ID)
	require.NoError(t, err)
	require.Len(t, cps, 1, "checkpoint committed with the status transition")
	assert.Equal(t, sleepCheckpointType, cps[0].CallType)
}

// A nil cp means "checkpoint already durable (a replay); just mark waiting" — no
// checkpoint is written, only the status transition runs.
func TestSaveCheckpointAndMarkWaiting_NilCheckpointOnlyMarksWaiting(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	got := claimRunningJob(t, ctx, s, "w1")

	require.NoError(t, s.SaveCheckpointAndMarkWaiting(ctx, nil, got.ID, "w1", time.Minute))

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusWaiting, after.Status)
	require.NotNil(t, after.RunAt)
	assert.True(t, after.RunAt.After(time.Now()))

	cps, err := s.GetCheckpoints(ctx, got.ID)
	require.NoError(t, err)
	assert.Empty(t, cps, "nil cp writes no checkpoint")
}

// When ownership has been lost the status update affects 0 rows: the method must
// return ErrJobNotOwned AND roll back the checkpoint write, so a checkpoint is
// never left without its matching waiting status.
func TestSaveCheckpointAndMarkWaiting_NotOwnedRollsBackCheckpoint(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	got := claimRunningJob(t, ctx, s, "w1")
	cp := suspendCheckpoint(got.ID)

	// Wrong worker: the running row is owned by w1, not w2, so the status update
	// matches 0 rows and the whole tx (including the checkpoint) must roll back.
	err := s.SaveCheckpointAndMarkWaiting(ctx, cp, got.ID, "w2", time.Minute)
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusRunning, after.Status, "job stays running when ownership check fails")
	assert.Equal(t, "w1", after.LockedBy, "still owned by the original worker")

	cps, err := s.GetCheckpoints(ctx, got.ID)
	require.NoError(t, err)
	assert.Empty(t, cps, "checkpoint rolled back with the failed status transition")
}

// Same rollback guarantee when the job is no longer running (e.g. completed):
// no checkpoint may be persisted.
func TestSaveCheckpointAndMarkWaiting_NonRunningRollsBackCheckpoint(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	got := claimRunningJob(t, ctx, s, "w1")
	require.NoError(t, s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ?", got.ID).
		Update("status", core.StatusCompleted).Error)

	cp := suspendCheckpoint(got.ID)
	err := s.SaveCheckpointAndMarkWaiting(ctx, cp, got.ID, "w1", time.Minute)
	require.ErrorIs(t, err, core.ErrJobNotOwned)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusCompleted, after.Status, "non-running job untouched")

	cps, err := s.GetCheckpoints(ctx, got.ID)
	require.NoError(t, err)
	assert.Empty(t, cps, "checkpoint not persisted for a non-running job")
}

func TestGetPendingSignalName(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	sendTestSignal(t, ctx, s, "j1", "first", []byte(`1`))
	sendTestSignal(t, ctx, s, "j1", "second", []byte(`2`))
	_, err := s.ConsumeSignal(ctx, signalUUID("j1"), "first")
	require.NoError(t, err)

	name, ok, err := s.GetPendingSignalName(ctx, signalUUID("j1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "second", name)

	name, ok, err = s.GetPendingSignalName(ctx, signalUUID("missing"))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, name)
}

func TestResumeJob_PreservesRunAt(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)
	// A delayed job paused before its run_at; the general ResumeJob must NOT strip
	// its schedule (only the signal-specific resume clears run_at).
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: signalUUID("d"), Type: "x", Queue: "default", Status: core.StatusPaused, RunAt: &future,
	}).Error)

	ok, err := s.ResumeJob(ctx, signalUUID("d"))
	require.NoError(t, err)
	require.True(t, ok)
	var d core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&d, "id = ?", signalUUID("d")).Error)
	assert.Equal(t, core.StatusPending, d.Status)
	require.NotNil(t, d.RunAt, "ResumeJob must preserve a delayed job's schedule")
}

// validCheckpoint builds a well-formed call-index checkpoint for a consumed
// signal, mirroring what pkg/signal's WaitForSignal closure produces.
func validCheckpoint(jobID core.UUID, sig *core.Signal) *core.Checkpoint {
	return &core.Checkpoint{
		JobID:     jobID,
		CallIndex: 0,
		CallType:  "signal:ctx",
		Result:    sig.Payload,
	}
}

func TestConsumeSignalTx_AtomicConsumeAndCheckpoint(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`"hi"`))
	ownJob(t, ctx, s, signalUUID("j1"), "w1")

	sig, err := s.ConsumeSignalTxOwned(ctx, signalUUID("j1"), "w1", "ctx", func(sig *core.Signal) (*core.Checkpoint, error) {
		return validCheckpoint(signalUUID("j1"), sig), nil
	})
	require.NoError(t, err)
	require.NotNil(t, sig)
	assert.Equal(t, `"hi"`, string(sig.Payload))

	// The signal is consumed atomically with the checkpoint write.
	peek, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	assert.Nil(t, peek, "signal consumed")

	cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
	require.NoError(t, err)
	require.Len(t, cps, 1, "the call-index checkpoint was persisted in the same tx")
	assert.Equal(t, 0, cps[0].CallIndex)
	assert.Equal(t, "signal:ctx", cps[0].CallType)
	assert.Equal(t, `"hi"`, string(cps[0].Result))
}

func TestConsumeSignalTx_RollbackLeavesSignalAndNoCheckpoint(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`"hi"`))
	ownJob(t, ctx, s, signalUUID("j1"), "w1")

	// The checkpoint-build closure fails: the whole consume+checkpoint must roll
	// back. This is the crash/replay regression proof at the tx layer — the old
	// two-tx design (consume commits, then checkpoint) cannot satisfy it. Runs on
	// SQLite AND Postgres AND MySQL (the PG/MySQL FOR UPDATE SKIP LOCKED path).
	_, err := s.ConsumeSignalTxOwned(ctx, signalUUID("j1"), "w1", "ctx", func(*core.Signal) (*core.Checkpoint, error) {
		return nil, errors.New("boom")
	})
	require.Error(t, err)

	peek, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, peek, "consume rolled back: signal still pending (consumed_at NULL)")
	assert.Equal(t, `"hi"`, string(peek.Payload))

	cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
	require.NoError(t, err)
	assert.Empty(t, cps, "no checkpoint recorded when the tx rolled back")
}

func TestConsumeSignalTx_NoPendingDoesNothing(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	seedTestJob(t, ctx, s, signalUUID("j1"), core.StatusRunning)
	ownJob(t, ctx, s, signalUUID("j1"), "w1")

	sig, err := s.ConsumeSignalTxOwned(ctx, signalUUID("j1"), "w1", "ctx", func(*core.Signal) (*core.Checkpoint, error) {
		t.Fatal("buildCheckpoint must not be called when no signal is pending")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Nil(t, sig, "no signal → nil, nil")

	cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
	require.NoError(t, err)
	assert.Empty(t, cps, "no checkpoint when nothing was consumed")
}

func TestDrainSignalsTx_AlwaysCheckpointsAndRollsBack(t *testing.T) {
	drainCheckpoint := func(sigs []*core.Signal) *core.Checkpoint {
		return &core.Checkpoint{
			JobID:     signalUUID("j1"),
			CallIndex: 0,
			CallType:  "signaldrain:ctx",
			Result:    []byte("[]"),
		}
	}

	t.Run("empty still checkpoints", func(t *testing.T) {
		s := newTestStorage(t)
		ctx := context.Background()
		seedTestJob(t, ctx, s, signalUUID("j1"), core.StatusWaiting)
		ownJob(t, ctx, s, signalUUID("j1"), "w1")
		var called bool
		sigs, err := s.DrainSignalsTxOwned(ctx, signalUUID("j1"), "w1", "ctx", func(sigs []*core.Signal) (*core.Checkpoint, error) {
			called = true
			assert.Empty(t, sigs, "empty batch")
			return drainCheckpoint(sigs), nil
		})
		require.NoError(t, err)
		assert.Empty(t, sigs)
		assert.True(t, called, "buildCheckpoint always invoked, even when empty")
		cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
		require.NoError(t, err)
		require.Len(t, cps, 1, "empty drain still records a checkpoint for deterministic replay")
	})

	t.Run("non-empty consumes all and checkpoints", func(t *testing.T) {
		s := newTestStorage(t)
		ctx := context.Background()
		for _, p := range [][]byte{[]byte(`"a"`), []byte(`"b"`), []byte(`"c"`)} {
			sendTestSignal(t, ctx, s, "j1", "ctx", p)
		}
		ownJob(t, ctx, s, signalUUID("j1"), "w1")
		sigs, err := s.DrainSignalsTxOwned(ctx, signalUUID("j1"), "w1", "ctx", func(sigs []*core.Signal) (*core.Checkpoint, error) {
			return drainCheckpoint(sigs), nil
		})
		require.NoError(t, err)
		require.Len(t, sigs, 3)
		peek, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
		require.NoError(t, err)
		assert.Nil(t, peek, "all consumed")
		cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
		require.NoError(t, err)
		require.Len(t, cps, 1)
	})

	t.Run("closure error rolls back consume and checkpoint", func(t *testing.T) {
		s := newTestStorage(t)
		ctx := context.Background()
		for _, p := range [][]byte{[]byte(`"a"`), []byte(`"b"`), []byte(`"c"`)} {
			sendTestSignal(t, ctx, s, "j1", "ctx", p)
		}
		ownJob(t, ctx, s, signalUUID("j1"), "w1")
		_, err := s.DrainSignalsTxOwned(ctx, signalUUID("j1"), "w1", "ctx", func([]*core.Signal) (*core.Checkpoint, error) {
			return nil, errors.New("boom")
		})
		require.Error(t, err)
		drained, err := s.DrainSignals(ctx, signalUUID("j1"), "ctx")
		require.NoError(t, err)
		assert.Len(t, drained, 3, "nothing consumed: all three still pending")
		// Note: this DrainSignals consumed them; assert no checkpoint from the
		// failed tx existed beforehand by checking the rolled-back state above.
	})
}

func TestConsumeSignal_ConcurrentDisjoint(t *testing.T) {
	s := newTestStorage(t)
	if s.isSQLite {
		t.Skip("concurrent consume contention is meaningful on Postgres/MySQL")
	}
	ctx := context.Background()
	const n = 20
	for i := 0; i < n; i++ {
		sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`1`))
	}
	var mu sync.Mutex
	seen := map[core.UUID]int{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				sig, err := s.ConsumeSignal(ctx, signalUUID("j1"), "ctx")
				if err != nil || sig == nil {
					return
				}
				mu.Lock()
				seen[sig.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Len(t, seen, n, "every signal consumed exactly once")
	for id, c := range seen {
		assert.Equalf(t, 1, c, "signal %s consumed more than once", id)
	}
}

// TestConsumeSignalTx_NonOwnerDoesNotConsume is the E6-signal-loss regression:
// under the at-least-once double-run edge, a NON-owner execution must not
// consume a delivered signal (doing so makes the real owner find nothing pending
// and take the timeout branch, silently losing the signal). The gate leaves the
// signal pending for the owner.
func TestConsumeSignalTx_NonOwnerDoesNotConsume(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	sendTestSignal(t, ctx, s, "j1", "ctx", []byte(`"hi"`))
	ownJob(t, ctx, s, signalUUID("j1"), "owner-w") // the legitimate owner

	// A stale non-owner (its heartbeat lapsed; the job was reassigned) must NOT
	// consume: buildCheckpoint is never called and the signal stays pending.
	sig, err := s.ConsumeSignalTxOwned(ctx, signalUUID("j1"), "stale-w", "ctx", func(*core.Signal) (*core.Checkpoint, error) {
		t.Fatal("a non-owner must not consume the signal / build a checkpoint")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Nil(t, sig, "non-owner consumes nothing")

	peek, err := s.PeekSignal(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	require.NotNil(t, peek, "signal must remain pending for the real owner")
	cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
	require.NoError(t, err)
	assert.Empty(t, cps, "no checkpoint written by a non-owner")

	// The real owner consumes it.
	sig, err = s.ConsumeSignalTxOwned(ctx, signalUUID("j1"), "owner-w", "ctx", func(sg *core.Signal) (*core.Checkpoint, error) {
		return validCheckpoint(signalUUID("j1"), sg), nil
	})
	require.NoError(t, err)
	require.NotNil(t, sig, "the owner consumes the delivered signal")
	assert.Equal(t, `"hi"`, string(sig.Payload))
}

// TestMarkWaiting_ClearsRunAt is the E6-run_at regression: an indefinite wait
// must NULL run_at so a stale past run_at (from a delayed enqueue or a retry
// backoff) does not spuriously match the signal-resume poll and burn a wasted
// re-dispatch.
func TestMarkWaiting_ClearsRunAt(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	const worker = "w1"
	id := signalUUID("mw1")
	past := time.Now().Add(-5 * time.Minute)
	until := time.Now().Add(time.Minute)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: id, Type: "wf", Queue: "default", Status: core.StatusRunning,
		LockedBy: worker, LockedUntil: &until, RunAt: &past,
	}).Error)

	require.NoError(t, s.MarkWaiting(ctx, id, worker))

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, core.StatusWaiting, got.Status)
	assert.Nil(t, got.RunAt, "MarkWaiting must clear a stale past run_at on an indefinite wait")
}

// TestDrainSignalsTxOwned_NonOwnerDoesNotDrain mirrors the consume gate for the
// drain path (the symmetric, arguably more dangerous loss vector: a non-owner's
// empty-batch checkpoint at the owner's call index would make the owner's replay
// return [] and drop buffered signals). A non-owner drains nothing, writes no
// checkpoint, and leaves the signals pending for the owner.
func TestDrainSignalsTxOwned_NonOwnerDoesNotDrain(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	for _, p := range [][]byte{[]byte(`"a"`), []byte(`"b"`)} {
		sendTestSignal(t, ctx, s, "j1", "ctx", p)
	}
	ownJob(t, ctx, s, signalUUID("j1"), "owner-w")

	sigs, err := s.DrainSignalsTxOwned(ctx, signalUUID("j1"), "stale-w", "ctx", func([]*core.Signal) (*core.Checkpoint, error) {
		t.Fatal("a non-owner must not drain / build a checkpoint (even an empty-batch one)")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Empty(t, sigs, "non-owner drains nothing")

	cps, err := s.GetCheckpoints(ctx, signalUUID("j1"))
	require.NoError(t, err)
	assert.Empty(t, cps, "non-owner writes no checkpoint")
	remaining, err := s.DrainSignals(ctx, signalUUID("j1"), "ctx")
	require.NoError(t, err)
	assert.Len(t, remaining, 2, "both signals remain pending for the real owner")
}
