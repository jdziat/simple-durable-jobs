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

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
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

	sig, err := s.ConsumeSignalTx(ctx, signalUUID("j1"), "ctx", func(sig *core.Signal) (*core.Checkpoint, error) {
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

	// The checkpoint-build closure fails: the whole consume+checkpoint must roll
	// back. This is the crash/replay regression proof at the tx layer — the old
	// two-tx design (consume commits, then checkpoint) cannot satisfy it. Runs on
	// SQLite AND Postgres AND MySQL (the PG/MySQL FOR UPDATE SKIP LOCKED path).
	_, err := s.ConsumeSignalTx(ctx, signalUUID("j1"), "ctx", func(*core.Signal) (*core.Checkpoint, error) {
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

	sig, err := s.ConsumeSignalTx(ctx, signalUUID("j1"), "ctx", func(*core.Signal) (*core.Checkpoint, error) {
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
		var called bool
		sigs, err := s.DrainSignalsTx(ctx, signalUUID("j1"), "ctx", func(sigs []*core.Signal) (*core.Checkpoint, error) {
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
		sigs, err := s.DrainSignalsTx(ctx, signalUUID("j1"), "ctx", func(sigs []*core.Signal) (*core.Checkpoint, error) {
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
		_, err := s.DrainSignalsTx(ctx, signalUUID("j1"), "ctx", func([]*core.Signal) (*core.Checkpoint, error) {
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
