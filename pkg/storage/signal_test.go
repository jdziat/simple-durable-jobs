package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

func TestSendConsumeSignal_FIFO(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	require.NoError(t, s.SendSignal(ctx, "j1", "ctx", []byte(`"a"`)))
	require.NoError(t, s.SendSignal(ctx, "j1", "ctx", []byte(`"b"`)))
	require.NoError(t, s.SendSignal(ctx, "j1", "other", []byte(`"x"`)))

	got1, err := s.ConsumeSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	require.NotNil(t, got1)
	assert.Equal(t, `"a"`, string(got1.Payload), "FIFO: oldest first")

	got2, err := s.ConsumeSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	require.NotNil(t, got2)
	assert.Equal(t, `"b"`, string(got2.Payload))

	none, err := s.ConsumeSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	assert.Nil(t, none, "no more pending of this name")

	// The other name is untouched.
	other, err := s.ConsumeSignal(ctx, "j1", "other")
	require.NoError(t, err)
	require.NotNil(t, other)
	assert.Equal(t, `"x"`, string(other.Payload))
}

func TestPeekSignal_DoesNotConsume(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	require.NoError(t, s.SendSignal(ctx, "j1", "ctx", []byte(`"v"`)))

	p1, err := s.PeekSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	require.NotNil(t, p1)
	p2, err := s.PeekSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	require.NotNil(t, p2, "peek leaves the signal pending")

	c, err := s.ConsumeSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	require.NotNil(t, c)
	after, err := s.PeekSignal(ctx, "j1", "ctx")
	require.NoError(t, err)
	assert.Nil(t, after, "consumed signal no longer pending")
}

func TestDrainSignals(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	for _, p := range [][]byte{[]byte(`"a"`), []byte(`"b"`), []byte(`"c"`)} {
		require.NoError(t, s.SendSignal(ctx, "j1", "ctx", p))
	}
	require.NoError(t, s.SendSignal(ctx, "j1", "other", []byte(`"x"`)))

	drained, err := s.DrainSignals(ctx, "j1", "ctx")
	require.NoError(t, err)
	require.Len(t, drained, 3)
	assert.Equal(t, `"a"`, string(drained[0].Payload))
	assert.Equal(t, `"c"`, string(drained[2].Payload))

	again, err := s.DrainSignals(ctx, "j1", "ctx")
	require.NoError(t, err)
	assert.Empty(t, again, "all consumed")

	// Unrelated name still there.
	o, err := s.PeekSignal(ctx, "j1", "other")
	require.NoError(t, err)
	assert.NotNil(t, o)
}

func TestGetSignalWaitingJobsToResume(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	mkJob := func(id string, status core.JobStatus, runAt *time.Time) {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: id, Type: "wf", Queue: "default", Status: status, RunAt: runAt,
		}).Error)
	}
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)

	mkJob("has-signal", core.StatusWaiting, nil) // waiting + a pending signal → resume
	require.NoError(t, s.SendSignal(ctx, "has-signal", "ctx", []byte(`1`)))
	mkJob("timed-out", core.StatusWaiting, &past)       // waiting + deadline passed → resume
	mkJob("still-waiting", core.StatusWaiting, &future) // waiting, no signal, future deadline → NOT yet
	mkJob("plain-waiting", core.StatusWaiting, nil)     // waiting, no signal, no deadline → NOT
	mkJob("running-signal", core.StatusRunning, nil)    // running (not waiting) → NOT
	require.NoError(t, s.SendSignal(ctx, "running-signal", "ctx", []byte(`1`)))

	// A parent still waiting on a pending fan-out must NOT be signal-resumed even
	// if it has a pending signal — resuming would replay and re-suspend on the
	// incomplete fan-out. The signal stays buffered for when the fan-out finishes.
	mkJob("fanout-parent", core.StatusWaiting, nil)
	require.NoError(t, s.SendSignal(ctx, "fanout-parent", "ctx", []byte(`1`)))
	require.NoError(t, s.db.WithContext(ctx).Create(&core.FanOut{
		ID: "fo1", ParentJobID: "fanout-parent", Status: core.FanOutPending, TotalCount: 1,
	}).Error)

	jobs, err := s.GetSignalWaitingJobsToResume(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, j := range jobs {
		ids[j.ID] = true
	}
	assert.True(t, ids["has-signal"], "waiting job with a pending signal must resume")
	assert.True(t, ids["timed-out"], "waiting job past its deadline must resume")
	assert.False(t, ids["still-waiting"], "future deadline, no signal → not yet")
	assert.False(t, ids["plain-waiting"], "no signal, no deadline → not")
	assert.False(t, ids["running-signal"], "only waiting jobs are resumed")
	assert.False(t, ids["fanout-parent"], "a parent waiting on a pending fan-out must not be signal-resumed")
}

func TestGetSignalWaitingJobsToResumeAfter_KeysetPagesInIDOrder(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
			ID: id, Type: "wf", Queue: "default", Status: core.StatusWaiting,
		}).Error)
		require.NoError(t, s.SendSignal(ctx, id, "ctx", []byte(`1`)))
	}

	first, err := s.GetSignalWaitingJobsToResumeAfter(ctx, "", 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	assert.Equal(t, "a", first[0].ID)
	assert.Equal(t, "b", first[1].ID)

	second, err := s.GetSignalWaitingJobsToResumeAfter(ctx, first[1].ID, 2)
	require.NoError(t, err)
	require.Len(t, second, 2)
	assert.Equal(t, "c", second[0].ID)
	assert.Equal(t, "d", second[1].ID)
}

func TestResumeSignalWaitingJob(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)

	// A waiting job with a future run_at (a WaitForSignalTimeout wake deadline).
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: "w", Type: "x", Queue: "default", Status: core.StatusWaiting, RunAt: &future,
	}).Error)
	// A paused job — a signal must NOT un-pause it.
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: "p", Type: "x", Queue: "default", Status: core.StatusPaused,
	}).Error)

	ok, err := s.ResumeSignalWaitingJob(ctx, "w")
	require.NoError(t, err)
	require.True(t, ok)
	var w core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&w, "id = ?", "w").Error)
	assert.Equal(t, core.StatusPending, w.Status)
	assert.Nil(t, w.RunAt, "signal resume must clear the timeout wake deadline")

	ok2, err := s.ResumeSignalWaitingJob(ctx, "p")
	require.NoError(t, err)
	assert.False(t, ok2, "a paused job is not signal-resumable")
	var p core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&p, "id = ?", "p").Error)
	assert.Equal(t, core.StatusPaused, p.Status, "paused job stays paused")
}

func TestResumeJob_PreservesRunAt(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)
	// A delayed job paused before its run_at; the general ResumeJob must NOT strip
	// its schedule (only the signal-specific resume clears run_at).
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID: "d", Type: "x", Queue: "default", Status: core.StatusPaused, RunAt: &future,
	}).Error)

	ok, err := s.ResumeJob(ctx, "d")
	require.NoError(t, err)
	require.True(t, ok)
	var d core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&d, "id = ?", "d").Error)
	assert.Equal(t, core.StatusPending, d.Status)
	require.NotNil(t, d.RunAt, "ResumeJob must preserve a delayed job's schedule")
}

func TestConsumeSignal_ConcurrentDisjoint(t *testing.T) {
	s := newTestStorage(t)
	if s.isSQLite {
		t.Skip("concurrent consume contention is meaningful on Postgres/MySQL")
	}
	ctx := context.Background()
	const n = 20
	for i := 0; i < n; i++ {
		require.NoError(t, s.SendSignal(ctx, "j1", "ctx", []byte(`1`)))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				sig, err := s.ConsumeSignal(ctx, "j1", "ctx")
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
