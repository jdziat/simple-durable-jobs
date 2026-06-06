package signal_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/signal"
)

// fakeSignalStore implements the (unexported) signalStorage capability plus the
// SuspendJob bits pkg/signal needs. It embeds core.Storage so it satisfies the
// full interface; only the methods exercised here are implemented (others would
// panic, but aren't called).
type fakeSignalStore struct {
	core.Storage
	mu             sync.Mutex
	pending        []*core.Signal // unconsumed, FIFO
	checkpoints    []core.Checkpoint
	checkpointsErr error
	suspended      int
	waitDur        time.Duration
}

func (f *fakeSignalStore) SendSignal(_ context.Context, _, name string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending = append(f.pending, &core.Signal{Name: name, Payload: payload})
	return nil
}

func (f *fakeSignalStore) PeekSignal(_ context.Context, _, name string) (*core.Signal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.pending {
		if s.Name == name {
			return s, nil
		}
	}
	return nil, nil
}

func (f *fakeSignalStore) ConsumeSignal(_ context.Context, _, name string) (*core.Signal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.pending {
		if s.Name == name {
			f.pending = append(f.pending[:i], f.pending[i+1:]...)
			return s, nil
		}
	}
	return nil, nil
}

func (f *fakeSignalStore) DrainSignals(_ context.Context, _, name string) ([]*core.Signal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out, rest []*core.Signal
	for _, s := range f.pending {
		if s.Name == name {
			out = append(out, s)
		} else {
			rest = append(rest, s)
		}
	}
	f.pending = rest
	return out, nil
}

func (f *fakeSignalStore) SuspendJob(_ context.Context, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspended++
	return nil
}

func (f *fakeSignalStore) SuspendJobWithDeadline(_ context.Context, _, _ string, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspended++
	f.waitDur = d
	return nil
}

func (f *fakeSignalStore) GetCheckpoints(_ context.Context, _ string) ([]core.Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.Checkpoint(nil), f.checkpoints...), f.checkpointsErr
}

type recorder struct {
	mu  sync.Mutex
	cps map[intctx.CheckpointKey]*core.Checkpoint
}

func newRecorder() *recorder {
	return &recorder{cps: map[intctx.CheckpointKey]*core.Checkpoint{}}
}

func (r *recorder) list() []core.Checkpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]core.Checkpoint, 0, len(r.cps))
	for _, c := range r.cps {
		out = append(out, *c)
	}
	return out
}

func buildCtx(store core.Storage, rec *recorder, seed []core.Checkpoint) context.Context {
	jc := &intctx.JobContext{
		Job:      &core.Job{ID: "j1"},
		Storage:  store,
		WorkerID: "w1",
		SaveCheckpoint: func(_ context.Context, cp *core.Checkpoint) error {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			rec.cps[intctx.CheckpointKey{Index: cp.CallIndex, Type: cp.CallType}] = cp
			return nil
		},
	}
	ctx := intctx.WithJobContext(context.Background(), jc)
	return intctx.WithCallState(ctx, seed)
}

func send(store *fakeSignalStore, name string, v any) {
	p, _ := json.Marshal(v)
	_ = store.SendSignal(context.Background(), "j1", name, p)
}

func TestWaitForSignal_ConsumeThenReplay(t *testing.T) {
	store := &fakeSignalStore{}
	send(store, "ctx", "hello")
	rec := newRecorder()

	got, err := signal.WaitForSignal[string](buildCtx(store, rec, nil), "ctx")
	require.NoError(t, err)
	assert.Equal(t, "hello", got)
	assert.Empty(t, store.pending, "signal consumed on first execution")

	// Replay: seeded with the recorded checkpoint; storage is now empty.
	got2, err := signal.WaitForSignal[string](buildCtx(store, rec, rec.list()), "ctx")
	require.NoError(t, err)
	assert.Equal(t, "hello", got2, "replay returns the checkpointed payload")
	assert.Equal(t, 0, store.suspended, "replay must not suspend")
}

func TestWaitForSignal_SuspendsWhenNone(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()
	_, err := signal.WaitForSignal[string](buildCtx(store, rec, nil), "ctx")
	require.Error(t, err)
	assert.True(t, core.IsWaiting(err), "no signal → self-suspend")
	assert.Equal(t, 1, store.suspended)
}

func TestCheckSignal_PeekDoesNotConsume(t *testing.T) {
	store := &fakeSignalStore{}
	send(store, "ctx", "v1")
	rec := newRecorder()

	got, ok, err := signal.CheckSignal[string](buildCtx(store, rec, nil), "ctx")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "v1", got)
	assert.Len(t, store.pending, 1, "CheckSignal must not consume")

	// Replay returns the recorded peek even though storage changed.
	store.pending = nil
	got2, ok2, err := signal.CheckSignal[string](buildCtx(store, rec, rec.list()), "ctx")
	require.NoError(t, err)
	assert.True(t, ok2)
	assert.Equal(t, "v1", got2)
}

func TestCheckSignal_NoneIsDeterministic(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()
	_, ok, err := signal.CheckSignal[string](buildCtx(store, rec, nil), "ctx")
	require.NoError(t, err)
	assert.False(t, ok)

	// A signal arrives AFTER the first check; replay must still report none.
	send(store, "ctx", "late")
	_, ok2, err := signal.CheckSignal[string](buildCtx(store, rec, rec.list()), "ctx")
	require.NoError(t, err)
	assert.False(t, ok2, "a later arrival cannot change a recorded peek outcome")
}

func TestDrainSignals_FIFO(t *testing.T) {
	store := &fakeSignalStore{}
	for _, v := range []string{"a", "b", "c"} {
		send(store, "ctx", v)
	}
	send(store, "misc", "x")
	rec := newRecorder()

	got, err := signal.DrainSignals[string](buildCtx(store, rec, nil), "ctx")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, got)
	assert.Len(t, store.pending, 1, "only the unrelated signal remains")

	// Replay returns the same batch.
	got2, err := signal.DrainSignals[string](buildCtx(store, rec, rec.list()), "ctx")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, got2)
}

func TestWaitForSignalTimeout_PresentReturnsValue(t *testing.T) {
	store := &fakeSignalStore{}
	send(store, "ctx", "now")
	got, ok, err := signal.WaitForSignalTimeout[string](buildCtx(store, newRecorder(), nil), "ctx", time.Hour)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "now", got)
	assert.Equal(t, 0, store.suspended)
}

func TestWaitForSignalTimeout_ImmediateTimeout(t *testing.T) {
	store := &fakeSignalStore{}
	// Negative duration → deadline already passed → resolve as timed-out
	// without suspending.
	_, ok, err := signal.WaitForSignalTimeout[string](buildCtx(store, newRecorder(), nil), "ctx", -time.Second)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, 0, store.suspended)
}

func TestWaitForSignalTimeout_SuspendsBeforeDeadline(t *testing.T) {
	store := &fakeSignalStore{}
	_, ok, err := signal.WaitForSignalTimeout[string](buildCtx(store, newRecorder(), nil), "ctx", time.Hour)
	require.Error(t, err)
	assert.False(t, ok)
	assert.True(t, core.IsWaiting(err))
	assert.Equal(t, 1, store.suspended)
}

func TestWaitForSignalTimeout_ReplayAfterTimeout(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	// First execution: no signal, positive deadline → records an unresolved
	// checkpoint and suspends.
	_, ok, err := signal.WaitForSignalTimeout[string](buildCtx(store, rec, nil), "ctx", 20*time.Millisecond)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err))
	require.False(t, ok)
	require.Equal(t, 1, store.suspended)

	// Let the deadline pass, then resume (replay seeded with the unresolved
	// checkpoint): no signal arrived → times out, and must NOT suspend again.
	time.Sleep(30 * time.Millisecond)
	_, ok2, err2 := signal.WaitForSignalTimeout[string](buildCtx(store, rec, rec.list()), "ctx", 20*time.Millisecond)
	require.NoError(t, err2)
	require.False(t, ok2, "no signal before the deadline → timed out")
	require.Equal(t, 1, store.suspended, "resume that times out must not suspend again")

	// Replay again (seeded with the now-resolved checkpoint): deterministic, no
	// storage read, same outcome even though a signal is now present.
	send(store, "ctx", "too-late")
	_, ok3, err3 := signal.WaitForSignalTimeout[string](buildCtx(store, rec, rec.list()), "ctx", 20*time.Millisecond)
	require.NoError(t, err3)
	require.False(t, ok3, "replay returns the recorded timeout outcome")
}

func TestWaitForSignalTimeout_SignalArrivesAfterSuspend(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	// First execution suspends (long deadline, no signal yet).
	_, ok, err := signal.WaitForSignalTimeout[string](buildCtx(store, rec, nil), "ctx", time.Hour)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err))
	require.False(t, ok)

	// A signal arrives while suspended; the resume consumes it (signal wins over
	// the not-yet-passed deadline) and records a resolved-with-payload checkpoint.
	send(store, "ctx", "arrived")
	got, ok2, err2 := signal.WaitForSignalTimeout[string](buildCtx(store, rec, rec.list()), "ctx", time.Hour)
	require.NoError(t, err2)
	require.True(t, ok2)
	require.Equal(t, "arrived", got)

	// Replay is deterministic from the resolved checkpoint.
	store.pending = nil
	got3, ok3, err3 := signal.WaitForSignalTimeout[string](buildCtx(store, rec, rec.list()), "ctx", time.Hour)
	require.NoError(t, err3)
	require.True(t, ok3)
	require.Equal(t, "arrived", got3)
}

func TestSignal_RequiresJobContext(t *testing.T) {
	_, err := signal.WaitForSignal[string](context.Background(), "ctx")
	require.Error(t, err)
}

func TestSignal_ReservedNameRejected(t *testing.T) {
	store := &fakeSignalStore{}
	_, err := signal.WaitForSignal[string](buildCtx(store, newRecorder(), nil), "_sleep")
	require.Error(t, err)
	assert.True(t, errors.Is(err, signal.ErrSignalNameReserved))

	_, _, err = signal.WaitForSignalTimeout[string](buildCtx(store, newRecorder(), nil), "_sleep", time.Second)
	require.Error(t, err)
	assert.True(t, errors.Is(err, signal.ErrSignalNameReserved))

	_, _, err = signal.CheckSignal[string](buildCtx(store, newRecorder(), nil), "_sleep")
	require.Error(t, err)
	assert.True(t, errors.Is(err, signal.ErrSignalNameReserved))

	_, err = signal.DrainSignals[string](buildCtx(store, newRecorder(), nil), "_sleep")
	require.Error(t, err)
	assert.True(t, errors.Is(err, signal.ErrSignalNameReserved))
}

func TestSleep_RequiresJobContext(t *testing.T) {
	err := signal.Sleep(context.Background(), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be used within a job handler")
}

func TestSleep_NonPositiveReturnsAndCheckpoints(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	require.NoError(t, signal.Sleep(buildCtx(store, rec, nil), 0))
	assert.Equal(t, 0, store.suspended)
	require.Len(t, rec.list(), 1)
	assert.Equal(t, signal.SleepCheckpointType, rec.list()[0].CallType)

	require.NoError(t, signal.Sleep(buildCtx(store, rec, rec.list()), time.Hour))
	assert.Equal(t, 0, store.suspended, "resolved fast-path checkpoint must replay without suspending")
}

func TestSleepUntil_PastReturnsAndCheckpoints(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	require.NoError(t, signal.SleepUntil(buildCtx(store, rec, nil), time.Now().Add(-time.Second)))
	assert.Equal(t, 0, store.suspended)
	require.Len(t, rec.list(), 1)

	require.NoError(t, signal.SleepUntil(buildCtx(store, rec, rec.list()), time.Now().Add(time.Hour)))
	assert.Equal(t, 0, store.suspended, "resolved past-time checkpoint must replay without suspending")
}

func TestSleep_SuspendsBeforeDeadline(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	err := signal.Sleep(buildCtx(store, rec, nil), time.Hour)
	require.Error(t, err)
	assert.True(t, core.IsWaiting(err))
	assert.Equal(t, 1, store.suspended)
	assert.Greater(t, store.waitDur, 59*time.Minute)
}

func TestSleep_ReplayAfterCompletionDoesNotRewait(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	err := signal.Sleep(buildCtx(store, rec, nil), 20*time.Millisecond)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err))
	require.Equal(t, 1, store.suspended)

	time.Sleep(30 * time.Millisecond)
	require.NoError(t, signal.Sleep(buildCtx(store, rec, rec.list()), 20*time.Millisecond))
	require.Equal(t, 1, store.suspended, "resume after elapsed deadline resolves without suspending again")

	require.NoError(t, signal.Sleep(buildCtx(store, rec, rec.list()), time.Hour))
	require.Equal(t, 1, store.suspended, "resolved checkpoint must replay without re-waiting")
}

func TestSleep_ReplayBeforeDeadlineKeepsOriginalDeadline(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	err := signal.Sleep(buildCtx(store, rec, nil), time.Hour)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err))
	firstWait := store.waitDur

	err = signal.Sleep(buildCtx(store, rec, rec.list()), 2*time.Hour)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err))
	assert.Less(t, store.waitDur, firstWait, "replay uses remaining time to the original deadline, not the new duration")
	assert.Greater(t, store.waitDur, 59*time.Minute)
}

func TestWaitingOnFutureSleep_GuardRequiresInternalCallCheckpointWithDeadline(t *testing.T) {
	future := time.Now().Add(time.Hour)
	unresolvedSleep, err := json.Marshal(map[string]any{
		"deadline": future.UnixNano(),
		"resolved": false,
	})
	require.NoError(t, err)
	zeroDeadline, err := json.Marshal(map[string]any{
		"deadline": 0,
		"resolved": false,
	})
	require.NoError(t, err)

	job := &core.Job{ID: "j1", RunAt: &future}
	store := &fakeSignalStore{checkpoints: []core.Checkpoint{
		{JobID: "j1", CallIndex: -1, CallType: signal.SleepCheckpointType, Result: unresolvedSleep},
		{JobID: "j1", CallIndex: 0, CallType: "sleep", Result: unresolvedSleep},
		{JobID: "j1", CallIndex: 1, CallType: signal.SleepCheckpointType, Result: zeroDeadline},
	}}
	assert.False(t, signal.WaitingOnFutureSleep(context.Background(), store, job, nil),
		"phase checkpoints, user call type collisions, and zero deadlines must not suppress resume")

	store.checkpoints = append(store.checkpoints, core.Checkpoint{
		JobID: "j1", CallIndex: 2, CallType: signal.SleepCheckpointType, Result: unresolvedSleep,
	})
	assert.True(t, signal.WaitingOnFutureSleep(context.Background(), store, job, nil))
}
