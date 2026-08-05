package signal_test

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markerStore is a fakeSignalStore that also implements core.SignalWaitMarker, so
// WaitForSignal should record the awaited name through the capability rather than
// falling back to the plain MarkWaiting.
type markerStore struct {
	*fakeSignalStore
	recordedName  string
	recordedCalls int
}

func (m *markerStore) MarkWaitingForSignal(_ context.Context, _ core.UUID, _ string, name string) error {
	m.recordedCalls++
	m.recordedName = name
	m.mu.Lock()
	m.suspended++
	m.mu.Unlock()
	return nil
}

// A suspend must record the name the handler is parked on. The storage-side
// correlation in GetSignalWaitingJobsToResumeAfter is only as good as this write:
// if the name is never recorded, every waiting job carries the empty string, the
// poll takes its permissive branch, and an unconsumable pending signal resumes and
// fully replays the job on every tick again.
func TestWaitForSignal_SuspendRecordsTheAwaitedName(t *testing.T) {
	store := &markerStore{fakeSignalStore: &fakeSignalStore{}}
	rec := newRecorder()

	_, err := signal.WaitForSignal[string](buildCtx(store, rec, nil), "approval")
	require.Error(t, err)
	require.True(t, core.IsWaiting(err), "no signal yet → self-suspend")

	assert.Equal(t, 1, store.recordedCalls,
		"the suspend must go through MarkWaitingForSignal when the storage offers it, not the plain MarkWaiting")
	assert.Equal(t, "approval", store.recordedName,
		"the recorded name is what the resume poll correlates against; an empty or wrong name puts the job back on the every-tick replay path")
}

// A storage that does not implement the capability must still suspend. Adding the
// capability must not become a hard requirement of core.Storage, which is exported
// and implemented outside this repo.
func TestWaitForSignal_SuspendsWithoutTheCapability(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	_, err := signal.WaitForSignal[string](buildCtx(store, rec, nil), "approval")
	require.Error(t, err)
	require.True(t, core.IsWaiting(err))
	assert.Equal(t, 1, store.suspended, "the plain MarkWaiting fallback must still suspend the job")
}

// WaitForSignalTimeout suspends through the atomic checkpoint+suspend primitive
// rather than MarkWaiting, so it needs its own coverage: without the name it would
// still be replayed on every poll tick by an unconsumable signal, right up until
// its deadline fired.
func TestWaitForSignalTimeout_SuspendRecordsTheAwaitedName(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	_, _, err := signal.WaitForSignalTimeout[string](buildCtx(store, rec, nil), "approval", time.Hour)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err), "no signal yet → self-suspend with a deadline")
	assert.Equal(t, "approval", store.awaitedName,
		"a timed wait must record the name it is parked on, not leave it empty")
}

// A durable sleep waits on the clock and on nothing else. It records the reserved
// sleep type so no user signal correlates with it.
//
// This is the case GetSignalWaitingJobsToResumeAfter's doc comment already claimed
// — "scan past durable timers that have buffered user signals but should not be
// resumed before run_at" — while its query resumed them anyway. Before this, any
// buffered signal replayed a sleeping job on every tick for the whole sleep.
func TestSleep_RecordsTheReservedSleepTypeSoNoSignalWakesIt(t *testing.T) {
	store := &fakeSignalStore{}
	rec := newRecorder()

	err := signal.Sleep(buildCtx(store, rec, nil), time.Hour)
	require.Error(t, err)
	require.True(t, core.IsWaiting(err), "a durable sleep suspends")
	assert.Equal(t, signal.SleepCheckpointType, store.awaitedName,
		"a sleep must record the reserved sleep type; an empty name puts it back on the permissive resume path where a buffered signal replays it every tick")

	// The sentinel can never collide with a real signal name. Asserted through a
	// public entry point that validates, rather than by re-stating the "_" rule
	// here — a test that copies the rule cannot catch the rule changing.
	_, waitErr := signal.WaitForSignal[string](buildCtx(store, rec, nil), signal.SleepCheckpointType)
	assert.ErrorIs(t, waitErr, signal.ErrSignalNameReserved,
		"the sleep sentinel must be rejected as a user signal name, or a caller could send a signal that wakes every sleeping job early")
}
