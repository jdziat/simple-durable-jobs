package signal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/stretchr/testify/require"
)

// unresolvedTimeoutCheckpoint builds the on-disk shape WaitForSignalTimeout writes
// when it suspends: a deadline, not yet resolved. Constructed as raw JSON because
// the struct is unexported, which also means this test pins the persisted FORMAT —
// if the tags change, it fails here rather than silently testing nothing.
func unresolvedTimeoutCheckpoint(t *testing.T, name string, deadline time.Time) []core.Checkpoint {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"deadline": deadline.UnixNano(),
		"resolved": false,
	})
	require.NoError(t, err)
	return []core.Checkpoint{{
		JobID:     "j1",
		CallIndex: 0,
		CallType:  core.CheckpointTypeSignalTimeoutPrefix + name,
		Result:    body,
	}}
}

// WaitForSignalTimeout consumed any pending signal BEFORE checking its own
// deadline, so a signal that arrived after the deadline was reported as
// arrived-in-time (timedOut=false) with no bound on how late it could be.
//
// Reachable whenever nothing resumes the job at its deadline: a worker outage, a
// saturated fleet, a paused queue. The job's run_at fires the timeout only if a
// worker is there to act on it; otherwise the next replay happens later, and
// whatever signal has since arrived wins.
//
// The fix keys on the SIGNAL's arrival time rather than on "now", and the second
// subtest is why: a signal that genuinely arrived before the deadline must still
// be honoured when the replay happens afterwards. A now-vs-deadline check — the
// obvious fix — passes the first subtest and breaks that one.
func TestWaitForSignalTimeout_ALateSignalDoesNotSatisfyTheWait(t *testing.T) {
	const name = "approval"

	t.Run("signal that arrived AFTER the deadline times out", func(t *testing.T) {
		deadline := time.Now().Add(-10 * time.Second)
		store := &fakeSignalStore{}
		store.pending = append(store.pending, &core.Signal{
			ID: core.NewID(), JobID: "j1", Name: name,
			Payload:   json.RawMessage(`"late-approval"`),
			CreatedAt: deadline.Add(3 * time.Second), // after the deadline
		})
		rec := newRecorder()

		got, ok, err := signal.WaitForSignalTimeout[string](
			buildCtx(store, rec, unresolvedTimeoutCheckpoint(t, name, deadline)), name, time.Minute)
		require.NoError(t, err)
		require.False(t, ok,
			"a signal that arrived after the deadline was reported as arrived-in-time (value %q); the timeout is then not honoured at all, and nothing bounds how late the signal may be", got)
		require.Empty(t, got)

		require.Len(t, store.pending, 1,
			"a signal that did not satisfy this wait must stay pending for a later wait rather than being consumed and discarded")
	})

	t.Run("signal that arrived BEFORE the deadline is still honoured on a late replay", func(t *testing.T) {
		deadline := time.Now().Add(-10 * time.Second)
		store := &fakeSignalStore{}
		store.pending = append(store.pending, &core.Signal{
			ID: core.NewID(), JobID: "j1", Name: name,
			Payload:   json.RawMessage(`"in-time-approval"`),
			CreatedAt: deadline.Add(-3 * time.Second), // before the deadline
		})
		rec := newRecorder()

		got, ok, err := signal.WaitForSignalTimeout[string](
			buildCtx(store, rec, unresolvedTimeoutCheckpoint(t, name, deadline)), name, time.Minute)
		require.NoError(t, err)
		require.True(t, ok,
			"this signal arrived before the deadline and must be delivered even though the replay only happens now; timing it out would be a new bug introduced by checking now-vs-deadline instead of the signal's arrival time")
		require.Equal(t, "in-time-approval", got)
		require.Empty(t, store.pending, "an in-time signal must be consumed")
	})

	t.Run("no signal at all still times out", func(t *testing.T) {
		deadline := time.Now().Add(-10 * time.Second)
		store := &fakeSignalStore{}
		rec := newRecorder()

		_, ok, err := signal.WaitForSignalTimeout[string](
			buildCtx(store, rec, unresolvedTimeoutCheckpoint(t, name, deadline)), name, time.Minute)
		require.NoError(t, err)
		require.False(t, ok, "the ordinary timeout path must be unaffected")
	})

	// A peek failure must fall through to the consume rather than inventing a
	// timeout: delivering the signal is the pre-existing behaviour, and discarding
	// a signal on the strength of a failed read is the worse error.
	t.Run("a peek failure falls back to delivering the signal", func(t *testing.T) {
		deadline := time.Now().Add(-10 * time.Second)
		store := &peekFailStore{fakeSignalStore: &fakeSignalStore{}}
		store.pending = append(store.pending, &core.Signal{
			ID: core.NewID(), JobID: "j1", Name: name,
			Payload:   json.RawMessage(`"late-approval"`),
			CreatedAt: deadline.Add(3 * time.Second),
		})
		rec := newRecorder()

		_, ok, err := signal.WaitForSignalTimeout[string](
			buildCtx(store, rec, unresolvedTimeoutCheckpoint(t, name, deadline)), name, time.Minute)
		require.NoError(t, err)
		require.True(t, ok, "an unreadable peek must not be turned into a timeout")
	})
}

type peekFailStore struct {
	*fakeSignalStore
}

func (p *peekFailStore) PeekSignal(context.Context, core.UUID, string) (*core.Signal, error) {
	return nil, fmt.Errorf("peek unavailable")
}
