package signal_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notOwnerStore is a storage whose ownership gate refuses this run — what every
// storage returns once the lease has moved to another worker. Everything else
// behaves normally, so the only variable is the refusal.
type notOwnerStore struct {
	*fakeSignalStore
	consumeCalls int
}

func (n *notOwnerStore) ConsumeSignalTxOwned(_ context.Context, _ core.UUID, _ string, _ string,
	_ func(sig *core.Signal) (*core.Checkpoint, error)) (*core.Signal, error) {
	n.consumeCalls++
	return nil, core.ErrJobNotOwned
}

// A run that has lost its lease must NOT decide the outcome of a wait.
//
// The failure this pins is not the storage return value — that is asserted in
// pkg/storage. It is what the caller DOES with it. WaitForSignalTimeout's structure
// is:
//
//	sig, err := ConsumeSignalTxOwned(...)
//	if err != nil { return err }
//	if sig != nil { deliver }
//	if deadline passed { write a durable "timed out" checkpoint }   // <-- here
//
// While the ownership gate returned (nil, nil), a non-owner fell straight through
// to that last branch. The checkpoint write is an unfenced upsert, so the verdict
// LANDED — committed by a run with no claim on the job, for a signal that was still
// pending and still in time. Replay treats the checkpoint as authoritative, so the
// job completes down the timeout branch and the signal is never delivered to anyone.
//
// The signal-present branch was never the problem: a non-owner could not consume.
// The bug was entirely in what "I consumed nothing" was allowed to mean.
func TestWaitForSignalTimeout_ALostLeaseDoesNotCommitATimeoutVerdict(t *testing.T) {
	const name = "approval"
	// A deadline well in the past: the timeout branch is unambiguously eligible.
	deadline := time.Now().Add(-10 * time.Second)

	store := &notOwnerStore{fakeSignalStore: &fakeSignalStore{}}
	// The signal is present and arrived IN TIME. This is what makes a timeout
	// verdict wrong rather than merely premature.
	store.pending = append(store.pending, &core.Signal{
		ID: core.NewID(), JobID: "j1", Name: name,
		Payload:   json.RawMessage(`"APPROVED"`),
		CreatedAt: deadline.Add(-3 * time.Second),
	})
	rec := newRecorder()

	_, ok, err := signal.WaitForSignalTimeout[string](
		buildCtx(store, rec, unresolvedTimeoutCheckpoint(t, name, deadline)), name, time.Minute)

	require.Error(t, err,
		"a run that no longer owns the job must surface the refusal, not return a verdict")
	assert.ErrorIs(t, err, core.ErrJobNotOwned,
		"the error must remain identifiable through the wrap: the worker's disposition "+
			"machinery keys on core.ErrJobNotOwned to abandon the attempt without writing")
	assert.False(t, ok)

	// The load-bearing assertion. An error return would be cold comfort if the
	// verdict had already been persisted on the way out.
	for key, cp := range rec.list() {
		var tc struct {
			Resolved bool `json:"resolved"`
			TimedOut bool `json:"timed_out"`
		}
		require.NoError(t, json.Unmarshal(cp.Result, &tc))
		assert.False(t, tc.Resolved && tc.TimedOut,
			"a non-owner committed a durable 'timed out' verdict (checkpoint %v). The signal "+
				"is still pending and arrived before the deadline, so replay will now read this "+
				"checkpoint as authoritative and complete the job down the wrong branch, with "+
				"the signal undelivered forever", key)
	}

	assert.Len(t, store.pending, 1,
		"the signal must remain pending for the run that actually owns the job")
}

// The earlier test reaches the consume fence because its in-time signal is still
// pending. This one pins the other interleave: the owner consumed that signal and
// wrote the delivered verdict after this stale run loaded its unresolved snapshot.
// Peek now sees nothing, so the stale run reaches the EARLY timeout write directly.
func TestWaitForSignalTimeout_AStaleSnapshotCannotOverwriteTheOwnersDeliveredVerdict(t *testing.T) {
	const name = "approval"
	deadline := time.Now().Add(-10 * time.Second)
	store := &fakeSignalStore{} // the owner already consumed the signal
	rec := newRecorder()
	ctx := buildCtx(store, rec, unresolvedTimeoutCheckpoint(t, name, deadline))

	// Model the worker's ownership-fenced SaveCheckpoint closure after its lease
	// moved. If WaitForSignalTimeout tries to commit the stale timeout verdict, it
	// must surface ErrJobNotOwned and leave durable state untouched.
	jc := intctx.GetJobContext(ctx)
	require.NotNil(t, jc)
	jc.SaveCheckpoint = func(context.Context, *core.Checkpoint) error {
		return core.ErrJobNotOwned
	}

	_, ok, err := signal.WaitForSignalTimeout[string](ctx, name, time.Minute)
	require.ErrorIs(t, err, core.ErrJobNotOwned)
	assert.False(t, ok)
	assert.Empty(t, rec.list(), "the stale timeout verdict must not be recorded")
}
