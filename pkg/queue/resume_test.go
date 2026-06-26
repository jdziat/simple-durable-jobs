package queue

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests complement the existing resume coverage in queue_test.go and
// zz_coverage_gen_test.go. The latter already assert that a storage error
// propagates out of Queue.ResumeJob / Queue.ResumeQueue; what was missing is the
// *event* invariant: a resume that does not durably succeed must emit no
// JobResumed / QueueResumed event (the event is the operator-visible signal that
// the job/queue is live again). These tests pin that, plus the defensive branch
// where the job has vanished between UnpauseJob and the post-resume GetJob.
//
// They reuse the unpauseJobErrStorage / pauseQueueErrStorage fault wrappers
// defined in zz_coverage_gen_test.go.

// assertNoEvent asserts that no event is sitting on the subscription channel.
func assertNoEvent(t *testing.T, events <-chan core.Event) {
	t.Helper()
	select {
	case ev := <-events:
		t.Fatalf("expected no event, got %T: %+v", ev, ev)
	default:
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Queue.ResumeJob — error and defensive branches emit no event
// ──────────────────────────────────────────────────────────────────────────────

func TestQueue_ResumeJob_StorageErrorEmitsNoEvent(t *testing.T) {
	store := &unpauseJobErrStorage{mockStorage: newMockStorage()}
	q := New(store)
	ctx := context.Background()
	events := q.Events()
	defer q.Unsubscribe(events)

	err := q.ResumeJob(ctx, core.UUID("job-1"))
	require.Error(t, err, "storage error must propagate")
	assertNoEvent(t, events)
}

func TestQueue_ResumeJob_NoEventWhenJobMissingAfterUnpause(t *testing.T) {
	// UnpauseJob succeeds (no-op on an unknown id), but the subsequent GetJob
	// returns nil — ResumeJob must still return nil and emit no JobResumed event.
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()
	events := q.Events()
	defer q.Unsubscribe(events)

	err := q.ResumeJob(ctx, core.UUID("does-not-exist"))
	require.NoError(t, err, "unpausing a missing job is not an error")
	assertNoEvent(t, events)
}

func TestQueue_ResumeJob_EmitsResumedEventOnSuccess(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()

	q.Register("resume-evt", func(ctx context.Context, _ struct{}) error { return nil })
	jobID, err := q.Enqueue(ctx, "resume-evt", struct{}{})
	require.NoError(t, err)
	require.NoError(t, q.PauseJob(ctx, jobID))

	events := q.Events()
	defer q.Unsubscribe(events)

	require.NoError(t, q.ResumeJob(ctx, jobID))
	select {
	case ev := <-events:
		resumed, ok := ev.(*core.JobResumed)
		require.True(t, ok, "expected *core.JobResumed, got %T", ev)
		assert.Equal(t, jobID, resumed.Job.ID)
	default:
		t.Fatal("expected a JobResumed event")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Queue.ResumeQueue — error and validation branches emit no event
// ──────────────────────────────────────────────────────────────────────────────

func TestQueue_ResumeQueue_StorageErrorEmitsNoEvent(t *testing.T) {
	store := &pauseQueueErrStorage{mockStorage: newMockStorage()}
	q := New(store)
	ctx := context.Background()
	events := q.Events()
	defer q.Unsubscribe(events)

	err := q.ResumeQueue(ctx, "valid-queue")
	require.Error(t, err, "storage error must propagate")
	assertNoEvent(t, events)
}

func TestQueue_ResumeQueue_InvalidNameEmitsNoEvent(t *testing.T) {
	store := newMockStorage()
	q := New(store)
	ctx := context.Background()
	events := q.Events()
	defer q.Unsubscribe(events)

	err := q.ResumeQueue(ctx, "bad name!")
	require.Error(t, err, "an invalid queue name is rejected before any storage call")
	assertNoEvent(t, events)
}
