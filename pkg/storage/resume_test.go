package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover resume code paths that the existing suite leaves untested:
//   - ResumeJob resuming a job that is *paused* (not just waiting). The dequeue
//     predicate matches `status IN (waiting, paused)` so that a fan-out parent
//     paused by an operator while its sub-jobs run is still resumed when the
//     fan-out completes — see the ResumeJob doc comment in gorm.go.
//   - ResumeJob's non-resumable / not-found short-circuits returning (false, nil).
//   - ResumeSignalWaitingJob's status guard: only StatusWaiting is signal-resumable
//     (running / not-found return (false, nil)), the operator-pause boundary being
//     covered separately in signal_test.go.

func resumeUUID(t *testing.T, v string) core.UUID {
	t.Helper()
	return signalUUID(v) // deterministic SHA1-derived UUID, reused for parity with signal_test.go
}

// ──────────────────────────────────────────────────────────────────────────────
// ResumeJob — paused jobs are resumable
// ──────────────────────────────────────────────────────────────────────────────

func TestResumeJob_ResumesPausedJob(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "resume-paused")

	// A paused job — e.g. a fan-out parent an operator paused mid-flight.
	seedTestJob(t, ctx, s, id, core.StatusPaused)

	resumed, err := s.ResumeJob(ctx, id)
	require.NoError(t, err)
	assert.True(t, resumed, "a paused job must be resumable (fan-out completion path)")

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusPending, after.Status, "paused → pending on resume")
}

func TestResumeJob_PausedResumeSetsDqReady(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "resume-paused-dqready")

	// Seed paused with dq_ready explicitly false to prove resume flips the
	// dequeue-eligibility hint back on.
	seedTestJob(t, ctx, s, id, core.StatusPaused)
	require.NoError(t, s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ?", id).
		Update("dq_ready", false).Error)

	resumed, err := s.ResumeJob(ctx, id)
	require.NoError(t, err)
	require.True(t, resumed)

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.True(t, after.DQReady, "resume must set dq_ready so a delayed-eligible job can be dequeued")
}

// ──────────────────────────────────────────────────────────────────────────────
// ResumeJob — non-resumable states are a (false, nil) no-op
// ──────────────────────────────────────────────────────────────────────────────

func TestResumeJob_NotFoundReturnsFalse(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	resumed, err := s.ResumeJob(ctx, resumeUUID(t, "resume-missing"))
	require.NoError(t, err, "resuming a missing job is not an error")
	assert.False(t, resumed, "a missing job affects no rows")
}

func TestResumeJob_RunningReturnsFalse(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "resume-running")

	seedTestJob(t, ctx, s, id, core.StatusRunning)

	resumed, err := s.ResumeJob(ctx, id)
	require.NoError(t, err)
	assert.False(t, resumed, "a running job is not resumable")

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusRunning, after.Status, "status is left untouched")
}

func TestResumeJob_CompletedReturnsFalse(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "resume-completed")

	seedTestJob(t, ctx, s, id, core.StatusCompleted)

	resumed, err := s.ResumeJob(ctx, id)
	require.NoError(t, err)
	assert.False(t, resumed, "a terminal job is not resumable")

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusCompleted, after.Status)
}

// ──────────────────────────────────────────────────────────────────────────────
// ResumeJob — preserves run_at across a paused resume (delayed schedule honored)
// ──────────────────────────────────────────────────────────────────────────────

func TestResumeJob_PausedResumePreservesRunAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "resume-paused-runat")

	// A delayed job that was paused before its run_at fired.
	future := time.Now().Add(time.Hour).UTC()
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID:     id,
		Type:   "fixture.job",
		Queue:  "default",
		Status: core.StatusPaused,
		RunAt:  &future,
	}).Error)

	resumed, err := s.ResumeJob(ctx, id)
	require.NoError(t, err)
	require.True(t, resumed)

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.NotNil(t, after.RunAt, "ResumeJob must not clear run_at; the original schedule is honored")
	assert.WithinDuration(t, future, after.RunAt.UTC(), time.Second)
}

// ──────────────────────────────────────────────────────────────────────────────
// ResumeSignalWaitingJob — status guard (waiting-only) for non-waiting inputs
// ──────────────────────────────────────────────────────────────────────────────

func TestResumeSignalWaitingJob_NotFoundReturnsFalse(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	ok, err := s.ResumeSignalWaitingJob(ctx, resumeUUID(t, "sig-resume-missing"))
	require.NoError(t, err)
	assert.False(t, ok, "a missing job is not signal-resumable")
}

func TestResumeSignalWaitingJob_RunningNotResumable(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "sig-resume-running")

	seedTestJob(t, ctx, s, id, core.StatusRunning)

	ok, err := s.ResumeSignalWaitingJob(ctx, id)
	require.NoError(t, err)
	assert.False(t, ok, "only a waiting job is signal-resumable")

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusRunning, after.Status)
}

func TestResumeSignalWaitingJob_DecrementsAttemptCounter(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	id := resumeUUID(t, "sig-resume-attempt")

	// Waiting job that has already consumed an attempt; the signal resume must
	// give it back so the suspend/resume cycle is not counted as a retry.
	require.NoError(t, s.db.WithContext(ctx).Create(&core.Job{
		ID:      id,
		Type:    "fixture.job",
		Queue:   "default",
		Status:  core.StatusWaiting,
		Attempt: 2,
	}).Error)

	ok, err := s.ResumeSignalWaitingJob(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)

	after, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, core.StatusPending, after.Status)
	assert.Equal(t, 1, after.Attempt, "attempt is decremented on signal resume")
}
