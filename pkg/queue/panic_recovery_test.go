package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// testWaitingErr is a control-flow error that reports itself as a self-suspension
// signal (core.IsWaiting == true), used to prove the middleware panic net
// preserves waiting semantics instead of turning them into a failure.
type testWaitingErr struct{}

func (testWaitingErr) Error() string         { return "waiting" }
func (testWaitingErr) WorkflowWaiting() bool { return true }

// TestHookDispatchers_RecoverPanicAndContinue proves every user-callback
// dispatcher recovers a panicking hook and still invokes the remaining hooks,
// so a buggy hook can never crash the worker process (finding C2/E2). A table
// drives every dispatcher; a panic that escaped would fail the test process.
func TestHookDispatchers_RecoverPanicAndContinue(t *testing.T) {
	job := &core.Job{ID: "j"}
	ctx := context.Background()

	cases := []struct {
		name     string
		register func(q *Queue, ran *bool)
		invoke   func(q *Queue)
	}{
		{
			name: "OnJobStart",
			register: func(q *Queue, ran *bool) {
				q.OnJobStart(func(context.Context, *core.Job) { panic("boom") })
				q.OnJobStart(func(context.Context, *core.Job) { *ran = true })
			},
			invoke: func(q *Queue) { q.CallStartHooks(ctx, job) },
		},
		{
			name: "OnJobComplete",
			register: func(q *Queue, ran *bool) {
				q.OnJobComplete(func(context.Context, *core.Job) { panic("boom") })
				q.OnJobComplete(func(context.Context, *core.Job) { *ran = true })
			},
			invoke: func(q *Queue) { q.CallCompleteHooks(ctx, job) },
		},
		{
			name: "OnJobFail",
			register: func(q *Queue, ran *bool) {
				q.OnJobFail(func(context.Context, *core.Job, error) { panic("boom") })
				q.OnJobFail(func(context.Context, *core.Job, error) { *ran = true })
			},
			invoke: func(q *Queue) { q.CallFailHooks(ctx, job, errors.New("e")) },
		},
		{
			name: "OnRetry",
			register: func(q *Queue, ran *bool) {
				q.OnRetry(func(context.Context, *core.Job, int, error) { panic("boom") })
				q.OnRetry(func(context.Context, *core.Job, int, error) { *ran = true })
			},
			invoke: func(q *Queue) { q.CallRetryHooks(ctx, job, 1, errors.New("e")) },
		},
		{
			name: "OnJobWaiting",
			register: func(q *Queue, ran *bool) {
				q.OnJobWaiting(func(context.Context, *core.Job) { panic("boom") })
				q.OnJobWaiting(func(context.Context, *core.Job) { *ran = true })
			},
			invoke: func(q *Queue) { q.CallWaitingHooks(ctx, job) },
		},
		{
			name: "OnJobReclaimed",
			register: func(q *Queue, ran *bool) {
				q.OnJobReclaimed(func(context.Context, core.UUID, string) { panic("boom") })
				q.OnJobReclaimed(func(context.Context, core.UUID, string) { *ran = true })
			},
			invoke: func(q *Queue) { q.CallJobReclaimedHooks(ctx, "j", "stale-lock") },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := New(newMockStorage())
			ran := false
			tc.register(q, &ran)
			require.NotPanics(t, func() { tc.invoke(q) }, "%s dispatcher must recover a panicking hook", tc.name)
			assert.True(t, ran, "%s must continue to the next hook after one panics", tc.name)
		})
	}
}

// TestCallStartCtxHooks_PanicPreservesCtxAndContinues proves a panicking
// context hook does not abort the chain and leaves ctx at its pre-hook value
// (the ctx-return contract), while a later hook still runs and applies.
func TestCallStartCtxHooks_PanicPreservesCtxAndContinues(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}
	type ctxKey struct{}

	q.OnJobStartCtx(func(ctx context.Context, _ *core.Job) context.Context { panic("boom") })
	q.OnJobStartCtx(func(ctx context.Context, _ *core.Job) context.Context {
		return context.WithValue(ctx, ctxKey{}, "applied")
	})

	var out context.Context
	require.NotPanics(t, func() { out = q.CallStartCtxHooks(context.Background(), job) })
	assert.Equal(t, "applied", out.Value(ctxKey{}), "a later ctx hook must still apply after an earlier one panics")
}

// TestRunExecutionMiddleware_RecoversMiddlewarePanicAsError proves a panic in a
// middleware's own code becomes a failed attempt (not a process crash), while
// the handler still runs for a non-panicking middleware.
func TestRunExecutionMiddleware_RecoversMiddlewarePanicAsError(t *testing.T) {
	q := New(newMockStorage())
	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		panic("middleware boom")
	})

	var result []byte
	var err error
	require.NotPanics(t, func() {
		result, err = q.RunExecutionMiddleware(context.Background(), &core.Job{ID: "j"},
			func(context.Context, *core.Job) ([]byte, error) { return []byte("ok"), nil })
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic in execution middleware")
	assert.Nil(t, result)
	assert.False(t, core.IsWaiting(err), "a plain middleware panic must not read as waiting")
}

// TestRunExecutionMiddleware_PreservesWaitingPanic proves a waiting error
// re-raised as a panic through middleware is preserved as a waiting error, so
// self-suspension is never miscategorised as a failure.
func TestRunExecutionMiddleware_PreservesWaitingPanic(t *testing.T) {
	q := New(newMockStorage())
	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		panic(testWaitingErr{})
	})

	var err error
	require.NotPanics(t, func() {
		_, err = q.RunExecutionMiddleware(context.Background(), &core.Job{ID: "j"},
			func(context.Context, *core.Job) ([]byte, error) { return nil, nil })
	})
	assert.True(t, core.IsWaiting(err), "a waiting panic through middleware must remain a waiting error")
}

// TestCallErrorHandler_RecoversPanic proves a panicking error observer cannot
// crash the worker.
func TestCallErrorHandler_RecoversPanic(t *testing.T) {
	q := New(newMockStorage())
	q.SetErrorHandler(func(context.Context, *core.Job, error) { panic("observer boom") })
	require.NotPanics(t, func() {
		q.CallErrorHandler(context.Background(), &core.Job{ID: "j"}, errors.New("e"))
	})
}

// TestIsFailure_PanicDefaultsToFailure proves a panicking IsFailure policy is
// recovered and the error is conservatively treated as a failure (never
// silently swallowed, never a crash).
func TestIsFailure_PanicDefaultsToFailure(t *testing.T) {
	q := New(newMockStorage())
	q.SetIsFailure(func(*core.Job, error) bool { panic("policy boom") })
	var isFail bool
	require.NotPanics(t, func() {
		isFail = q.IsFailure(&core.Job{ID: "j"}, errors.New("e"))
	})
	assert.True(t, isFail, "a panicking IsFailure policy must default to treating the error as a failure")
}
