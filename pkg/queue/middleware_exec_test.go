package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Covers pkg/queue/middleware_exec.go, which was entirely untested:
// execution-middleware registration/ordering/short-circuit, the error-handler
// observer, and the is-failure classification policy.

// ──────────────────────────────────────────────────────────────────────────────
// RunExecutionMiddleware — ordering, transforms, short-circuit
// ──────────────────────────────────────────────────────────────────────────────

func TestRunExecutionMiddleware_NoMiddlewareCallsInvoke(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}

	called := false
	out, err := q.RunExecutionMiddleware(context.Background(), job,
		func(ctx context.Context, j *core.Job) ([]byte, error) {
			called = true
			return []byte("ok"), nil
		})
	require.NoError(t, err)
	assert.True(t, called, "invoke must run when no middleware is registered")
	assert.Equal(t, "ok", string(out))
}

func TestRunExecutionMiddleware_RegistrationOrderIsOutermostFirst(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}

	var order []string
	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		order = append(order, "A.before")
		out, err := next(ctx, j)
		order = append(order, "A.after")
		return out, err
	})
	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		order = append(order, "B.before")
		out, err := next(ctx, j)
		order = append(order, "B.after")
		return out, err
	})

	_, err := q.RunExecutionMiddleware(context.Background(), job,
		func(ctx context.Context, j *core.Job) ([]byte, error) {
			order = append(order, "handler")
			return nil, nil
		})
	require.NoError(t, err)
	// First-registered (A) is the outermost wrapper.
	assert.Equal(t, []string{"A.before", "B.before", "handler", "B.after", "A.after"}, order)
}

func TestRunExecutionMiddleware_CanShortCircuitWithoutCallingNext(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}

	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		return []byte("short"), nil // never calls next
	})

	handlerRan := false
	out, err := q.RunExecutionMiddleware(context.Background(), job,
		func(ctx context.Context, j *core.Job) ([]byte, error) {
			handlerRan = true
			return []byte("handler"), nil
		})
	require.NoError(t, err)
	assert.False(t, handlerRan, "short-circuiting middleware must not call the handler")
	assert.Equal(t, "short", string(out))
}

func TestRunExecutionMiddleware_TransformsResultAndError(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}
	sentinel := errors.New("downstream")

	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		out, err := next(ctx, j)
		if err != nil {
			return []byte("recovered"), nil // swallow and replace
		}
		return append(out, '!'), nil
	})

	out, err := q.RunExecutionMiddleware(context.Background(), job,
		func(ctx context.Context, j *core.Job) ([]byte, error) {
			return nil, sentinel
		})
	require.NoError(t, err, "middleware swallowed the downstream error")
	assert.Equal(t, "recovered", string(out))
}

func TestRunExecutionMiddleware_PropagatesContextToHandler(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}
	type ctxKey struct{}

	q.UseExecutionMiddleware(func(ctx context.Context, j *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
		return next(context.WithValue(ctx, ctxKey{}, "v"), j)
	})

	var got any
	_, err := q.RunExecutionMiddleware(context.Background(), job,
		func(ctx context.Context, j *core.Job) ([]byte, error) {
			got = ctx.Value(ctxKey{})
			return nil, nil
		})
	require.NoError(t, err)
	assert.Equal(t, "v", got, "context replaced by middleware reaches the handler")
}

// ──────────────────────────────────────────────────────────────────────────────
// SetErrorHandler / CallErrorHandler
// ──────────────────────────────────────────────────────────────────────────────

func TestCallErrorHandler_NilIsSafeNoop(t *testing.T) {
	q := New(newMockStorage())
	// No handler registered — must not panic.
	q.CallErrorHandler(context.Background(), &core.Job{ID: "j"}, errors.New("boom"))
}

func TestCallErrorHandler_InvokesRegisteredObserver(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}
	wantErr := errors.New("boom")

	var gotJob *core.Job
	var gotErr error
	q.SetErrorHandler(func(ctx context.Context, j *core.Job, err error) {
		gotJob = j
		gotErr = err
	})

	q.CallErrorHandler(context.Background(), job, wantErr)
	assert.Same(t, job, gotJob)
	assert.Same(t, wantErr, gotErr)
}

// ──────────────────────────────────────────────────────────────────────────────
// SetIsFailure / IsFailure
// ──────────────────────────────────────────────────────────────────────────────

func TestIsFailure_DefaultPolicyFailsEveryError(t *testing.T) {
	q := New(newMockStorage())
	// No policy registered: every non-nil error is a failure.
	assert.True(t, q.IsFailure(&core.Job{ID: "j"}, errors.New("boom")))
}

func TestIsFailure_CustomPolicyCanTreatErrorAsSuccess(t *testing.T) {
	q := New(newMockStorage())
	skip := errors.New("skip")

	q.SetIsFailure(func(j *core.Job, err error) bool {
		return !errors.Is(err, skip) // skip is a non-failure
	})

	assert.False(t, q.IsFailure(&core.Job{ID: "j"}, skip), "policy classifies skip as success")
	assert.True(t, q.IsFailure(&core.Job{ID: "j"}, errors.New("other")), "other errors still fail")
}
