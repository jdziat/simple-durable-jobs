package queue

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// ExecutionMiddleware wraps a job handler attempt.
//
// The first registered middleware is the outermost wrapper. Middleware can run
// code before or after the handler, replace the context, transform result bytes
// or errors, recover panics from downstream code, or short-circuit by not
// calling next.
type ExecutionMiddleware func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) ([]byte, error)) ([]byte, error)

// UseExecutionMiddleware registers middleware that wraps each job handler
// attempt. Middleware is called in registration order.
func (q *Queue) UseExecutionMiddleware(mw ExecutionMiddleware) {
	q.mu.Lock()
	q.executionMiddleware = append(q.executionMiddleware, mw)
	q.mu.Unlock()
}

// RunExecutionMiddleware executes the handler-attempt middleware chain, ending
// with invoke. The first registered middleware is the outermost wrapper.
func (q *Queue) RunExecutionMiddleware(ctx context.Context, job *core.Job, invoke func(context.Context, *core.Job) ([]byte, error)) ([]byte, error) {
	q.mu.RLock()
	mws := make([]ExecutionMiddleware, len(q.executionMiddleware))
	copy(mws, q.executionMiddleware)
	q.mu.RUnlock()

	next := invoke
	for i := len(mws) - 1; i >= 0; i-- {
		mw := mws[i]
		inner := next
		next = func(ctx context.Context, j *core.Job) ([]byte, error) {
			return mw(ctx, j, inner)
		}
	}
	return next(ctx, job)
}

// SetErrorHandler registers an observer for final handler-attempt errors after
// middleware and IsFailure classification. Waiting errors and swallowed errors
// are not observed.
func (q *Queue) SetErrorHandler(fn func(context.Context, *core.Job, error)) {
	q.mu.Lock()
	q.errorHandler = fn
	q.mu.Unlock()
}

// CallErrorHandler invokes the registered execution error observer, if any.
func (q *Queue) CallErrorHandler(ctx context.Context, job *core.Job, err error) {
	q.mu.RLock()
	fn := q.errorHandler
	q.mu.RUnlock()
	if fn != nil {
		fn(ctx, job, err)
	}
}

// SetIsFailure registers a policy that classifies non-nil, non-waiting handler
// errors as job failures. Returning false treats the attempt as a success.
func (q *Queue) SetIsFailure(fn func(*core.Job, error) bool) {
	q.mu.Lock()
	q.isFailure = fn
	q.mu.Unlock()
}

// IsFailure reports whether err should be handled as a job failure. Nil policy
// preserves the default behavior: every non-nil, non-waiting error fails.
func (q *Queue) IsFailure(job *core.Job, err error) bool {
	q.mu.RLock()
	fn := q.isFailure
	q.mu.RUnlock()
	if fn == nil {
		return true
	}
	return fn(job, err)
}
