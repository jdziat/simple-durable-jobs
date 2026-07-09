package queue

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
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
//
// A panic raised by user middleware in its own before/after code (as opposed to
// a downstream handler panic, which executeHandler already recovers into an
// error) is recovered at this outermost boundary and converted into a failed
// attempt, so a buggy middleware fails the job rather than crashing the worker
// process. Self-suspension (a waiting error) re-raised as a panic is preserved
// as a waiting error, not turned into a failure.
func (q *Queue) RunExecutionMiddleware(ctx context.Context, job *core.Job, invoke func(context.Context, *core.Job) ([]byte, error)) (result []byte, err error) {
	q.mu.RLock()
	mws := make([]ExecutionMiddleware, len(q.executionMiddleware))
	copy(mws, q.executionMiddleware)
	q.mu.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && core.IsWaiting(e) {
				result, err = nil, e
				return
			}
			slog.Default().Error("recovered panic in execution middleware; failing this attempt",
				"job_id", job.ID,
				"panic", r,
				"stack", string(debug.Stack()))
			result, err = nil, fmt.Errorf("panic in execution middleware: %v", r)
		}
	}()

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

// CallErrorHandler invokes the registered execution error observer, if any. A
// panic in the observer is recovered and logged so it cannot crash the worker.
func (q *Queue) CallErrorHandler(ctx context.Context, job *core.Job, err error) {
	q.mu.RLock()
	fn := q.errorHandler
	q.mu.RUnlock()
	if fn != nil {
		safeUserCallback("error handler", job.ID, func() { fn(ctx, job, err) })
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
//
// A panic in the user policy is recovered and the error is treated as a failure
// (the safe, conservative default) so a buggy policy cannot crash the worker or
// silently swallow a real failure.
func (q *Queue) IsFailure(job *core.Job, err error) (isFailure bool) {
	q.mu.RLock()
	fn := q.isFailure
	q.mu.RUnlock()
	if fn == nil {
		return true
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Error("recovered panic in user IsFailure policy; treating error as failure",
				"job_id", job.ID,
				"panic", r,
				"stack", string(debug.Stack()))
			isFailure = true
		}
	}()
	return fn(job, err)
}
