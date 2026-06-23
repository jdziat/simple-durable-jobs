// Package typed provides type-safe handles over the string-keyed durable job API.
package typed

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/call"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// Def is a typed handle to a registered job definition.
//
// A is the job argument type and R is the persisted result type. Def keeps the
// existing string-keyed routing model: it stores the queue and registered job
// name, then delegates enqueueing, nested calls, and result loading to the
// existing durable jobs APIs.
type Def[A any, R any] struct {
	q    *queue.Queue
	name string
}

// Define registers a typed job handler and returns its typed definition.
//
// Define panics on invalid registration, matching queue.Queue.Register. Use
// DefineE when handler names or functions are configuration-driven and should
// return errors instead.
func Define[A any, R any](
	q *queue.Queue,
	name string,
	fn any,
	opts ...queue.Option,
) *Def[A, R] {
	def, err := DefineE[A, R](q, name, fn, opts...)
	if err != nil {
		panic(err)
	}
	return def
}

// DefineE registers a typed job handler and returns validation errors.
//
// The handler is registered through queue.Queue.RegisterE, so routing remains
// string-keyed and execution uses the same reflective handler path as the
// untyped API.
func DefineE[A any, R any](
	q *queue.Queue,
	name string,
	fn any,
	opts ...queue.Option,
) (*Def[A, R], error) {
	h, err := handler.NewHandler(fn)
	if err != nil {
		return nil, fmt.Errorf("jobs: handler for %q: %w", name, err)
	}
	wantA := reflect.TypeOf((*A)(nil)).Elem()
	if h.ArgsType == nil {
		return nil, fmt.Errorf("jobs: typed definition %q argument type mismatch: handler accepts no arguments, definition expects %s", name, wantA)
	}
	if !wantA.AssignableTo(h.ArgsType) {
		return nil, fmt.Errorf("jobs: typed definition %q argument type mismatch: handler accepts %s, definition expects %s", name, h.ArgsType, wantA)
	}
	wantR := reflect.TypeOf((*R)(nil)).Elem()
	if h.ResultType == nil {
		return nil, fmt.Errorf("jobs: typed definition %q result type mismatch: handler returns no result, definition expects %s", name, wantR)
	}
	if !h.ResultType.AssignableTo(wantR) {
		return nil, fmt.Errorf("jobs: typed definition %q result type mismatch: handler returns %s, definition expects %s", name, h.ResultType, wantR)
	}
	if err := q.RegisterE(name, fn, opts...); err != nil {
		return nil, err
	}
	return &Def[A, R]{q: q, name: name}, nil
}

// DeclareUnchecked returns a typed definition handle without registering a handler.
//
// Use DeclareUnchecked in producer-only processes that enqueue work for workers
// running elsewhere. The returned handle remains string-routed by name and does
// not create a type-keyed registry.
//
// Warning: A and R are unchecked on this producer-only EnqueueRemote path.
// There is no registered handler for DeclareUnchecked to validate them against,
// so they must be hand-synchronized with the remote worker's handler signature
// or the worker will fail to decode the payload.
func DeclareUnchecked[A any, R any](q *queue.Queue, name string) *Def[A, R] {
	return &Def[A, R]{q: q, name: name}
}

// DefineVoid registers an error-only typed job handler.
//
// DefineVoid adapts fn to a handler returning struct{} so a completed job has a
// typed result value that can be used with Def.Call and Def.Load.
func DefineVoid[A any](
	q *queue.Queue,
	name string,
	fn func(context.Context, A) error,
	opts ...queue.Option,
) *Def[A, struct{}] {
	wrapped := func(ctx context.Context, args A) (struct{}, error) {
		return struct{}{}, fn(ctx, args)
	}
	return Define[A, struct{}](q, name, wrapped, opts...)
}

// Name returns the string job type registered for d.
func (d *Def[A, R]) Name() string {
	return d.name
}

// Enqueue adds a typed job to the queue.
//
// Arguments are passed unchanged to queue.Queue.Enqueue, preserving the same
// JSON marshaling, size checks, middleware, and payload codec behavior as the
// untyped API.
func (d *Def[A, R]) Enqueue(ctx context.Context, args A, opts ...queue.Option) (core.UUID, error) {
	return d.q.Enqueue(ctx, d.name, args, opts...)
}

// EnqueueRemote adds a typed job without requiring a local handler registration.
//
// Arguments are passed unchanged to queue.Queue.EnqueueRemote, preserving the
// same wire format as untyped remote enqueue while checking A at compile time.
func (d *Def[A, R]) EnqueueRemote(ctx context.Context, args A, opts ...queue.Option) (core.UUID, error) {
	return d.q.EnqueueRemote(ctx, d.name, args, opts...)
}

// EnqueueTx adds a typed job through a caller-owned GORM transaction.
//
// Arguments are passed unchanged to queue.Queue.EnqueueTx. The caller remains
// responsible for committing or rolling back tx.
func (d *Def[A, R]) EnqueueTx(ctx context.Context, tx *gorm.DB, args A, opts ...queue.Option) (core.UUID, error) {
	return d.q.EnqueueTx(ctx, tx, d.name, args, opts...)
}

// Call executes d as a durable nested call from inside another job handler.
//
// Call delegates to pkg/call.Call using d's registered name and typed
// arguments, so checkpointing and replay semantics remain identical to the
// untyped Call API.
func (d *Def[A, R]) Call(ctx context.Context, args A) (R, error) {
	return call.Call[R](ctx, d.name, args)
}

// Load decodes the persisted result for a completed job enqueued through d.
//
// Load uses the queue's public storage API to read the job row and decodes the
// result bytes into R. It returns the same sentinel errors as jobs.LoadResult:
// ErrJobNotCompleted for non-terminal jobs, ErrJobFailed for failed jobs,
// ErrJobCancelled for cancelled jobs, ErrNoResult for completed jobs without a
// result, and ErrJobNotFound when the ID does not exist.
func (d *Def[A, R]) Load(ctx context.Context, jobID core.UUID) (R, error) {
	var zero R
	job, err := d.q.Storage().GetJob(ctx, jobID)
	if err != nil {
		return zero, err
	}
	if job == nil {
		return zero, fmt.Errorf("%w: %s", core.ErrJobNotFound, jobID)
	}
	switch job.Status {
	case core.StatusCompleted:
		if job.Result == nil {
			return zero, core.ErrNoResult
		}
		var out R
		if err := json.Unmarshal(job.Result, &out); err != nil {
			return zero, fmt.Errorf("jobs: failed to decode result: %w", err)
		}
		return out, nil
	case core.StatusFailed:
		return zero, fmt.Errorf("%w: %s", core.ErrJobFailed, job.LastError)
	case core.StatusCancelled:
		return zero, fmt.Errorf("%w: %s", core.ErrJobCancelled, jobID)
	default:
		return zero, core.ErrJobNotCompleted
	}
}
