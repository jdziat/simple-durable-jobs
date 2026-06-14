package typed

import (
	"context"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/signal"
)

// Result wraps a typed fan-out sub-job result with its index and error.
type Result[T any] = fanout.Result[T]

// SubJob represents a fan-out sub-job to be spawned.
type SubJob = fanout.SubJob

// FanOut spawns typed sub-jobs in parallel and waits for typed results.
func FanOut[T any](ctx context.Context, subJobs []fanout.SubJob, opts ...fanout.Option) ([]fanout.Result[T], error) {
	return fanout.FanOut[T](ctx, subJobs, opts...)
}

// SubJobOf creates a fan-out sub-job from a typed definition and typed args.
func SubJobOf[A any, R any](def *Def[A, R], args A, opts ...queue.Option) fanout.SubJob {
	return fanout.Sub(def.Name(), args, opts...)
}

// Signal delivers a named signal carrying payload to a job.
// Signal delivers a signal to a job from within the typed workflow API. It is a
// thin wrapper over (*queue.Queue).Signal so the validation, delivery, event
// emission, and immediate signal-resume behavior stay defined in exactly one
// place (R36 moved the canonical implementation onto *Queue).
func Signal(ctx context.Context, q *queue.Queue, jobID core.UUID, name string, payload any) error {
	return q.Signal(ctx, jobID, name, payload)
}

// WaitForSignal consumes the oldest pending signal of name from a job handler.
func WaitForSignal[T any](ctx context.Context, name string) (T, error) {
	return signal.WaitForSignal[T](ctx, name)
}

// WaitForSignalTimeout is WaitForSignal with a deadline.
func WaitForSignalTimeout[T any](ctx context.Context, name string, d time.Duration) (T, bool, error) {
	return signal.WaitForSignalTimeout[T](ctx, name, d)
}
