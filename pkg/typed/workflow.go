package typed

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
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
func Signal(ctx context.Context, q *queue.Queue, jobID, name string, payload any) error {
	if name == "" {
		return fmt.Errorf("jobs: signal name must not be empty")
	}
	if strings.HasPrefix(name, "_") {
		return signal.ErrSignalNameReserved
	}
	if len(name) > security.MaxSignalNameLength {
		return core.ErrSignalNameTooLong
	}
	type signalSender interface {
		SendSignal(ctx context.Context, jobID, name string, payload []byte) error
	}
	sender, ok := q.Storage().(signalSender)
	if !ok {
		return core.ErrStorageNoSignals
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("jobs: marshal signal payload: %w", err)
	}
	if len(data) > security.MaxResultSize {
		return fmt.Errorf("jobs: signal %q payload is %d bytes, limit is %d", name, len(data), security.MaxResultSize)
	}

	job, err := q.Storage().GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("%w: %s", core.ErrJobNotFound, jobID)
	}

	if err := sender.SendSignal(ctx, jobID, name, data); err != nil {
		return err
	}
	q.Emit(&core.SignalDelivered{JobID: jobID, Name: name, Timestamp: time.Now()})

	if job.Status == core.StatusWaiting {
		if signal.WaitingOnFutureSleep(ctx, q.Storage(), job, slog.Default()) {
			return nil
		}
		type signalResumer interface {
			ResumeSignalWaitingJob(ctx context.Context, jobID string) (bool, error)
		}
		if r, ok := q.Storage().(signalResumer); ok {
			resumed, err := r.ResumeSignalWaitingJob(ctx, jobID)
			if err != nil {
				return fmt.Errorf("jobs: signal sent but resume failed (poll will recover): %w", err)
			}
			if resumed {
				q.Emit(&core.JobResumedBySignal{JobID: jobID, SignalName: name, Timestamp: time.Now()})
			}
		}
	}
	return nil
}

// WaitForSignal consumes the oldest pending signal of name from a job handler.
func WaitForSignal[T any](ctx context.Context, name string) (T, error) {
	return signal.WaitForSignal[T](ctx, name)
}

// WaitForSignalTimeout is WaitForSignal with a deadline.
func WaitForSignalTimeout[T any](ctx context.Context, name string, d time.Duration) (T, bool, error) {
	return signal.WaitForSignalTimeout[T](ctx, name, d)
}
