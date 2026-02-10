// Package context provides context helpers for the jobs package.
package context

import (
	"context"
	"sync"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// JobContextKey is the key for storing job context in context.Context.
type JobContextKey struct{}

// JobContext holds the current job and queue reference.
type JobContext struct {
	Job      *core.Job
	Storage  core.Storage
	WorkerID string
	// HandlerLookup is a function to look up handlers by name
	HandlerLookup func(name string) (any, bool)
	// SaveCheckpoint saves a checkpoint to storage
	SaveCheckpoint func(ctx context.Context, cp *core.Checkpoint) error
}

// GetJobContext retrieves the job context from a context.Context.
func GetJobContext(ctx context.Context) *JobContext {
	if jc, ok := ctx.Value(JobContextKey{}).(*JobContext); ok {
		return jc
	}
	return nil
}

// WithJobContext adds job context to a context.Context.
func WithJobContext(ctx context.Context, jc *JobContext) context.Context {
	return context.WithValue(ctx, JobContextKey{}, jc)
}

// CallStateKey is the key for storing call state in context.Context.
type CallStateKey struct{}

// CallState tracks the current call index for replay.
type CallState struct {
	Mu          sync.Mutex
	CallIndex   int
	Checkpoints map[int]*core.Checkpoint
}

// GetCallState retrieves the call state from a context.Context.
func GetCallState(ctx context.Context) *CallState {
	if cs, ok := ctx.Value(CallStateKey{}).(*CallState); ok {
		return cs
	}
	return nil
}

// WithCallState adds call state to a context.Context.
func WithCallState(ctx context.Context, checkpoints []core.Checkpoint) context.Context {
	cs := &CallState{
		Checkpoints: make(map[int]*core.Checkpoint),
	}
	for i := range checkpoints {
		cs.Checkpoints[checkpoints[i].CallIndex] = &checkpoints[i]
	}
	return context.WithValue(ctx, CallStateKey{}, cs)
}
