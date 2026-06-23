package context

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// JobContextKey is the key for storing job context in context.Context.
type JobContextKey struct{}

// JobContext holds the current job and queue reference.
type JobContext struct {
	Job              *core.Job
	Storage          core.Storage
	WorkerID         string
	BestEffortReplay bool // when true, Call relaxes the replay type-mismatch guard
	// DeterminismStrict, when true, additionally requires that every recorded
	// Call checkpoint is replayed (the handler's Call sequence is fully
	// deterministic). Enforced after the handler returns, not inside Call.
	DeterminismStrict bool
	// Logger is optional and may be nil.
	Logger *slog.Logger
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

// CheckpointKey identifies a durable checkpoint by both call position and type.
type CheckpointKey struct {
	Index int
	Type  string
}

// CallState tracks the current call index for replay.
type CallState struct {
	Mu          sync.Mutex
	CallIndex   int
	Checkpoints map[CheckpointKey]*core.Checkpoint
}

// UnconsumedCallCheckpoints returns how many Call checkpoints (those with a
// real call index, i.e. Index >= 0; phase checkpoints use Index == -1 and are
// excluded) were never reached during this execution.
//
// After a handler runs, Call has consumed indices [0, CallIndex). A checkpoint
// whose index is >= CallIndex was recorded by a previous run but not replayed
// this time — meaning the handler issued fewer (or reordered-away) Calls than
// before. That is a determinism violation that Strict mode reports; the more
// common type-mismatch case is caught inline by Call itself.
func (cs *CallState) UnconsumedCallCheckpoints() int {
	cs.Mu.Lock()
	defer cs.Mu.Unlock()
	n := 0
	for key := range cs.Checkpoints {
		if key.Index >= 0 && key.Index >= cs.CallIndex {
			n++
		}
	}
	return n
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
		Checkpoints: make(map[CheckpointKey]*core.Checkpoint),
	}
	for i := range checkpoints {
		key := CheckpointKey{Index: checkpoints[i].CallIndex, Type: checkpoints[i].CallType}
		cs.Checkpoints[key] = &checkpoints[i]
	}
	return context.WithValue(ctx, CallStateKey{}, cs)
}
