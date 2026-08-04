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
	// phasesSaved holds the phase names THIS execution has written, which is
	// what tells a duplicate name apart from a legitimate replay: checkpoints
	// loaded from an earlier run are not in it.
	phasesSaved map[string]struct{}
	// LegacySpanWarned records that this execution has already warned about
	// replaying pre-span checkpoints, so the warning fires once per run rather
	// than once per call.
	LegacySpanWarned bool
}

// HasLegacyCallSpans reports whether this job carries MORE THAN ONE user Call()
// checkpoint written before span tracking existed (SpanEnd == 0).
//
// Such a job is a candidate for the pre-v4.6 nested-call defect: its indices were
// assigned by the old flat counter, so if any of those calls nested, every later
// call reads a checkpoint one or more slots too low and the workflow can complete
// carrying another call's result.
//
// This is deliberately a conservative OVER-approximation. Nothing persisted tells
// us whether a legacy call actually nested, so a purely flat workflow with two or
// more calls is also reported. Flagging safe work is cheap; missing genuinely
// corrupted work is not.
func (cs *CallState) HasLegacyCallSpans() bool {
	cs.Mu.Lock()
	defer cs.Mu.Unlock()
	legacy := 0
	for key, cp := range cs.Checkpoints {
		if key.Index < 0 || cp == nil {
			continue // phase checkpoints use Index == -1
		}
		if !core.IsCallCheckpointType(cp.CallType) {
			// A built-in durable operation (fan-out, signal wait). Only Call()
			// records a span, so these carry SpanEnd == 0 in every version
			// including this one — counting them warns about healthy work.
			continue
		}
		if cp.SpanEnd == 0 {
			legacy++
			if legacy > 1 {
				return true
			}
		}
	}
	return false
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

// HasUnreachedCallCheckpoints reports whether any indexed durable checkpoint
// recorded by an EARLIER execution sits at or beyond the current call cursor —
// i.e. a previous run of this job got PAST the point the handler occupies right
// now, and this run has not replayed that far yet.
//
// This is the library's replay signal, and it is sound in both directions
// because of one invariant: every indexed durable operation (Call, fan-out,
// signal wait, sleep) RESERVES its index by incrementing CallIndex before it
// writes, and none of them insert into Checkpoints. So the map holds exactly the
// rows loaded from storage at run start (plus Index == -1 markers this run
// wrote, which are excluded), and every row this run has already consumed sits
// at an index strictly BELOW the cursor.
//
//   - A first execution therefore never reports true: it has no loaded rows, and
//     nothing it writes itself can land at or above the cursor.
//   - A replay reports true exactly while the earlier run's unreplayed durable
//     work is still ahead of it.
//
// The residual is one-sided and harmless: a replay that passed this point
// WITHOUT recording any durable step at or after it is indistinguishable from a
// first execution — but by definition there is then no recorded step for a
// changed handler shape to collide with.
//
// Deliberately delegates to UnconsumedCallCheckpoints rather than repeating the
// predicate: the two must never disagree about which rows count.
func (cs *CallState) HasUnreachedCallCheckpoints() bool {
	return cs.UnconsumedCallCheckpoints() > 0
}

// ReservePhaseName claims phaseName for this execution and reports whether the
// claim is new. A phase checkpoint is keyed {Index: -1, Type: name}, so a second
// phase reusing a name is the SAME checkpoint: it overwrites the first, and
// afterwards neither phase can be told from the other on replay.
//
// Only writes made by this run reserve, so a phase the handler redoes in a later
// run — its effect rolled back, or the run that recorded it never finished —
// still saves.
func (cs *CallState) ReservePhaseName(phaseName string) bool {
	cs.Mu.Lock()
	defer cs.Mu.Unlock()
	if _, dup := cs.phasesSaved[phaseName]; dup {
		return false
	}
	if cs.phasesSaved == nil {
		cs.phasesSaved = make(map[string]struct{})
	}
	cs.phasesSaved[phaseName] = struct{}{}
	return true
}

// ReleasePhaseName drops a reservation whose checkpoint write failed, so a
// handler retrying that same phase in this run is not wedged by a save that
// persisted nothing.
func (cs *CallState) ReleasePhaseName(phaseName string) {
	cs.Mu.Lock()
	defer cs.Mu.Unlock()
	delete(cs.phasesSaved, phaseName)
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
