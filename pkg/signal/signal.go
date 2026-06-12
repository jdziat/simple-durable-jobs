// Package signal provides durable, named workflow signals: an external producer
// can deliver a message to a running or waiting job, and the job's handler can
// wait for, drain, or peek those messages. Signals are buffered (one sent before
// the handler waits is not lost) and delivered FIFO per (job, name).
//
// Like Call and FanOut, every signal observation is checkpointed by call index
// so handler replay is deterministic: a wait that consumed payload X, a peek
// that found nothing, or a timeout that fired all replay to the same outcome.
package signal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v2/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/security"
)

// maxSignalNameLen bounds the signal name (matches the storage column size).
const maxSignalNameLen = security.MaxSignalNameLength

// SleepCheckpointType is the internal checkpoint CallType used by durable
// timers. The leading underscore reserves it outside the user Call/phase
// checkpoint namespace.
const SleepCheckpointType = "_sleep"

// ErrSignalNameReserved indicates a signal name uses a prefix reserved for
// library-internal workflow primitives.
var ErrSignalNameReserved = errors.New("signal: names starting with _ are reserved for internal use")

// signalStorage is the optional storage capability signals require. GormStorage
// implements it; backends that don't get core.ErrStorageNoSignals. MarkWaiting
// (indefinite wait) is part of core.Storage and used directly.
type signalStorage interface {
	SendSignal(ctx context.Context, jobID, name string, payload []byte) error
	PeekSignal(ctx context.Context, jobID, name string) (*core.Signal, error)
	// ConsumeSignalTx consumes the oldest pending signal of name AND persists
	// the replay checkpoint built from it in ONE transaction (atomic
	// consume+checkpoint). Replaces the old ConsumeSignal+separate-writeCheckpoint
	// torn-write pair.
	ConsumeSignalTx(ctx context.Context, jobID, name string, buildCheckpoint func(sig *core.Signal) (*core.Checkpoint, error)) (*core.Signal, error)
	// DrainSignalsTx consumes ALL pending signals of name AND persists a single
	// replay checkpoint built from the batch in ONE transaction (the closure is
	// always invoked, even for an empty batch).
	DrainSignalsTx(ctx context.Context, jobID, name string, buildCheckpoint func(sigs []*core.Signal) (*core.Checkpoint, error)) ([]*core.Signal, error)
	MarkWaitingWithDeadline(ctx context.Context, jobID, workerID string, d time.Duration) error
}

// WaitingError signals the worker that the job has suspended itself into
// StatusWaiting pending a signal. The worker treats it as "stop, do not fail"
// (via core.IsWaiting), identical to a fan-out wait.
type WaitingError struct{ Name string }

func (e *WaitingError) Error() string {
	if e.Name == SleepCheckpointType {
		return "job waiting for durable timer"
	}
	return fmt.Sprintf("job waiting for signal %q", e.Name)
}
func (e *WaitingError) WorkflowWaiting() bool { return true }

// IsWaitingError reports whether err is a signal wait produced here.
func IsWaitingError(err error) bool {
	_, ok := err.(*WaitingError)
	return ok
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("signal: name must not be empty")
	}
	if strings.HasPrefix(name, "_") {
		return ErrSignalNameReserved
	}
	if len(name) > maxSignalNameLen {
		return core.ErrSignalNameTooLong
	}
	return nil
}

func parts(ctx context.Context) (*intctx.JobContext, *intctx.CallState, signalStorage, error) {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil, nil, nil, fmt.Errorf("signal: must be used within a job handler")
	}
	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return nil, nil, nil, fmt.Errorf("signal: call state not initialized")
	}
	ss, ok := jc.Storage.(signalStorage)
	if !ok {
		return nil, nil, nil, core.ErrStorageNoSignals
	}
	return jc, cs, ss, nil
}

func nextCheckpoint(cs *intctx.CallState, ctype string) (idx int, cp *core.Checkpoint, has bool) {
	cs.Mu.Lock()
	defer cs.Mu.Unlock()
	idx = cs.CallIndex
	cs.CallIndex++
	cp, has = cs.Checkpoints[intctx.CheckpointKey{Index: idx, Type: ctype}]
	return idx, cp, has
}

func writeCheckpoint(ctx context.Context, jc *intctx.JobContext, idx int, ctype string, result []byte) error {
	cp := &core.Checkpoint{
		ID:        core.NewID(),
		JobID:     jc.Job.ID,
		CallIndex: idx,
		CallType:  ctype,
		Result:    result,
	}
	if err := jc.SaveCheckpoint(ctx, cp); err != nil {
		return fmt.Errorf("signal: save checkpoint: %w", err)
	}
	return nil
}

func unmarshalInto[T any](payload []byte) (T, error) {
	var v T
	if len(payload) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(payload, &v); err != nil {
		return v, fmt.Errorf("signal: unmarshal payload: %w", err)
	}
	return v, nil
}

// WaitForSignal consumes the oldest pending signal of name. If none is pending
// the job suspends to StatusWaiting until one arrives, then resumes and returns
// it. Consuming a signal is exactly-once across all wait/drain calls. The
// handler must be replay-safe (its progress checkpointed via Call/phase
// checkpoints), because suspending unwinds and replays the handler on resume.
func WaitForSignal[T any](ctx context.Context, name string) (T, error) {
	var zero T
	jc, cs, ss, err := parts(ctx)
	if err != nil {
		return zero, err
	}
	if err := validateName(name); err != nil {
		return zero, err
	}
	ctype := "signal:" + name

	idx, cp, has := nextCheckpoint(cs, ctype)
	if has {
		return unmarshalInto[T](cp.Result)
	}

	// Consume the signal and persist its replay checkpoint atomically. A crash
	// between the two used to wedge the waiting job forever; the single tx makes
	// rollback (signal stays pending) or commit (checkpoint present) the only
	// outcomes.
	sig, err := ss.ConsumeSignalTx(ctx, jc.Job.ID, name, func(s *core.Signal) (*core.Checkpoint, error) {
		return &core.Checkpoint{
			ID:        core.NewID(),
			JobID:     jc.Job.ID,
			CallIndex: idx,
			CallType:  ctype,
			Result:    s.Payload,
		}, nil
	})
	if err != nil {
		return zero, fmt.Errorf("signal: consume: %w", err)
	}
	if sig == nil {
		// No signal yet — suspend until one arrives (resumed by the Signal()
		// fast path or the signal-resume poll). NO checkpoint written.
		if err := jc.Storage.MarkWaiting(ctx, jc.Job.ID, jc.WorkerID); err != nil {
			return zero, fmt.Errorf("signal: suspend: %w", err)
		}
		return zero, &WaitingError{Name: name}
	}
	return unmarshalInto[T](sig.Payload)
}

type timeoutCheckpoint struct {
	DeadlineUnixNano int64           `json:"deadline"`
	Resolved         bool            `json:"resolved"`
	TimedOut         bool            `json:"timed_out,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}

type sleepCheckpoint struct {
	DeadlineUnixNano int64 `json:"deadline"`
	Resolved         bool  `json:"resolved"`
}

type checkpointReader interface {
	GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error)
}

// WaitingOnFutureSleep reports whether job is currently parked by an
// unresolved durable timer whose wake deadline is still in the future. It is
// used by signal resume paths so buffered user signals do not wake a Sleep.
func WaitingOnFutureSleep(ctx context.Context, s checkpointReader, job *core.Job, logger *slog.Logger) bool {
	if job == nil || job.RunAt == nil {
		return false
	}
	// run_at is written from the DB clock on multi-worker backends, but this
	// guard compares it to the local worker clock. Any skew is bounded and
	// self-healing: the 5s backstop re-polls, and a spurious early resume simply
	// re-suspends with the remaining time.
	if !job.RunAt.After(time.Now()) {
		return false
	}
	checkpoints, err := s.GetCheckpoints(ctx, job.ID)
	if err != nil {
		if logger != nil {
			logger.Warn("failed to inspect durable timer checkpoint before signal resume",
				"job_id", job.ID,
				"error", err)
		}
		return false
	}
	for _, cp := range checkpoints {
		if cp.CallIndex < 0 || cp.CallType != SleepCheckpointType {
			continue
		}
		var state sleepCheckpoint
		if err := json.Unmarshal(cp.Result, &state); err != nil {
			continue
		}
		if state.DeadlineUnixNano != 0 && !state.Resolved {
			if logger != nil {
				logger.Debug("suppressed signal resume for durable timer",
					"job_id", job.ID,
					"run_at", job.RunAt,
					"deadline", time.Unix(0, state.DeadlineUnixNano))
			}
			return true
		}
	}
	return false
}

// WaitForSignalTimeout is WaitForSignal with a deadline: it returns
// (value, true, nil) when a signal arrives within d, or (zero, false, nil) if
// the deadline passes first. The deadline is fixed on first execution and
// checkpointed, so replays agree on it.
func WaitForSignalTimeout[T any](ctx context.Context, name string, d time.Duration) (T, bool, error) {
	var zero T
	jc, cs, ss, err := parts(ctx)
	if err != nil {
		return zero, false, err
	}
	if err := validateName(name); err != nil {
		return zero, false, err
	}
	ctype := "signaltimeout:" + name

	idx, cp, has := nextCheckpoint(cs, ctype)
	var tc timeoutCheckpoint
	if has {
		if err := json.Unmarshal(cp.Result, &tc); err != nil {
			return zero, false, fmt.Errorf("signal: unmarshal timeout checkpoint: %w", err)
		}
		if tc.Resolved {
			if tc.TimedOut {
				return zero, false, nil
			}
			v, uerr := unmarshalInto[T](tc.Payload)
			return v, uerr == nil, uerr
		}
	} else {
		tc.DeadlineUnixNano = time.Now().Add(d).UnixNano()
	}

	// Signal-present branch: consume + write the resolved timeout checkpoint in
	// one tx. The closure marshals the JSON-wrapped {resolved,payload} object as
	// the checkpoint Result (NOT the raw signal payload), so replay reconstructs
	// the timeout outcome.
	sig, err := ss.ConsumeSignalTx(ctx, jc.Job.ID, name, func(s *core.Signal) (*core.Checkpoint, error) {
		tc.Resolved = true
		tc.Payload = s.Payload
		data, e := json.Marshal(tc)
		if e != nil {
			return nil, fmt.Errorf("signal: marshal checkpoint: %w", e)
		}
		return &core.Checkpoint{
			ID:        core.NewID(),
			JobID:     jc.Job.ID,
			CallIndex: idx,
			CallType:  ctype,
			Result:    data,
		}, nil
	})
	if err != nil {
		return zero, false, fmt.Errorf("signal: consume: %w", err)
	}
	if sig != nil {
		v, uerr := unmarshalInto[T](sig.Payload)
		return v, uerr == nil, uerr
	}

	if time.Now().UnixNano() >= tc.DeadlineUnixNano {
		tc.Resolved = true
		tc.TimedOut = true
		if err := writeCheckpointObj(ctx, jc, idx, ctype, tc); err != nil {
			return zero, false, err
		}
		return zero, false, nil
	}

	// Still waiting and before the deadline. On first execution persist the
	// (unresolved) checkpoint so the deadline survives replays; then suspend with
	// the REMAINING time until the deadline. Using the remaining duration (not the
	// original d) means a spurious early resume — e.g. resumed by an unrelated
	// signal, or by a worker whose clock lagged the deadline — re-suspends for
	// what's left rather than restarting the full timeout.
	if !has {
		if err := writeCheckpointObj(ctx, jc, idx, ctype, tc); err != nil {
			return zero, false, err
		}
	}
	remaining := max(time.Until(time.Unix(0, tc.DeadlineUnixNano)), 0)
	if err := ss.MarkWaitingWithDeadline(ctx, jc.Job.ID, jc.WorkerID, remaining); err != nil {
		return zero, false, fmt.Errorf("signal: suspend: %w", err)
	}
	return zero, false, &WaitingError{Name: name}
}

// Sleep durably pauses the current job until at least d has elapsed. It may
// only be called from inside a registered job handler. While sleeping, the job
// is stored as StatusWaiting with a wake deadline in run_at and does not occupy
// a worker slot; any worker can resume it after the deadline. The deadline is
// fixed and checkpointed on first execution, so a crash/restart does not restart
// the duration and replay after completion returns immediately.
//
// Durable timers require storage that implements the optional signal capability
// (GormStorage does); storage without it returns core.ErrStorageNoSignals.
//
// Non-positive durations return nil without suspending, but still record a
// resolved checkpoint so replay remains deterministic. Wakeups are coarse: the
// worker detects elapsed sleeps through the existing waiting-job polling
// backstop, currently about every 5 seconds, plus normal dispatch polling. Sleep
// is suitable for retry pacing, scheduling gaps, and backoff, not sub-second
// timing.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return sleepUntil(ctx, time.Now(), true)
	}
	return sleepUntil(ctx, time.Now().Add(d), false)
}

// SleepUntil durably pauses the current job until t. It has the same durability,
// replay, worker-slot, and coarse wake-granularity semantics as Sleep. Times in
// the past return nil without suspending, but still record a resolved
// checkpoint so replay remains deterministic. Durable timers require storage
// that implements the optional signal capability (GormStorage does); storage
// without it returns core.ErrStorageNoSignals.
func SleepUntil(ctx context.Context, t time.Time) error {
	return sleepUntil(ctx, t, !t.After(time.Now()))
}

func sleepUntil(ctx context.Context, deadline time.Time, immediate bool) error {
	jc, cs, ss, err := parts(ctx)
	if err != nil {
		return err
	}

	const ctype = SleepCheckpointType
	idx, cp, has := nextCheckpoint(cs, ctype)
	var sc sleepCheckpoint
	if has {
		if err := json.Unmarshal(cp.Result, &sc); err != nil {
			return fmt.Errorf("signal: unmarshal sleep checkpoint: %w", err)
		}
		if sc.Resolved {
			return nil
		}
	} else {
		sc.DeadlineUnixNano = deadline.UnixNano()
		sc.Resolved = immediate
		if sc.Resolved {
			return writeCheckpointObj(ctx, jc, idx, ctype, sc)
		}
		if err := writeCheckpointObj(ctx, jc, idx, ctype, sc); err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if time.Now().UnixNano() >= sc.DeadlineUnixNano {
		sc.Resolved = true
		return writeCheckpointObj(ctx, jc, idx, ctype, sc)
	}

	remaining := max(time.Until(time.Unix(0, sc.DeadlineUnixNano)), 0)
	if err := ss.MarkWaitingWithDeadline(ctx, jc.Job.ID, jc.WorkerID, remaining); err != nil {
		return fmt.Errorf("signal: sleep suspend: %w", err)
	}
	return &WaitingError{Name: SleepCheckpointType}
}

type peekCheckpoint struct {
	Found   bool            `json:"found"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// CheckSignal reports the oldest pending signal of name WITHOUT consuming it
// (it stays queued for a later WaitForSignal/DrainSignals). Non-blocking;
// returns (zero, false, nil) when none is pending. The observation is
// checkpointed, so replays return the same result.
func CheckSignal[T any](ctx context.Context, name string) (T, bool, error) {
	var zero T
	jc, cs, ss, err := parts(ctx)
	if err != nil {
		return zero, false, err
	}
	if err := validateName(name); err != nil {
		return zero, false, err
	}
	ctype := "signalpeek:" + name

	idx, cp, has := nextCheckpoint(cs, ctype)
	if has {
		var pc peekCheckpoint
		if err := json.Unmarshal(cp.Result, &pc); err != nil {
			return zero, false, fmt.Errorf("signal: unmarshal peek checkpoint: %w", err)
		}
		if !pc.Found {
			return zero, false, nil
		}
		v, uerr := unmarshalInto[T](pc.Payload)
		return v, uerr == nil, uerr
	}

	sig, err := ss.PeekSignal(ctx, jc.Job.ID, name)
	if err != nil {
		return zero, false, fmt.Errorf("signal: peek: %w", err)
	}
	pc := peekCheckpoint{Found: sig != nil}
	if sig != nil {
		pc.Payload = sig.Payload
	}
	if err := writeCheckpointObj(ctx, jc, idx, ctype, pc); err != nil {
		return zero, false, err
	}
	if sig == nil {
		return zero, false, nil
	}
	v, uerr := unmarshalInto[T](sig.Payload)
	return v, uerr == nil, uerr
}

// DrainSignals consumes ALL currently-pending signals of name, in FIFO order,
// and returns their decoded values (empty if none). Non-blocking.
func DrainSignals[T any](ctx context.Context, name string) ([]T, error) {
	jc, cs, ss, err := parts(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	ctype := "signaldrain:" + name

	idx, cp, has := nextCheckpoint(cs, ctype)
	if has {
		var payloads []json.RawMessage
		if err := json.Unmarshal(cp.Result, &payloads); err != nil {
			return nil, fmt.Errorf("signal: unmarshal drain checkpoint: %w", err)
		}
		return decodeAll[T](payloads)
	}

	// Consume the whole batch and persist the single drain checkpoint atomically.
	// The closure ALWAYS runs (even for an empty batch) so an empty drain records
	// a deterministic checkpoint.
	sigs, err := ss.DrainSignalsTx(ctx, jc.Job.ID, name, func(batch []*core.Signal) (*core.Checkpoint, error) {
		payloads := make([]json.RawMessage, len(batch))
		for i, sg := range batch {
			payloads[i] = sg.Payload
		}
		data, e := json.Marshal(payloads)
		if e != nil {
			return nil, fmt.Errorf("signal: marshal drain checkpoint: %w", e)
		}
		return &core.Checkpoint{
			ID:        core.NewID(),
			JobID:     jc.Job.ID,
			CallIndex: idx,
			CallType:  ctype,
			Result:    data,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("signal: drain: %w", err)
	}
	payloads := make([]json.RawMessage, len(sigs))
	for i, sg := range sigs {
		payloads[i] = sg.Payload
	}
	return decodeAll[T](payloads)
}

func decodeAll[T any](payloads []json.RawMessage) ([]T, error) {
	out := make([]T, 0, len(payloads))
	for _, p := range payloads {
		v, err := unmarshalInto[T](p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func writeCheckpointObj(ctx context.Context, jc *intctx.JobContext, idx int, ctype string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("signal: marshal checkpoint: %w", err)
	}
	return writeCheckpoint(ctx, jc, idx, ctype, data)
}
