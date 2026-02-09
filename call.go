package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
)

// callState tracks the current call index for replay.
type callState struct {
	mu          sync.Mutex
	callIndex   int
	checkpoints map[int]*Checkpoint
}

type callStateKey struct{}

func getCallState(ctx context.Context) *callState {
	if cs, ok := ctx.Value(callStateKey{}).(*callState); ok {
		return cs
	}
	return nil
}

func withCallState(ctx context.Context, checkpoints []Checkpoint) context.Context {
	cs := &callState{
		checkpoints: make(map[int]*Checkpoint),
	}
	for i := range checkpoints {
		cs.checkpoints[checkpoints[i].CallIndex] = &checkpoints[i]
	}
	return context.WithValue(ctx, callStateKey{}, cs)
}

// Call executes a durable nested job call.
// Results are checkpointed; on replay, cached results are returned without re-execution.
func Call[T any](ctx context.Context, name string, args any, opts ...Option) (T, error) {
	var zero T

	jc := getJobContext(ctx)
	if jc == nil {
		return zero, fmt.Errorf("jobs.Call must be used within a job handler")
	}

	cs := getCallState(ctx)
	if cs == nil {
		return zero, fmt.Errorf("jobs.Call: call state not initialized")
	}

	cs.mu.Lock()
	callIndex := cs.callIndex
	cs.callIndex++
	checkpoint, hasCheckpoint := cs.checkpoints[callIndex]
	cs.mu.Unlock()

	// If we have a checkpoint, return the cached result
	if hasCheckpoint {
		if checkpoint.Error != "" {
			return zero, fmt.Errorf("%s", checkpoint.Error)
		}
		var result T
		if err := json.Unmarshal(checkpoint.Result, &result); err != nil {
			return zero, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		return result, nil
	}

	// Execute the nested call
	jc.queue.mu.RLock()
	handler, ok := jc.queue.handlers[name]
	jc.queue.mu.RUnlock()

	if !ok {
		return zero, fmt.Errorf("no handler registered for %q", name)
	}

	result, err := executeCall[T](ctx, handler, args)

	// Save checkpoint
	cp := &Checkpoint{
		ID:        uuid.New().String(),
		JobID:     jc.job.ID,
		CallIndex: callIndex,
		CallType:  name,
	}

	if err != nil {
		cp.Error = err.Error()
	} else {
		resultBytes, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return zero, fmt.Errorf("failed to marshal result: %w", marshalErr)
		}
		cp.Result = resultBytes
	}

	if saveErr := jc.queue.storage.SaveCheckpoint(ctx, cp); saveErr != nil {
		return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
	}

	return result, err
}

func executeCall[T any](ctx context.Context, handler *jobHandler, args any) (T, error) {
	var zero T

	var callArgs []reflect.Value

	if handler.hasContext {
		callArgs = append(callArgs, reflect.ValueOf(ctx))
	}

	if handler.argsType != nil {
		argsVal := reflect.ValueOf(args)
		if argsVal.Type() != handler.argsType {
			argsBytes, err := json.Marshal(args)
			if err != nil {
				return zero, fmt.Errorf("failed to marshal args: %w", err)
			}
			argPtr := reflect.New(handler.argsType)
			if err := json.Unmarshal(argsBytes, argPtr.Interface()); err != nil {
				return zero, fmt.Errorf("failed to unmarshal args: %w", err)
			}
			argsVal = argPtr.Elem()
		}
		callArgs = append(callArgs, argsVal)
	}

	results := handler.fn.Call(callArgs)

	numOut := handler.fn.Type().NumOut()

	if numOut == 1 {
		if !results[0].IsNil() {
			return zero, results[0].Interface().(error)
		}
		return zero, nil
	}

	if numOut == 2 {
		if !results[1].IsNil() {
			return zero, results[1].Interface().(error)
		}
		if results[0].CanInterface() {
			if result, ok := results[0].Interface().(T); ok {
				return result, nil
			}
			resultBytes, _ := json.Marshal(results[0].Interface())
			var result T
			json.Unmarshal(resultBytes, &result)
			return result, nil
		}
	}

	return zero, nil
}
