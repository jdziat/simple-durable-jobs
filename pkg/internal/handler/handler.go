// Package handler provides reflection-based handler execution for the jobs package.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// Handler holds metadata about a registered job handler.
type Handler struct {
	Fn         reflect.Value
	ArgsType   reflect.Type
	HasContext bool
}

// NewHandler creates a Handler from a function.
// The function must have signature: func(ctx context.Context, args T) error
// or func(ctx context.Context, args T) (T, error)
func NewHandler(fn any) (*Handler, error) {
	if fn == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}

	fnVal := reflect.ValueOf(fn)

	// Check for typed nil (e.g., var fn func() = nil)
	if !fnVal.IsValid() || (fnVal.Kind() == reflect.Func && fnVal.IsNil()) {
		return nil, fmt.Errorf("handler function cannot be nil")
	}

	fnType := fnVal.Type()

	if fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("handler must be a function")
	}

	handler := &Handler{Fn: fnVal}

	// Parse function signature
	numIn := fnType.NumIn()
	if numIn < 1 || numIn > 2 {
		return nil, fmt.Errorf("handler must have 1-2 arguments")
	}

	argIdx := 0
	if fnType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		handler.HasContext = true
		argIdx = 1
	}

	if argIdx < numIn {
		handler.ArgsType = fnType.In(argIdx)
	}

	// Validate return type - allow error or (T, error)
	numOut := fnType.NumOut()
	switch numOut {
	case 1:
		if !fnType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			return nil, fmt.Errorf("handler must return error")
		}
	case 2:
		if !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			return nil, fmt.Errorf("handler must return (T, error)")
		}
	default:
		return nil, fmt.Errorf("handler must return error or (T, error)")
	}

	return handler, nil
}

// Execute runs the handler with the given context and arguments.
func (h *Handler) Execute(ctx context.Context, argsJSON []byte) error {
	// Defensive check: ensure handler function is valid
	if !h.Fn.IsValid() || h.Fn.IsNil() {
		return fmt.Errorf("handler function is nil or invalid")
	}

	var args []reflect.Value

	if h.HasContext {
		args = append(args, reflect.ValueOf(ctx))
	}

	if h.ArgsType != nil {
		argVal := reflect.New(h.ArgsType)
		if err := json.Unmarshal(argsJSON, argVal.Interface()); err != nil {
			return fmt.Errorf("failed to unmarshal args: %w", err)
		}
		args = append(args, argVal.Elem())
	}

	results := h.Fn.Call(args)

	// Handle return values
	numOut := h.Fn.Type().NumOut()
	switch numOut {
	case 1:
		if !results[0].IsNil() {
			return results[0].Interface().(error)
		}
	case 2:
		if !results[1].IsNil() {
			return results[1].Interface().(error)
		}
	}
	return nil
}

// ExecuteCall runs the handler for a nested Call, returning the result.
func ExecuteCall[T any](ctx context.Context, h *Handler, args any) (T, error) {
	var zero T

	// Defensive check: ensure handler function is valid
	if !h.Fn.IsValid() || h.Fn.IsNil() {
		return zero, fmt.Errorf("handler function is nil or invalid")
	}

	var callArgs []reflect.Value

	if h.HasContext {
		callArgs = append(callArgs, reflect.ValueOf(ctx))
	}

	if h.ArgsType != nil {
		argsVal := reflect.ValueOf(args)
		if argsVal.Type() != h.ArgsType {
			argsBytes, err := json.Marshal(args)
			if err != nil {
				return zero, fmt.Errorf("failed to marshal args: %w", err)
			}
			argPtr := reflect.New(h.ArgsType)
			if err := json.Unmarshal(argsBytes, argPtr.Interface()); err != nil {
				return zero, fmt.Errorf("failed to unmarshal args: %w", err)
			}
			argsVal = argPtr.Elem()
		}
		callArgs = append(callArgs, argsVal)
	}

	results := h.Fn.Call(callArgs)

	numOut := h.Fn.Type().NumOut()

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
			if err := json.Unmarshal(resultBytes, &result); err != nil {
				return zero, err
			}
			return result, nil
		}
	}

	return zero, nil
}
