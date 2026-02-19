package handler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helper types used across multiple tests
// ---------------------------------------------------------------------------

type testArgs struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type testResult struct {
	Output string `json:"output"`
	Count  int    `json:"count"`
}

// ---------------------------------------------------------------------------
// NewHandler – nil / non-function rejection
// ---------------------------------------------------------------------------

func TestNewHandler_RejectsNil(t *testing.T) {
	_, err := NewHandler(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestNewHandler_RejectsTypedNil(t *testing.T) {
	var fn func(ctx context.Context, args string) error = nil
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestNewHandler_RejectsString(t *testing.T) {
	_, err := NewHandler("not a function")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "function")
}

func TestNewHandler_RejectsInt(t *testing.T) {
	_, err := NewHandler(42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "function")
}

func TestNewHandler_RejectsStruct(t *testing.T) {
	_, err := NewHandler(testArgs{Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "function")
}

// ---------------------------------------------------------------------------
// NewHandler – argument count validation
// ---------------------------------------------------------------------------

func TestNewHandler_RejectsZeroArgs(t *testing.T) {
	fn := func() error { return nil }
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1-2 arguments")
}

func TestNewHandler_RejectsThreeArgs(t *testing.T) {
	fn := func(_ context.Context, _ string, _ int) error { return nil }
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1-2 arguments")
}

// ---------------------------------------------------------------------------
// NewHandler – return type validation
// ---------------------------------------------------------------------------

func TestNewHandler_RejectsNoReturnValues(t *testing.T) {
	fn := func(_ context.Context, _ string) {}
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "return")
}

func TestNewHandler_RejectsSingleNonErrorReturn(t *testing.T) {
	fn := func(_ context.Context, _ string) string { return "" }
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "return")
}

func TestNewHandler_RejectsTwoReturnsSecondNotError(t *testing.T) {
	// (string, string) – second return is not error
	fn := func(_ context.Context, _ string) (string, string) { return "", "" }
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "return")
}

func TestNewHandler_RejectsThreeReturnValues(t *testing.T) {
	fn := func(_ context.Context, _ string) (string, string, error) { return "", "", nil }
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "return")
}

// ---------------------------------------------------------------------------
// NewHandler – valid signatures
// ---------------------------------------------------------------------------

func TestNewHandler_AcceptsValidFunction(t *testing.T) {
	fn := func(ctx context.Context, args string) error {
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.True(t, h.Fn.IsValid())
	assert.False(t, h.Fn.IsNil())
}

func TestNewHandler_AcceptsSingleContextArg(t *testing.T) {
	// One argument that is context.Context
	fn := func(_ context.Context) error { return nil }
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.True(t, h.HasContext)
	assert.Nil(t, h.ArgsType)
}

func TestNewHandler_AcceptsSingleNonContextArg(t *testing.T) {
	// One argument that is NOT context.Context (plain args only, no ctx)
	fn := func(_ string) error { return nil }
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.False(t, h.HasContext)
	require.NotNil(t, h.ArgsType)
}

func TestNewHandler_AcceptsContextPlusStructArgs(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) error { return nil }
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.True(t, h.HasContext)
	require.NotNil(t, h.ArgsType)
}

func TestNewHandler_AcceptsResultAndError(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) (testResult, error) { return testResult{}, nil }
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.NotNil(t, h)
}

// ---------------------------------------------------------------------------
// Execute – invalid handler function (zero value)
// ---------------------------------------------------------------------------

func TestHandler_Execute_ReturnsErrorForInvalidFn(t *testing.T) {
	h := &Handler{}
	err := h.Execute(context.Background(), []byte("{}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil or invalid")
}

// ---------------------------------------------------------------------------
// Execute – JSON unmarshal failure
// ---------------------------------------------------------------------------

func TestHandler_Execute_BadJSON(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) error { return nil }
	h, err := NewHandler(fn)
	require.NoError(t, err)

	err = h.Execute(context.Background(), []byte("not json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

// ---------------------------------------------------------------------------
// Execute – handler with struct args, success path
// ---------------------------------------------------------------------------

func TestHandler_Execute_StructArgs_Success(t *testing.T) {
	var received testArgs
	fn := func(_ context.Context, args testArgs) error {
		received = args
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	argsJSON := []byte(`{"name":"hello","value":7}`)
	err = h.Execute(context.Background(), argsJSON)
	require.NoError(t, err)
	assert.Equal(t, "hello", received.Name)
	assert.Equal(t, 7, received.Value)
}

// ---------------------------------------------------------------------------
// Execute – handler with primitive (string) args, success path
// ---------------------------------------------------------------------------

func TestHandler_Execute_StringArgs_Success(t *testing.T) {
	var received string
	fn := func(_ context.Context, args string) error {
		received = args
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	err = h.Execute(context.Background(), []byte(`"world"`))
	require.NoError(t, err)
	assert.Equal(t, "world", received)
}

// ---------------------------------------------------------------------------
// Execute – handler propagates error from handler body
// ---------------------------------------------------------------------------

func TestHandler_Execute_HandlerReturnsError(t *testing.T) {
	sentinel := errors.New("job failed")
	fn := func(_ context.Context, _ testArgs) error { return sentinel }
	h, err := NewHandler(fn)
	require.NoError(t, err)

	err = h.Execute(context.Background(), []byte("{}"))
	require.Error(t, err)
	assert.Equal(t, sentinel, err)
}

// ---------------------------------------------------------------------------
// Execute – handler with (T, error) signature, success path (nil error)
// ---------------------------------------------------------------------------

func TestHandler_Execute_TwoReturnValues_Success(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) (testResult, error) {
		return testResult{Output: "ok", Count: 3}, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	err = h.Execute(context.Background(), []byte("{}"))
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Execute – handler with (T, error) signature, error propagated
// ---------------------------------------------------------------------------

func TestHandler_Execute_TwoReturnValues_Error(t *testing.T) {
	sentinel := errors.New("two-return error")
	fn := func(_ context.Context, _ testArgs) (testResult, error) {
		return testResult{}, sentinel
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	err = h.Execute(context.Background(), []byte("{}"))
	require.Error(t, err)
	assert.Equal(t, sentinel, err)
}

// ---------------------------------------------------------------------------
// Execute – handler that takes no args (context only), nil ArgsType
// ---------------------------------------------------------------------------

func TestHandler_Execute_NoArgsType(t *testing.T) {
	called := false
	fn := func(_ context.Context) error {
		called = true
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.Nil(t, h.ArgsType)

	err = h.Execute(context.Background(), []byte("{}"))
	require.NoError(t, err)
	assert.True(t, called)
}

// ---------------------------------------------------------------------------
// Execute – handler without context (args-only), success
// ---------------------------------------------------------------------------

func TestHandler_Execute_NoContext_Success(t *testing.T) {
	var received string
	fn := func(args string) error {
		received = args
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)
	assert.False(t, h.HasContext)

	err = h.Execute(context.Background(), []byte(`"direct"`))
	require.NoError(t, err)
	assert.Equal(t, "direct", received)
}

// ---------------------------------------------------------------------------
// ExecuteCall – invalid handler function (zero value)
// ---------------------------------------------------------------------------

func TestExecuteCall_ReturnsErrorForInvalidFn(t *testing.T) {
	h := &Handler{}
	_, err := ExecuteCall[string](context.Background(), h, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil or invalid")
}

// ---------------------------------------------------------------------------
// ExecuteCall – handler returning (string, error), exact type match
// ---------------------------------------------------------------------------

func TestExecuteCall_StringResult_ExactType(t *testing.T) {
	fn := func(_ context.Context, args string) (string, error) {
		return "result:" + args, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	result, err := ExecuteCall[string](context.Background(), h, "input")
	require.NoError(t, err)
	assert.Equal(t, "result:input", result)
}

// ---------------------------------------------------------------------------
// ExecuteCall – handler returning (struct, error), exact type match
// ---------------------------------------------------------------------------

func TestExecuteCall_StructResult_ExactType(t *testing.T) {
	fn := func(_ context.Context, args testArgs) (testResult, error) {
		return testResult{Output: args.Name, Count: args.Value * 2}, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	result, err := ExecuteCall[testResult](context.Background(), h, testArgs{Name: "foo", Value: 5})
	require.NoError(t, err)
	assert.Equal(t, "foo", result.Output)
	assert.Equal(t, 10, result.Count)
}

// ---------------------------------------------------------------------------
// ExecuteCall – type mismatch triggers JSON round-trip marshal path
// ---------------------------------------------------------------------------

func TestExecuteCall_StructResult_JSONRoundTrip(t *testing.T) {
	fn := func(_ context.Context, args testArgs) (testResult, error) {
		return testResult{Output: args.Name, Count: args.Value + 1}, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	// Pass a map instead of testArgs to force the JSON round-trip code path
	mapArgs := map[string]any{"name": "bar", "value": 9}
	result, err := ExecuteCall[testResult](context.Background(), h, mapArgs)
	require.NoError(t, err)
	assert.Equal(t, "bar", result.Output)
	assert.Equal(t, 10, result.Count)
}

// ---------------------------------------------------------------------------
// ExecuteCall – error propagated from handler body
// ---------------------------------------------------------------------------

func TestExecuteCall_HandlerReturnsError(t *testing.T) {
	sentinel := errors.New("call error")
	fn := func(_ context.Context, _ testArgs) (testResult, error) {
		return testResult{}, sentinel
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	_, err = ExecuteCall[testResult](context.Background(), h, testArgs{})
	require.Error(t, err)
	assert.Equal(t, sentinel, err)
}

// ---------------------------------------------------------------------------
// ExecuteCall – single-return (error only) handler, success path
// ---------------------------------------------------------------------------

func TestExecuteCall_SingleReturn_Success(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) error { return nil }
	h, err := NewHandler(fn)
	require.NoError(t, err)

	result, err := ExecuteCall[testResult](context.Background(), h, testArgs{})
	require.NoError(t, err)
	// zero value returned when handler has no result
	assert.Equal(t, testResult{}, result)
}

// ---------------------------------------------------------------------------
// ExecuteCall – single-return (error only) handler, error path
// ---------------------------------------------------------------------------

func TestExecuteCall_SingleReturn_Error(t *testing.T) {
	sentinel := errors.New("single return error")
	fn := func(_ context.Context, _ testArgs) error { return sentinel }
	h, err := NewHandler(fn)
	require.NoError(t, err)

	_, err = ExecuteCall[testResult](context.Background(), h, testArgs{})
	require.Error(t, err)
	assert.Equal(t, sentinel, err)
}

// ---------------------------------------------------------------------------
// ExecuteCall – marshal failure (args that cannot be JSON-encoded)
// ---------------------------------------------------------------------------

func TestExecuteCall_MarshalFailure(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) (testResult, error) {
		return testResult{}, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	// Channels cannot be marshalled to JSON
	_, err = ExecuteCall[testResult](context.Background(), h, make(chan int))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal")
}

// ---------------------------------------------------------------------------
// ExecuteCall – no args handler (context only), returns value
// ---------------------------------------------------------------------------

func TestExecuteCall_NoArgsHandler_ReturnsValue(t *testing.T) {
	fn := func(_ context.Context) (string, error) {
		return "constant", nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.Nil(t, h.ArgsType)

	result, err := ExecuteCall[string](context.Background(), h, nil)
	require.NoError(t, err)
	assert.Equal(t, "constant", result)
}

// ---------------------------------------------------------------------------
// ExecuteCall – JSON serialisation round-trip (result → JSON → typed value)
// via interface assertion fallback path
// ---------------------------------------------------------------------------

func TestExecuteCall_JSONFallback_StructToStruct(t *testing.T) {
	// Return a struct from a handler typed differently; ExecuteCall uses
	// the JSON fallback path when the direct type assertion fails.
	type innerResult struct {
		Output string `json:"output"`
		Count  int    `json:"count"`
	}

	fn := func(_ context.Context, _ testArgs) (innerResult, error) {
		return innerResult{Output: "fallback", Count: 99}, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	// Ask for testResult (same JSON shape as innerResult)
	result, err := ExecuteCall[testResult](context.Background(), h, testArgs{})
	require.NoError(t, err)
	assert.Equal(t, "fallback", result.Output)
	assert.Equal(t, 99, result.Count)
}

// ---------------------------------------------------------------------------
// ExecuteCall – context propagation verification
// ---------------------------------------------------------------------------

func TestExecuteCall_ContextPropagation(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")

	var gotCtx context.Context
	fn := func(c context.Context, args string) (string, error) {
		gotCtx = c
		return "ok", nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	_, err = ExecuteCall[string](ctx, h, "")
	require.NoError(t, err)
	assert.Equal(t, "marker", gotCtx.Value(ctxKey{}))
}

// ---------------------------------------------------------------------------
// Execute – context propagation verification
// ---------------------------------------------------------------------------

func TestHandler_Execute_ContextPropagation(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "exec-marker")

	var gotCtx context.Context
	fn := func(c context.Context, _ string) error {
		gotCtx = c
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	err = h.Execute(ctx, []byte(`"anything"`))
	require.NoError(t, err)
	assert.Equal(t, "exec-marker", gotCtx.Value(ctxKey{}))
}

// ---------------------------------------------------------------------------
// Execute – nil/empty JSON with pointer-compatible args (zero value path)
// ---------------------------------------------------------------------------

func TestHandler_Execute_EmptyJSONObject(t *testing.T) {
	var received testArgs
	fn := func(_ context.Context, args testArgs) error {
		received = args
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	// Empty JSON object → zero-value struct
	err = h.Execute(context.Background(), []byte("{}"))
	require.NoError(t, err)
	assert.Equal(t, testArgs{}, received)
}

// ---------------------------------------------------------------------------
// Error message quality checks
// ---------------------------------------------------------------------------

func TestNewHandler_ErrorMessages(t *testing.T) {
	cases := []struct {
		name      string
		fn        any
		wantInMsg string
	}{
		{"string value", "bad", "function"},
		{"int value", 0, "function"},
		{"zero args", func() error { return nil }, "1-2 arguments"},
		{"three args", func(_ context.Context, _ string, _ int) error { return nil }, "1-2 arguments"},
		{"no return", func(_ context.Context, _ string) {}, "return"},
		{"wrong single return", func(_ context.Context, _ string) string { return "" }, "return"},
		{"wrong second return", func(_ context.Context, _ string) (string, string) { return "", "" }, "return"},
		{"three returns", func(_ context.Context, _ string) (string, string, error) { return "", "", nil }, "return"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewHandler(tc.fn)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantInMsg,
				"expected error to contain %q, got: %v", tc.wantInMsg, err)
		})
	}
}

// ---------------------------------------------------------------------------
// ExecuteCall – args unmarshal failure (JSON marshal succeeds, unmarshal fails)
// ---------------------------------------------------------------------------

// unmarshalBomb is a type that marshals to a JSON number, which cannot be
// unmarshalled back into testArgs, triggering the unmarshal-args error path.
type unmarshalBomb int

func TestExecuteCall_ArgsUnmarshalFailure(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) (testResult, error) {
		return testResult{}, nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	// unmarshalBomb marshals to a JSON number (e.g. 42).
	// That number cannot be unmarshalled into testArgs (a struct), so the
	// json.Unmarshal inside the type-mismatch branch will return an error.
	_, err = ExecuteCall[testResult](context.Background(), h, unmarshalBomb(42))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

// ---------------------------------------------------------------------------
// ExecuteCall – result unmarshal failure (result type mismatches T)
// ---------------------------------------------------------------------------

// badJSONResult marshals to a JSON string, which cannot unmarshal into
// testResult (a struct), exercising the result JSON fallback error path.
type badJSONResult string

func TestExecuteCall_ResultUnmarshalFailure(t *testing.T) {
	// The handler returns badJSONResult (a string alias), but the caller
	// asks for testResult. The direct type assertion fails, the JSON
	// fallback marshals to a JSON string, and json.Unmarshal into
	// testResult fails because you cannot unmarshal a string into a struct.
	fn := func(_ context.Context, _ testArgs) (badJSONResult, error) {
		return badJSONResult("not-a-struct"), nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	_, err = ExecuteCall[testResult](context.Background(), h, testArgs{})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// ExecuteCall – defensive tail (numOut != 1 and != 2), returns zero, nil
// ---------------------------------------------------------------------------

func TestExecuteCall_DefensiveTail_ZeroReturn(t *testing.T) {
	// Construct a Handler directly, bypassing NewHandler validation, with a
	// function that has no return values so numOut == 0. This exercises
	// the final "return zero, nil" fallback in ExecuteCall.
	fn := func(_ context.Context, _ testArgs) {}
	fnVal := reflect.ValueOf(fn)
	h := &Handler{
		Fn:         fnVal,
		ArgsType:   reflect.TypeOf(testArgs{}),
		HasContext:  true,
	}

	result, err := ExecuteCall[testResult](context.Background(), h, testArgs{})
	require.NoError(t, err)
	assert.Equal(t, testResult{}, result)
}

// ---------------------------------------------------------------------------
// Fuzz-style: Execute with various JSON payloads
// ---------------------------------------------------------------------------

func TestHandler_Execute_VariousPayloads(t *testing.T) {
	fn := func(_ context.Context, args testArgs) error {
		_ = fmt.Sprintf("%+v", args)
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)

	payloads := []struct {
		name    string
		json    []byte
		wantErr bool
	}{
		{"full struct", []byte(`{"name":"a","value":1}`), false},
		{"partial struct", []byte(`{"name":"b"}`), false},
		{"empty object", []byte(`{}`), false},
		{"invalid json", []byte(`{bad}`), true},
		{"null", []byte(`null`), false},
		{"array", []byte(`[]`), true},
	}

	for _, p := range payloads {
		t.Run(p.name, func(t *testing.T) {
			err := h.Execute(context.Background(), p.json)
			if p.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
