package call

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/internal/handler"
)

func TestCallWithoutJobContext(t *testing.T) {
	t.Run("returns error when job context not set", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		result, err := Call[string](ctx, "test", nil)

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
		if !contains(err.Error(), "must be used within a job handler") {
			t.Errorf("expected specific error message, got: %v", err)
		}
	})

	t.Run("returns error when call state not initialized", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		// No call state

		// Act
		result, err := Call[string](ctx, "test", nil)

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
		if !contains(err.Error(), "call state not initialized") {
			t.Errorf("expected specific error message, got: %v", err)
		}
	})
}

func TestCallWithCachedCheckpoint(t *testing.T) {
	t.Run("returns cached result without re-executing handler", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		expectedResult := "cached-result"
		resultBytes, _ := json.Marshal(expectedResult)

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "fetch",
				Result:    resultBytes,
			},
		}

		handlerCalled := false
		handlerLookup := func(name string) (any, bool) {
			handlerCalled = true
			return nil, false
		}

		jobCtx := &intctx.JobContext{
			Job:            &core.Job{ID: "job-1"},
			HandlerLookup:  handlerLookup,
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error { return nil },
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, err := Call[string](ctx, "fetch", nil)

		// Assert
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result != expectedResult {
			t.Errorf("expected %q, got %q", expectedResult, result)
		}
		if handlerCalled {
			t.Fatal("expected handler not to be called for cached result")
		}
	})

	t.Run("returns cached error without re-executing handler", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "fetch",
				Error:     "cached error message",
			},
		}

		handlerCalled := false
		handlerLookup := func(name string) (any, bool) {
			handlerCalled = true
			return nil, false
		}

		jobCtx := &intctx.JobContext{
			Job:            &core.Job{ID: "job-1"},
			HandlerLookup:  handlerLookup,
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error { return nil },
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, err := Call[string](ctx, "fetch", nil)

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "cached error message" {
			t.Errorf("expected error %q, got %q", "cached error message", err.Error())
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
		if handlerCalled {
			t.Fatal("expected handler not to be called for cached error")
		}
	})

	t.Run("returns zero value and false on checkpoint unmarshal error", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "fetch",
				Result:    []byte(`{invalid json}`),
			},
		}

		jobCtx := &intctx.JobContext{
			Job:            &core.Job{ID: "job-1"},
			HandlerLookup:  func(name string) (any, bool) { return nil, false },
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error { return nil },
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, err := Call[string](ctx, "fetch", nil)

		// Assert
		if err == nil {
			t.Fatal("expected unmarshal error")
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
	})
}

func TestCallWithMissingHandler(t *testing.T) {
	t.Run("returns error when handler not registered", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return nil, false // Handler not found
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error { return nil },
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act
		result, err := Call[string](ctx, "unknown-handler", nil)

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !contains(err.Error(), "no handler registered") {
			t.Errorf("expected no handler error, got: %v", err)
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
	})

	t.Run("returns error when handler has invalid type", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return "not-a-handler", true // Wrong type
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error { return nil },
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act
		result, err := Call[string](ctx, "bad-handler", nil)

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !contains(err.Error(), "invalid handler type") {
			t.Errorf("expected invalid handler type error, got: %v", err)
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
	})
}

func TestCallExecutesHandlerAndSavesCheckpoint(t *testing.T) {
	t.Run("executes handler and saves successful checkpoint", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		// Create a simple handler that returns a string
		testHandler := func(ctx context.Context, args int) (string, error) {
			return "result-" + string(rune(args)), nil
		}

		h, err := handler.NewHandler(testHandler)
		if err != nil {
			t.Fatalf("failed to create handler: %v", err)
		}

		savedCheckpoints := []*core.Checkpoint{}
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				if name == "test" {
					return h, true
				}
				return nil, false
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act
		result, err := Call[string](ctx, "test", 65)

		// Assert
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result != "result-A" {
			t.Errorf("expected %q, got %q", "result-A", result)
		}

		if len(savedCheckpoints) != 1 {
			t.Fatalf("expected 1 checkpoint saved, got %d", len(savedCheckpoints))
		}

		cp := savedCheckpoints[0]
		if cp.JobID != "job-1" {
			t.Errorf("expected job ID %q, got %q", "job-1", cp.JobID)
		}
		if cp.CallType != "test" {
			t.Errorf("expected call type %q, got %q", "test", cp.CallType)
		}
		if cp.CallIndex != 0 {
			t.Errorf("expected call index 0, got %d", cp.CallIndex)
		}
		if cp.Error != "" {
			t.Errorf("expected no error in checkpoint, got %q", cp.Error)
		}

		var savedResult string
		if err := json.Unmarshal(cp.Result, &savedResult); err != nil {
			t.Fatalf("failed to unmarshal checkpoint result: %v", err)
		}
		if savedResult != "result-A" {
			t.Errorf("expected saved result %q, got %q", "result-A", savedResult)
		}
	})

	t.Run("increments call index for multiple calls", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		testHandler := func(ctx context.Context, args string) (string, error) {
			return "result", nil
		}

		h, _ := handler.NewHandler(testHandler)

		savedCheckpoints := []*core.Checkpoint{}
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act - Make multiple calls
		_, err1 := Call[string](ctx, "handler1", "arg1")
		_, err2 := Call[string](ctx, "handler2", "arg2")
		_, err3 := Call[string](ctx, "handler3", "arg3")

		// Assert
		if err1 != nil {
			t.Fatalf("handler1 error: %v", err1)
		}
		if err2 != nil {
			t.Fatalf("handler2 error: %v", err2)
		}
		if err3 != nil {
			t.Fatalf("handler3 error: %v", err3)
		}
		if len(savedCheckpoints) != 3 {
			t.Fatalf("expected 3 checkpoints, got %d", len(savedCheckpoints))
		}
		if savedCheckpoints[0].CallIndex != 0 {
			t.Errorf("expected call index 0, got %d", savedCheckpoints[0].CallIndex)
		}
		if savedCheckpoints[1].CallIndex != 1 {
			t.Errorf("expected call index 1, got %d", savedCheckpoints[1].CallIndex)
		}
		if savedCheckpoints[2].CallIndex != 2 {
			t.Errorf("expected call index 2, got %d", savedCheckpoints[2].CallIndex)
		}
	})
}

func TestCallPropagatesHandlerErrors(t *testing.T) {
	t.Run("propagates handler error", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		testHandler := func(ctx context.Context, args string) (string, error) {
			return "", errors.New("handler failed")
		}

		h, _ := handler.NewHandler(testHandler)

		savedCheckpoints := []*core.Checkpoint{}
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act
		result, err := Call[string](ctx, "fail", "arg")

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !contains(err.Error(), "handler failed") {
			t.Errorf("expected handler error, got: %v", err)
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}

		// Verify error was saved in checkpoint
		if len(savedCheckpoints) != 1 {
			t.Fatalf("expected 1 checkpoint, got %d", len(savedCheckpoints))
		}
		if savedCheckpoints[0].Error != "handler failed" {
			t.Errorf("expected error in checkpoint, got %q", savedCheckpoints[0].Error)
		}
	})

	t.Run("error returned from handler is propagated immediately", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		testHandler := func(ctx context.Context, args int) (int, error) {
			if args < 0 {
				return 0, errors.New("negative input")
			}
			return args * 2, nil
		}

		h, _ := handler.NewHandler(testHandler)

		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act
		result, err := Call[int](ctx, "process", -5)

		// Assert
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "negative input" {
			t.Errorf("expected error %q, got %q", "negative input", err.Error())
		}
		if result != 0 {
			t.Errorf("expected zero value, got %d", result)
		}
	})
}

func TestCallIntegration(t *testing.T) {
	t.Run("full flow: multiple calls with mix of cached and new", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		handler1 := func(ctx context.Context, args string) (string, error) {
			return "step1-" + args, nil
		}
		handler2 := func(ctx context.Context, args string) (string, error) {
			return "step2-" + args, nil
		}

		h1, _ := handler.NewHandler(handler1)
		h2, _ := handler.NewHandler(handler2)

		// Simulate previous run with step 1 already completed
		previousCheckpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "step1",
				Result:    []byte(`"step1-input"`),
			},
		}

		savedCheckpoints := []*core.Checkpoint{}
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				switch name {
				case "step1":
					return h1, true
				case "step2":
					return h2, true
				}
				return nil, false
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, previousCheckpoints)

		// Act
		// First call should use cached result
		result1, err1 := Call[string](ctx, "step1", "input")

		// Second call should execute handler
		result2, err2 := Call[string](ctx, "step2", "data")

		// Assert
		if err1 != nil {
			t.Fatalf("step1 error: %v", err1)
		}
		if result1 != "step1-input" {
			t.Errorf("step1 result: expected %q, got %q", "step1-input", result1)
		}

		if err2 != nil {
			t.Fatalf("step2 error: %v", err2)
		}
		if result2 != "step2-data" {
			t.Errorf("step2 result: expected %q, got %q", "step2-data", result2)
		}

		// Only step2 should be saved
		if len(savedCheckpoints) != 1 {
			t.Fatalf("expected 1 new checkpoint, got %d", len(savedCheckpoints))
		}
		if savedCheckpoints[0].CallIndex != 1 {
			t.Errorf("expected call index 1, got %d", savedCheckpoints[0].CallIndex)
		}
	})

	t.Run("call index increments properly with checkpoints", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		testHandler := func(ctx context.Context, args int) (int, error) {
			return args + 1, nil
		}

		h, _ := handler.NewHandler(testHandler)

		savedCheckpoints := []*core.Checkpoint{}
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

		// Act - Make sequential calls
		for i := 0; i < 5; i++ {
			_, err := Call[int](ctx, "increment", i)
			if err != nil {
				t.Fatalf("call %d error: %v", i, err)
			}
		}

		// Assert
		if len(savedCheckpoints) != 5 {
			t.Fatalf("expected 5 checkpoints, got %d", len(savedCheckpoints))
		}

		for i := 0; i < 5; i++ {
			if savedCheckpoints[i].CallIndex != i {
				t.Errorf("checkpoint %d: expected call index %d, got %d", i, i, savedCheckpoints[i].CallIndex)
			}
		}
	})
}

// Helper function
func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || hasSubstring(s, substr)))
}

func hasSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
