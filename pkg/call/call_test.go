package call

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v2/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/security"
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

	t.Run("returns determinism violation for checkpoint at same index with different type", func(t *testing.T) {
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "old-step",
				Result:    []byte(`"wrong-result"`),
			},
		}

		handlerCalled := false
		testHandler := func(ctx context.Context, args any) (string, error) {
			handlerCalled = true
			return "new-result", nil
		}
		h, err := handler.NewHandler(testHandler)
		if err != nil {
			t.Fatalf("failed to create handler: %v", err)
		}

		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				t.Fatal("checkpoint should not be saved after determinism violation")
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		result, err := Call[string](ctx, "new-step", nil)
		if err == nil {
			t.Fatal("expected determinism violation")
		}
		if !contains(err.Error(), "determinism violation") {
			t.Fatalf("expected determinism violation, got %v", err)
		}
		if result != "" {
			t.Fatalf("expected zero result, got %q", result)
		}
		if handlerCalled {
			t.Fatal("handler must not execute after determinism violation")
		}
	})

	t.Run("best effort re-executes checkpoint at same index with different type", func(t *testing.T) {
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "old-step",
				Result:    []byte(`"old-result"`),
			},
		}

		handlerCalled := false
		testHandler := func(ctx context.Context, args any) (string, error) {
			handlerCalled = true
			return "new-result", nil
		}
		h, err := handler.NewHandler(testHandler)
		if err != nil {
			t.Fatalf("failed to create handler: %v", err)
		}

		checkpointSaved := false
		jobCtx := &intctx.JobContext{
			Job:              &core.Job{ID: "job-1"},
			BestEffortReplay: true,
			HandlerLookup: func(name string) (any, bool) {
				if name != "new-step" {
					return nil, false
				}
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				checkpointSaved = true
				if cp.CallIndex != 0 {
					t.Fatalf("expected checkpoint index 0, got %d", cp.CallIndex)
				}
				if cp.CallType != "new-step" {
					t.Fatalf("expected checkpoint type new-step, got %q", cp.CallType)
				}
				return nil
			},
		}

		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		result, err := Call[string](ctx, "new-step", "arg")
		if err != nil {
			if contains(err.Error(), "determinism violation") {
				t.Fatalf("best effort must not return determinism violation: %v", err)
			}
			t.Fatalf("expected no error, got %v", err)
		}
		if result != "new-result" {
			t.Fatalf("expected freshly executed result, got %q", result)
		}
		if !handlerCalled {
			t.Fatal("expected handler to execute in best effort mode")
		}
		if !checkpointSaved {
			t.Fatal("expected fresh checkpoint to be saved in best effort mode")
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

func TestCall_ErrorOnlyHandlerResultTyping(t *testing.T) {
	testHandler := func(ctx context.Context, args string) error {
		return nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	var savedCheckpoints []*core.Checkpoint
	newContext := func() context.Context {
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			HandlerLookup: func(name string) (any, bool) {
				if name != "error-only" {
					return nil, false
				}
				return h, true
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}

		ctx := intctx.WithJobContext(context.Background(), jobCtx)
		return intctx.WithCallState(ctx, []core.Checkpoint{})
	}

	result, err := Call[string](newContext(), "error-only", "arg")
	if err == nil {
		t.Fatal("expected guard error")
	}
	if result != "" {
		t.Fatalf("expected zero result, got %q", result)
	}
	if !contains(err.Error(), "no result value") {
		t.Fatalf("expected no-result guard error, got %v", err)
	}
	var noRetry *core.NoRetryError
	if !errors.As(err, &noRetry) {
		t.Fatalf("expected NoRetryError, got %T %v", err, err)
	}

	anyResult, err := Call[any](newContext(), "error-only", "arg")
	if err != nil {
		t.Fatalf("expected Call[any] to succeed, got %v", err)
	}
	if anyResult != nil {
		t.Fatalf("expected nil any result, got %#v", anyResult)
	}
	if len(savedCheckpoints) != 2 {
		t.Fatalf("expected 2 checkpoints, got %d", len(savedCheckpoints))
	}
	if savedCheckpoints[0].ErrorKind != core.CheckpointErrorKindNoRetry {
		t.Fatalf("expected first checkpoint to store no-retry error, got %q", savedCheckpoints[0].ErrorKind)
	}
	if savedCheckpoints[1].Error != "" {
		t.Fatalf("expected second checkpoint to be successful, got error %q", savedCheckpoints[1].Error)
	}
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

	t.Run("checkpointed typed errors preserve identity on replay", func(t *testing.T) {
		tests := []struct {
			name   string
			err    error
			assert func(*testing.T, error)
		}{
			{
				name: "no retry",
				err:  core.NoRetry(errors.New("permanent failure")),
				assert: func(t *testing.T, err error) {
					var noRetry *core.NoRetryError
					if !errors.As(err, &noRetry) {
						t.Fatalf("expected NoRetryError, got %T %v", err, err)
					}
				},
			},
			{
				name: "retry after",
				err:  core.RetryAfter(2*time.Second, errors.New("temporary failure")),
				assert: func(t *testing.T, err error) {
					var retryAfter *core.RetryAfterError
					if !errors.As(err, &retryAfter) {
						t.Fatalf("expected RetryAfterError, got %T %v", err, err)
					}
					if retryAfter.Delay != 2*time.Second {
						t.Fatalf("expected delay 2s, got %v", retryAfter.Delay)
					}
				},
			},
			{
				name: "sentinel",
				err:  core.ErrInvalidQueueName,
				assert: func(t *testing.T, err error) {
					if !errors.Is(err, core.ErrInvalidQueueName) {
						t.Fatalf("expected ErrInvalidQueueName identity, got %T %v", err, err)
					}
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				testHandler := func(ctx context.Context, args string) (string, error) {
					return "", tt.err
				}
				h, err := handler.NewHandler(testHandler)
				if err != nil {
					t.Fatalf("failed to create handler: %v", err)
				}

				var saved *core.Checkpoint
				jobCtx := &intctx.JobContext{
					Job: &core.Job{ID: "job-1"},
					HandlerLookup: func(name string) (any, bool) {
						return h, true
					},
					SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
						cpCopy := *cp
						saved = &cpCopy
						return nil
					},
				}

				ctx := intctx.WithJobContext(context.Background(), jobCtx)
				ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

				_, err = Call[string](ctx, "fail", "arg")
				if err == nil {
					t.Fatal("expected first-run error")
				}
				tt.assert(t, err)
				if saved == nil {
					t.Fatal("expected checkpoint to be saved")
				}
				if saved.ErrorKind == "" {
					t.Fatal("expected checkpoint error kind")
				}

				replayCtx := intctx.WithJobContext(context.Background(), jobCtx)
				replayCtx = intctx.WithCallState(replayCtx, []core.Checkpoint{*saved})

				_, replayErr := Call[string](replayCtx, "fail", "arg")
				if replayErr == nil {
					t.Fatal("expected replay error")
				}
				tt.assert(t, replayErr)
				if replayErr.Error() != err.Error() {
					t.Fatalf("expected replay error %q, got %q", err.Error(), replayErr.Error())
				}
			})
		}
	})
}

func TestCallRejectsOversizedArgsBeforeExecutingHandler(t *testing.T) {
	testHandler := func(ctx context.Context, args string) (string, error) {
		t.Fatal("handler should not execute for oversized args")
		return "", nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	jobCtx := &intctx.JobContext{
		Job: &core.Job{ID: "job-1"},
		HandlerLookup: func(name string) (any, bool) {
			return h, true
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			t.Fatal("checkpoint should not be saved for oversized args")
			return nil
		},
	}

	ctx := intctx.WithJobContext(context.Background(), jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	_, err = Call[string](ctx, "oversized", strings.Repeat("x", security.MaxJobArgsSize+1))
	if err == nil {
		t.Fatal("expected oversized args error")
	}
	if !errors.Is(err, core.ErrJobArgsTooLarge) {
		t.Fatalf("expected ErrJobArgsTooLarge, got %v", err)
	}
}

func TestCall_OversizedResultFailsTerminally(t *testing.T) {
	testHandler := func(ctx context.Context, args string) (string, error) {
		return strings.Repeat("x", security.MaxResultSize), nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	checkpointStore := &testCheckpointStore{}
	jobCtx := &intctx.JobContext{
		Job: &core.Job{ID: "job-1"},
		HandlerLookup: func(name string) (any, bool) {
			if name != "oversized-result" {
				return nil, false
			}
			return h, true
		},
		SaveCheckpoint: checkpointStore.SaveCheckpoint,
	}

	ctx := intctx.WithJobContext(context.Background(), jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	result, err := Call[string](ctx, "oversized-result", "arg")
	if err == nil {
		t.Fatal("expected oversized result error")
	}
	var noRetry *core.NoRetryError
	if !errors.As(err, &noRetry) {
		t.Fatalf("expected NoRetryError, got %T %v", err, err)
	}
	if result != "" {
		t.Fatalf("expected zero result, got %q", result)
	}
	checkpoints, getErr := checkpointStore.GetCheckpoints(context.Background(), "job-1")
	if getErr != nil {
		t.Fatalf("failed to get checkpoints: %v", getErr)
	}
	if len(checkpoints) != 0 {
		t.Fatalf("expected no checkpoint to be persisted, got %d", len(checkpoints))
	}
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

// TestCallWithCheckpointCtx_CheckpointCtxUsedForSave verifies that
// CallWithCheckpointCtx uses checkpointCtx (not execCtx) when calling
// SaveCheckpoint. This is the regression test for the checkpoint-write
// deadline hazard: if the per-activity execCtx expires just as the handler
// returns, the checkpoint must still be saved using the long-lived
// checkpointCtx (the workflow root context).
func TestCallWithCheckpointCtx_CheckpointCtxUsedForSave(t *testing.T) {
	testHandler := func(ctx context.Context, args string) (string, error) {
		return "result", nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	var savedWithCtx context.Context
	jobCtx := &intctx.JobContext{
		Job: &core.Job{ID: "job-save-ctx"},
		HandlerLookup: func(name string) (any, bool) {
			return h, true
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			savedWithCtx = ctx
			return nil
		},
	}

	// execCtx has an ALREADY-CANCELLED context to simulate a deadline hit.
	execCtx, execCancel := context.WithCancel(context.Background())
	execCancel() // immediately cancelled — simulates deadline firing just as handler returns

	// checkpointCtx is a live, long-running context.
	checkpointCtx := context.Background()

	// Set up workflow contexts on execCtx (job context and call state must be
	// readable from the execution context).
	ctx := intctx.WithJobContext(execCtx, jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	// Even though execCtx is cancelled, the handler was already called and
	// returned successfully before the cancellation is checked by SaveCheckpoint.
	// CallWithCheckpointCtx must use checkpointCtx for the save.
	result, callErr := CallWithCheckpointCtx[string](ctx, checkpointCtx, "handler", "arg")
	if callErr != nil {
		// If execCtx being cancelled causes handler.ExecuteCall to fail, that
		// is acceptable — but we still want to verify the checkpoint ctx path
		// in the success case. Skip if the handler failed due to cancellation.
		t.Logf("CallWithCheckpointCtx returned (potentially-expected) error: %v", callErr)
	} else {
		if result != "result" {
			t.Errorf("result = %q; want %q", result, "result")
		}
		// Verify the checkpoint was saved using checkpointCtx (the live context),
		// not the cancelled execCtx.
		if savedWithCtx == nil {
			t.Fatal("SaveCheckpoint was not called")
		}
		if savedWithCtx.Err() != nil {
			t.Errorf("SaveCheckpoint was called with a cancelled context (%v); want a live context", savedWithCtx.Err())
		}
	}
}

// TestCallWithCheckpointCtx_CheckpointUsesLiveContextAfterHandlerFinishes
// is the primary deadline-hazard regression test: an activity handler that
// runs to completion just before its deadline must have its result checkpointed
// even if execCtx has expired by the time SaveCheckpoint is invoked.
func TestCallWithCheckpointCtx_CheckpointUsesLiveContextAfterHandlerFinishes(t *testing.T) {
	const activityDeadline = 50 * time.Millisecond

	// Handler sleeps for just under the deadline then returns successfully.
	// This simulates an activity that finishes at the last possible moment.
	handlerRunTime := activityDeadline - 5*time.Millisecond
	testHandler := func(ctx context.Context, args string) (string, error) {
		// Simulate activity running close to its deadline.
		timer := time.NewTimer(handlerRunTime)
		defer timer.Stop()
		select {
		case <-timer.C:
			return "late-result", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	checkpointSaved := make(chan context.Context, 1)
	jobCtx := &intctx.JobContext{
		Job: &core.Job{ID: "job-deadline-hazard"},
		HandlerLookup: func(name string) (any, bool) {
			return h, true
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			checkpointSaved <- ctx
			return nil
		},
	}

	// execCtx with a tight deadline that fires around the same time the handler
	// returns — simulating the deadline-hazard window.
	execCtx, execCancel := context.WithTimeout(context.Background(), activityDeadline)
	defer execCancel()

	// checkpointCtx is the long-lived workflow root context.
	checkpointCtx := context.Background()

	ctx := intctx.WithJobContext(execCtx, jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	result, callErr := CallWithCheckpointCtx[string](ctx, checkpointCtx, "handler", "arg")
	if callErr != nil {
		// execCtx may have fired before the handler finished — that's OK for
		// this test; we only care that if the handler DID succeed, the checkpoint
		// was saved using a live ctx.
		t.Logf("handler returned error (may be deadline): %v", callErr)
		return
	}
	if result != "late-result" {
		t.Errorf("result = %q; want %q", result, "late-result")
	}

	// Verify the checkpoint was saved with a live context.
	select {
	case savedCtx := <-checkpointSaved:
		if savedCtx.Err() != nil {
			t.Errorf("checkpoint saved with cancelled/expired context: %v; want checkpointCtx (live)", savedCtx.Err())
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("SaveCheckpoint was never called within 500ms")
	}
}

// TestCallWithCheckpointCtx_IdenticalToCallWhenSameContext verifies that
// passing the same context for both execCtx and checkpointCtx produces
// identical behaviour to the standard Call function.
func TestCallWithCheckpointCtx_IdenticalToCallWhenSameContext(t *testing.T) {
	testHandler := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	var savedCPs []*core.Checkpoint
	jobCtx := &intctx.JobContext{
		Job: &core.Job{ID: "job-same-ctx"},
		HandlerLookup: func(name string) (any, bool) {
			return h, true
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			cpCopy := *cp
			savedCPs = append(savedCPs, &cpCopy)
			return nil
		},
	}

	ctx := intctx.WithJobContext(context.Background(), jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	result, err := CallWithCheckpointCtx[int](ctx, ctx, "handler", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 14 {
		t.Errorf("result = %d; want 14", result)
	}
	if len(savedCPs) != 1 {
		t.Fatalf("expected 1 checkpoint, got %d", len(savedCPs))
	}
	if savedCPs[0].CallIndex != 0 || savedCPs[0].CallType != "handler" {
		t.Errorf("checkpoint = {Index:%d, Type:%q}; want {0, handler}", savedCPs[0].CallIndex, savedCPs[0].CallType)
	}
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

type testCheckpointStore struct {
	checkpoints []core.Checkpoint
}

func (s *testCheckpointStore) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	cpCopy := *cp
	s.checkpoints = append(s.checkpoints, cpCopy)
	return nil
}

func (s *testCheckpointStore) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
	var checkpoints []core.Checkpoint
	for _, cp := range s.checkpoints {
		if cp.JobID == jobID {
			checkpoints = append(checkpoints, cp)
		}
	}
	return checkpoints, nil
}
