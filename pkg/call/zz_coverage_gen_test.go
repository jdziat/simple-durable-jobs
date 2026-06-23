package call

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// TestCallBestEffortReplayLogsWarning covers the jc.Logger != nil branch in the
// best-effort replay path: when a checkpoint at the same index has a different
// type and BestEffortReplay is enabled, a warning is logged before re-executing.
func TestCallBestEffortReplayLogsWarning(t *testing.T) {
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
	testHandler := func(ctx context.Context, args string) (string, error) {
		handlerCalled = true
		return "new-result", nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	// A real slog.Logger that discards output exercises the Logger.Warn branch
	// without touching stdout/stderr.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	checkpointSaved := false
	jobCtx := &intctx.JobContext{
		Job:              &core.Job{ID: "job-1"},
		BestEffortReplay: true,
		Logger:           logger,
		HandlerLookup: func(name string) (any, bool) {
			if name != "new-step" {
				return nil, false
			}
			return h, true
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			checkpointSaved = true
			return nil
		},
	}

	ctx := intctx.WithJobContext(baseCtx, jobCtx)
	ctx = intctx.WithCallState(ctx, checkpoints)

	result, err := Call[string](ctx, "new-step", "arg")
	if err != nil {
		t.Fatalf("expected no error in best-effort replay, got %v", err)
	}
	if result != "new-result" {
		t.Fatalf("expected freshly executed result, got %q", result)
	}
	if !handlerCalled {
		t.Fatal("expected handler to execute in best-effort mode")
	}
	if !checkpointSaved {
		t.Fatal("expected fresh checkpoint to be saved")
	}
}

// TestCallArgsMarshalError covers the json.Marshal(args) error branch in call.go.
// A channel value cannot be JSON-marshaled, so the failure is hit after a valid
// handler is looked up but before the handler executes.
func TestCallArgsMarshalError(t *testing.T) {
	baseCtx := context.Background()

	// Handler accepts `any`; it must never run because args marshaling fails first.
	testHandler := func(ctx context.Context, args any) (string, error) {
		t.Fatal("handler should not execute when args fail to marshal")
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
			t.Fatal("checkpoint should not be saved when args fail to marshal")
			return nil
		},
	}

	ctx := intctx.WithJobContext(baseCtx, jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	// A channel cannot be marshaled to JSON.
	result, err := Call[string](ctx, "marshal-fail", make(chan int))
	if err == nil {
		t.Fatal("expected args marshal error, got nil")
	}
	if !contains(err.Error(), "failed to marshal args") {
		t.Fatalf("expected args marshal error, got: %v", err)
	}
	if result != "" {
		t.Fatalf("expected zero value, got %q", result)
	}
}

// TestCallResultMarshalError covers the json.Marshal(result) error branch in
// call.go. The handler returns a channel, which ExecuteCall returns unchanged via
// its type-assertion fast path, but the subsequent result marshal in Call fails.
func TestCallResultMarshalError(t *testing.T) {
	baseCtx := context.Background()

	testHandler := func(ctx context.Context, args int) (chan int, error) {
		return make(chan int), nil
	}
	h, err := handler.NewHandler(testHandler)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	saveCalled := false
	jobCtx := &intctx.JobContext{
		Job: &core.Job{ID: "job-1"},
		HandlerLookup: func(name string) (any, bool) {
			return h, true
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			saveCalled = true
			return nil
		},
	}

	ctx := intctx.WithJobContext(baseCtx, jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	result, err := Call[chan int](ctx, "result-fail", 1)
	if err == nil {
		t.Fatal("expected result marshal error, got nil")
	}
	if !contains(err.Error(), "failed to marshal result") {
		t.Fatalf("expected result marshal error, got: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil channel result, got %v", result)
	}
	if saveCalled {
		t.Fatal("checkpoint must not be saved when result marshal fails")
	}
}

// TestCallSaveCheckpointError covers the SaveCheckpoint error branch in call.go.
func TestCallSaveCheckpointError(t *testing.T) {
	baseCtx := context.Background()

	testHandler := func(ctx context.Context, args string) (string, error) {
		return "ok", nil
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
			return errors.New("storage unavailable")
		},
	}

	ctx := intctx.WithJobContext(baseCtx, jobCtx)
	ctx = intctx.WithCallState(ctx, []core.Checkpoint{})

	result, err := Call[string](ctx, "save-fail", "arg")
	if err == nil {
		t.Fatal("expected save checkpoint error, got nil")
	}
	if !contains(err.Error(), "failed to save checkpoint") {
		t.Fatalf("expected save checkpoint error, got: %v", err)
	}
	if !contains(err.Error(), "storage unavailable") {
		t.Fatalf("expected wrapped storage error, got: %v", err)
	}
	if result != "" {
		t.Fatalf("expected zero value, got %q", result)
	}
}
