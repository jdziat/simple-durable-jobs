package context

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

func TestWithJobContextAndGetJobContext(t *testing.T) {
	t.Run("stores and retrieves job context", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		job := &core.Job{
			ID:   "test-job-123",
			Type: "email",
		}
		jc := &JobContext{
			Job:      job,
			WorkerID: "worker-1",
		}

		// Act
		ctx := WithJobContext(baseCtx, jc)
		retrieved := GetJobContext(ctx)

		// Assert
		if retrieved == nil {
			t.Fatal("expected job context to be set, got nil")
		}
		if retrieved == nil || retrieved.Job == nil {
			t.Fatal("job context or job is nil")
		}
		if retrieved.Job.ID != job.ID {
			t.Errorf("expected job ID %q, got %q", job.ID, retrieved.Job.ID)
		}
		if retrieved.WorkerID != "worker-1" {
			t.Errorf("expected worker ID %q, got %q", "worker-1", retrieved.WorkerID)
		}
	})

	t.Run("returns nil when job context not set", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		jc := GetJobContext(ctx)

		// Assert
		if jc != nil {
			t.Errorf("expected nil, got %v", jc)
		}
	})

	t.Run("overwrites previous job context", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jc1 := &JobContext{
			Job:      &core.Job{ID: "job-1"},
			WorkerID: "worker-1",
		}
		jc2 := &JobContext{
			Job:      &core.Job{ID: "job-2"},
			WorkerID: "worker-2",
		}

		// Act
		ctx1 := WithJobContext(baseCtx, jc1)
		ctx2 := WithJobContext(ctx1, jc2)
		retrieved := GetJobContext(ctx2)

		// Assert
		if retrieved.Job.ID != "job-2" {
			t.Errorf("expected job ID %q, got %q", "job-2", retrieved.Job.ID)
		}
		if retrieved.WorkerID != "worker-2" {
			t.Errorf("expected worker ID %q, got %q", "worker-2", retrieved.WorkerID)
		}
	})

	t.Run("stores all job context fields", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		handlerCalled := false
		jc := &JobContext{
			Job: &core.Job{
				ID:   "job-1",
				Type: "process",
			},
			Storage:  nil, // Can be nil in tests
			WorkerID: "test-worker",
			HandlerLookup: func(name string) (any, bool) {
				handlerCalled = true
				return nil, false
			},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				return nil
			},
		}

		// Act
		ctx := WithJobContext(baseCtx, jc)
		retrieved := GetJobContext(ctx)

		// Assert
		if retrieved.HandlerLookup == nil {
			t.Fatal("expected HandlerLookup to be set")
		}
		if retrieved.SaveCheckpoint == nil {
			t.Fatal("expected SaveCheckpoint to be set")
		}
		retrieved.HandlerLookup("test")
		if !handlerCalled {
			t.Fatal("expected HandlerLookup to be callable")
		}
	})
}

func TestWithCallStateAndGetCallState(t *testing.T) {
	t.Run("stores and retrieves call state from checkpoints", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "fetch",
				Result:    []byte(`{"data": "value"}`),
			},
			{
				ID:        "cp-2",
				JobID:     "job-1",
				CallIndex: 1,
				CallType:  "process",
				Result:    []byte(`{"status": "ok"}`),
			},
		}

		// Act
		ctx := WithCallState(baseCtx, checkpoints)
		cs := GetCallState(ctx)

		// Assert
		if cs == nil || cs.Checkpoints == nil {
			t.Fatal("expected call state to be set, got nil")
		}
		if len(cs.Checkpoints) != 2 {
			t.Fatalf("expected 2 checkpoints, got %d", len(cs.Checkpoints))
		}
		if cp0 := cs.Checkpoints[0]; cp0 != nil && cp0.CallType != "fetch" {
			t.Errorf("expected call type %q, got %q", "fetch", cp0.CallType)
		}
		if cp1 := cs.Checkpoints[1]; cp1 != nil && cp1.CallType != "process" {
			t.Errorf("expected call type %q, got %q", "process", cp1.CallType)
		}
	})

	t.Run("returns nil when call state not set", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		cs := GetCallState(ctx)

		// Assert
		if cs != nil {
			t.Errorf("expected nil, got %v", cs)
		}
	})

	t.Run("initializes with call index zero", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0,
				CallType:  "call1",
			},
		}

		// Act
		ctx := WithCallState(baseCtx, checkpoints)
		cs := GetCallState(ctx)

		// Assert
		if cs.CallIndex != 0 {
			t.Errorf("expected call index 0, got %d", cs.CallIndex)
		}
	})

	t.Run("handles empty checkpoint list", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{}

		// Act
		ctx := WithCallState(baseCtx, checkpoints)
		cs := GetCallState(ctx)

		// Assert
		if cs == nil || cs.Checkpoints == nil {
			t.Fatal("expected call state and checkpoints map to be initialized")
		}
		if len(cs.Checkpoints) != 0 {
			t.Errorf("expected 0 checkpoints, got %d", len(cs.Checkpoints))
		}
		if cs.CallIndex != 0 {
			t.Errorf("expected call index 0, got %d", cs.CallIndex)
		}
	})

	t.Run("maps checkpoints by call index", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 5,
				CallType:  "nested",
			},
			{
				ID:        "cp-2",
				JobID:     "job-1",
				CallIndex: 10,
				CallType:  "other",
			},
		}

		// Act
		ctx := WithCallState(baseCtx, checkpoints)
		cs := GetCallState(ctx)

		// Assert
		if cp, ok := cs.Checkpoints[5]; !ok {
			t.Fatal("expected checkpoint at index 5")
		} else if cp.CallType != "nested" {
			t.Errorf("expected call type %q, got %q", "nested", cp.CallType)
		}
		if cp, ok := cs.Checkpoints[10]; !ok {
			t.Fatal("expected checkpoint at index 10")
		} else if cp.CallType != "other" {
			t.Errorf("expected call type %q, got %q", "other", cp.CallType)
		}
	})

	t.Run("call state has mutex for thread safety", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{}

		// Act
		ctx := WithCallState(baseCtx, checkpoints)
		cs := GetCallState(ctx)

		// Assert
		if cs == nil {
			t.Fatal("expected call state to be set")
		}
		// Lock and unlock to verify mutex works
		if cs != nil {
			cs.Mu.Lock()
			defer cs.Mu.Unlock()
		}
	})
}

func TestContextIsolation(t *testing.T) {
	t.Run("job context does not leak between contexts", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jc1 := &JobContext{Job: &core.Job{ID: "job-1"}}
		jc2 := &JobContext{Job: &core.Job{ID: "job-2"}}

		// Act
		ctx1 := WithJobContext(baseCtx, jc1)
		ctx2 := WithJobContext(baseCtx, jc2)

		// Assert
		if GetJobContext(ctx1).Job.ID != "job-1" {
			t.Error("ctx1 job context was modified")
		}
		if GetJobContext(ctx2).Job.ID != "job-2" {
			t.Error("ctx2 job context was modified")
		}
		if GetJobContext(baseCtx) != nil {
			t.Error("base context should not have job context")
		}
	})

	t.Run("call state does not leak between contexts", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints1 := []core.Checkpoint{
			{ID: "cp-1", CallIndex: 0, CallType: "call1"},
		}
		checkpoints2 := []core.Checkpoint{
			{ID: "cp-2", CallIndex: 0, CallType: "call2"},
		}

		// Act
		ctx1 := WithCallState(baseCtx, checkpoints1)
		ctx2 := WithCallState(baseCtx, checkpoints2)

		// Assert
		cs1 := GetCallState(ctx1)
		cs2 := GetCallState(ctx2)
		if cs1 != nil && cs1.Checkpoints != nil && cs1.Checkpoints[0] != nil {
			if cs1.Checkpoints[0].CallType != "call1" {
				t.Error("ctx1 call state was modified")
			}
		} else {
			t.Fatal("ctx1 call state not properly set")
		}
		if cs2 != nil && cs2.Checkpoints != nil && cs2.Checkpoints[0] != nil {
			if cs2.Checkpoints[0].CallType != "call2" {
				t.Error("ctx2 call state was modified")
			}
		} else {
			t.Fatal("ctx2 call state not properly set")
		}
		if GetCallState(baseCtx) != nil {
			t.Error("base context should not have call state")
		}
	})
}

func TestCallStatePointerReferences(t *testing.T) {
	t.Run("checkpoint pointers reference original checkpoint data", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoint := core.Checkpoint{
			ID:        "cp-1",
			JobID:     "job-1",
			CallIndex: 0,
			CallType:  "fetch",
			Result:    []byte(`{"test": true}`),
		}
		checkpoints := []core.Checkpoint{checkpoint}

		// Act
		ctx := WithCallState(baseCtx, checkpoints)
		cs := GetCallState(ctx)

		// Assert
		storedCP := cs.Checkpoints[0]
		if storedCP == nil {
			t.Fatal("expected checkpoint pointer")
		}
		if storedCP != nil && string(storedCP.Result) != `{"test": true}` {
			t.Errorf("unexpected result: %s", storedCP.Result)
		}
	})
}
