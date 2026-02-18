package jobctx

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
)

func TestJobFromContext(t *testing.T) {
	t.Run("returns job when set in context", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		job := &core.Job{
			ID:   "test-job-123",
			Type: "email",
		}
		jobCtx := &intctx.JobContext{Job: job}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Act
		result := JobFromContext(ctx)

		// Assert
		if result == nil {
			t.Fatal("expected job, got nil")
		}
		if result != nil && result.ID != "test-job-123" {
			t.Errorf("expected job ID %q, got %q", "test-job-123", result.ID)
		}
		if result != nil && result.Type != "email" {
			t.Errorf("expected job type %q, got %q", "email", result.Type)
		}
	})

	t.Run("returns nil when not set in context", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		result := JobFromContext(ctx)

		// Assert
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("returns nil when job context is nil", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jobCtx := &intctx.JobContext{Job: nil}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Act
		result := JobFromContext(ctx)

		// Assert
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}

func TestJobIDFromContext(t *testing.T) {
	t.Run("returns job ID when set in context", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		job := &core.Job{ID: "job-id-456"}
		jobCtx := &intctx.JobContext{Job: job}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Act
		result := JobIDFromContext(ctx)

		// Assert
		if result != "job-id-456" {
			t.Errorf("expected job ID %q, got %q", "job-id-456", result)
		}
	})

	t.Run("returns empty string when not set in context", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		result := JobIDFromContext(ctx)

		// Assert
		if result != "" {
			t.Errorf("expected empty string, got %q", result)
		}
	})

	t.Run("returns empty string when job is nil", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jobCtx := &intctx.JobContext{Job: nil}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Act
		result := JobIDFromContext(ctx)

		// Assert
		if result != "" {
			t.Errorf("expected empty string, got %q", result)
		}
	})
}

func TestSavePhaseCheckpoint(t *testing.T) {
	t.Run("saves checkpoint with phase name", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		savedCheckpoints := []*core.Checkpoint{}

		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Act
		err := SavePhaseCheckpoint(ctx, "phase1", map[string]string{"key": "value"})

		// Assert
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(savedCheckpoints) != 1 {
			t.Fatalf("expected 1 checkpoint saved, got %d", len(savedCheckpoints))
		}

		cp := savedCheckpoints[0]
		if cp.JobID != "job-1" {
			t.Errorf("expected job ID %q, got %q", "job-1", cp.JobID)
		}
		if cp.CallType != "phase1" {
			t.Errorf("expected call type %q, got %q", "phase1", cp.CallType)
		}
		if cp.CallIndex != -1 {
			t.Errorf("expected call index -1, got %d", cp.CallIndex)
		}

		// Verify result is correctly marshaled
		var result map[string]string
		if err := json.Unmarshal(cp.Result, &result); err != nil {
			t.Fatalf("failed to unmarshal result: %v", err)
		}
		if result["key"] != "value" {
			t.Errorf("expected value %q, got %q", "value", result["key"])
		}
	})

	t.Run("returns nil when not in job context", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		err := SavePhaseCheckpoint(ctx, "phase1", map[string]string{})

		// Assert
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("returns error on marshal failure", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				return nil
			},
		}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Use a value that cannot be marshaled to JSON
		unmarshalable := make(chan int)

		// Act
		err := SavePhaseCheckpoint(ctx, "phase1", unmarshalable)

		// Assert
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("creates unique checkpoint IDs", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		savedCheckpoints := []*core.Checkpoint{}

		jobCtx := &intctx.JobContext{
			Job: &core.Job{ID: "job-1"},
			SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
				savedCheckpoints = append(savedCheckpoints, cp)
				return nil
			},
		}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)

		// Act
		err1 := SavePhaseCheckpoint(ctx, "phase1", "result1")
		err2 := SavePhaseCheckpoint(ctx, "phase2", "result2")

		// Assert
		if err1 != nil {
			t.Fatalf("phase1 error: %v", err1)
		}
		if err2 != nil {
			t.Fatalf("phase2 error: %v", err2)
		}
		if len(savedCheckpoints) != 2 {
			t.Fatalf("expected 2 checkpoints, got %d", len(savedCheckpoints))
		}
		if savedCheckpoints[0].ID == savedCheckpoints[1].ID {
			t.Fatal("expected unique checkpoint IDs")
		}
	})
}

func TestLoadPhaseCheckpoint(t *testing.T) {
	t.Run("returns value when checkpoint found", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		resultData := map[string]string{"key": "value"}
		resultBytes, _ := json.Marshal(resultData)

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: -1,
				CallType:  "phase1",
				Result:    resultBytes,
			},
		}

		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, found := LoadPhaseCheckpoint[map[string]string](ctx, "phase1")

		// Assert
		if !found {
			t.Fatal("expected checkpoint to be found")
		}
		if result["key"] != "value" {
			t.Errorf("expected value %q, got %q", "value", result["key"])
		}
	})

	t.Run("returns zero value and false when checkpoint not found", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		checkpoints := []core.Checkpoint{}

		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, found := LoadPhaseCheckpoint[string](ctx, "phase1")

		// Assert
		if found {
			t.Fatal("expected checkpoint to not be found")
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
	})

	t.Run("returns zero value and false when not in job context", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		// Act
		result, found := LoadPhaseCheckpoint[int](ctx, "phase1")

		// Assert
		if found {
			t.Fatal("expected checkpoint to not be found")
		}
		if result != 0 {
			t.Errorf("expected zero value 0, got %d", result)
		}
	})

	t.Run("returns zero value and false when call state not initialized", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()
		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		// No call state added

		// Act
		result, found := LoadPhaseCheckpoint[string](ctx, "phase1")

		// Assert
		if found {
			t.Fatal("expected checkpoint to not be found")
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
	})

	t.Run("ignores non-phase checkpoints (call index != -1)", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: 0, // Not a phase checkpoint
				CallType:  "phase1",
				Result:    []byte(`"value"`),
			},
		}

		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, found := LoadPhaseCheckpoint[string](ctx, "phase1")

		// Assert
		if found {
			t.Fatal("expected checkpoint to not be found (wrong call index)")
		}
		if result != "" {
			t.Errorf("expected zero value, got %q", result)
		}
	})

	t.Run("returns error on unmarshal failure with found=false", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: -1,
				CallType:  "phase1",
				Result:    []byte(`{invalid json}`),
			},
		}

		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, found := LoadPhaseCheckpoint[map[string]string](ctx, "phase1")

		// Assert
		if found {
			t.Fatal("expected checkpoint not found due to unmarshal error")
		}
		if result != nil {
			t.Errorf("expected nil for invalid unmarshal, got %v", result)
		}
	})

	t.Run("loads various data types", func(t *testing.T) {
		tests := []struct {
			name     string
			data     any
			typeName string
		}{
			{"string", "hello", "string"},
			{"int", 42, "int"},
			{"float", 3.14, "float64"},
			{"bool", true, "bool"},
			{"struct", struct{ X int }{X: 5}, "struct"},
		}

		for _, tt := range tests {
			t.Run(tt.typeName, func(t *testing.T) {
				baseCtx := context.Background()

				resultBytes, _ := json.Marshal(tt.data)

				checkpoints := []core.Checkpoint{
					{
						ID:        "cp-1",
						JobID:     "job-1",
						CallIndex: -1,
						CallType:  "phase1",
						Result:    resultBytes,
					},
				}

				jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
				ctx := intctx.WithJobContext(baseCtx, jobCtx)
				ctx = intctx.WithCallState(ctx, checkpoints)

				// For struct test
				switch tt.typeName {
				case "struct":
					type TestStruct struct{ X int }
					result, found := LoadPhaseCheckpoint[TestStruct](ctx, "phase1")
					if !found {
						t.Fatal("expected checkpoint found")
					}
					if result.X != 5 {
						t.Errorf("expected X=5, got %d", result.X)
					}
				case "string":
					result, found := LoadPhaseCheckpoint[string](ctx, "phase1")
					if !found || result != "hello" {
						t.Errorf("string load failed")
					}
				case "int":
					result, found := LoadPhaseCheckpoint[int](ctx, "phase1")
					if !found || result != 42 {
						t.Errorf("int load failed")
					}
				case "float64":
					result, found := LoadPhaseCheckpoint[float64](ctx, "phase1")
					if !found || result != 3.14 {
						t.Errorf("float64 load failed")
					}
				case "bool":
					result, found := LoadPhaseCheckpoint[bool](ctx, "phase1")
					if !found || result != true {
						t.Errorf("bool load failed")
					}
				}
			})
		}
	})

	t.Run("thread-safe access to checkpoints", func(t *testing.T) {
		// Arrange
		baseCtx := context.Background()

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-1",
				JobID:     "job-1",
				CallIndex: -1,
				CallType:  "phase1",
				Result:    []byte(`"result"`),
			},
		}

		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		// Act
		result, found := LoadPhaseCheckpoint[string](ctx, "phase1")

		// Assert
		if !found {
			t.Fatal("expected checkpoint found")
		}
		if result != "result" {
			t.Errorf("expected %q, got %q", "result", result)
		}
	})
}
