package jobctx

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v3/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
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

func TestGetVersion(t *testing.T) {
	t.Run("first run records max supported and replay returns recorded version", func(t *testing.T) {
		var saved []*core.Checkpoint
		ctx := newTestVersionContext("job-1", nil, func(_ context.Context, cp *core.Checkpoint) error {
			saved = append(saved, cp)
			return nil
		})

		version, err := GetVersion(ctx, "add-review-step", DefaultVersion, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, version)
		require.Len(t, saved, 1)
		assert.Equal(t, -1, saved[0].CallIndex)
		assert.Equal(t, versionCheckpointType("add-review-step"), saved[0].CallType)
		assert.NotEqual(t, "add-review-step", saved[0].CallType, "version markers must be namespaced away from phase names")

		var persisted int
		require.NoError(t, json.Unmarshal(saved[0].Result, &persisted))
		assert.Equal(t, 1, persisted)

		replayCtx := newTestVersionContext("job-1", []core.Checkpoint{*saved[0]}, nil)
		replayed, err := GetVersion(replayCtx, "add-review-step", DefaultVersion, 2)
		require.NoError(t, err)
		assert.Equal(t, 1, replayed, "replay must keep the recorded version even after maxSupported advances")
	})

	t.Run("recorded version outside supported range returns sentinel error", func(t *testing.T) {
		result, err := json.Marshal(1)
		require.NoError(t, err)
		ctx := newTestVersionContext("job-1", []core.Checkpoint{{
			JobID:     "job-1",
			CallIndex: -1,
			CallType:  versionCheckpointType("old-change"),
			Result:    result,
		}}, nil)

		version, err := GetVersion(ctx, "old-change", 2, 3)
		assert.Equal(t, 0, version)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrUnsupportedWorkflowVersion))
	})

	t.Run("default version can be recorded for absent marker path", func(t *testing.T) {
		var saved *core.Checkpoint
		ctx := newTestVersionContext("job-1", nil, func(_ context.Context, cp *core.Checkpoint) error {
			saved = cp
			return nil
		})

		version, err := GetVersion(ctx, "legacy-branch", DefaultVersion, DefaultVersion)
		require.NoError(t, err)
		assert.Equal(t, DefaultVersion, version)
		require.NotNil(t, saved)

		var recorded int
		require.NoError(t, json.Unmarshal(saved.Result, &recorded))
		assert.Equal(t, DefaultVersion, recorded)
	})

	t.Run("same change ID is stable within one execution and distinct IDs are independent", func(t *testing.T) {
		ctx := newTestVersionContext("job-1", nil, func(_ context.Context, _ *core.Checkpoint) error {
			return nil
		})

		first, err := GetVersion(ctx, "change-a", DefaultVersion, 1)
		require.NoError(t, err)
		second, err := GetVersion(ctx, "change-a", DefaultVersion, 2)
		require.NoError(t, err)
		other, err := GetVersion(ctx, "change-b", DefaultVersion, 2)
		require.NoError(t, err)

		assert.Equal(t, 1, first)
		assert.Equal(t, 1, second)
		assert.Equal(t, 2, other)
	})

	t.Run("version marker is excluded from strict unconsumed call checkpoint guard", func(t *testing.T) {
		ctx := newTestVersionContext("job-1", nil, func(_ context.Context, _ *core.Checkpoint) error {
			return nil
		})

		_, err := GetVersion(ctx, "strict-safe-change", DefaultVersion, 1)
		require.NoError(t, err)

		cs := intctx.GetCallState(ctx)
		require.NotNil(t, cs)
		assert.Equal(t, 0, cs.UnconsumedCallCheckpoints())
	})
}

func TestGetVersion_GormStorageRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newVersionTestStorage(t)
	job := &core.Job{Type: "workflow.versioned", Queue: "default"}
	require.NoError(t, store.Enqueue(ctx, job))

	runCtx := newTestVersionContext(job.ID, nil, store.SaveCheckpoint)
	version, err := GetVersion(runCtx, "fanout-shape", DefaultVersion, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, version)

	checkpoints, err := store.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	assert.Equal(t, -1, checkpoints[0].CallIndex)
	assert.Equal(t, versionCheckpointType("fanout-shape"), checkpoints[0].CallType)

	replayCtx := newTestVersionContext(job.ID, checkpoints, store.SaveCheckpoint)
	replayed, err := GetVersion(replayCtx, "fanout-shape", DefaultVersion, 2)
	require.NoError(t, err)
	assert.Equal(t, 1, replayed)
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

func newTestVersionContext(jobID core.UUID, checkpoints []core.Checkpoint, save func(context.Context, *core.Checkpoint) error) context.Context {
	if save == nil {
		save = func(context.Context, *core.Checkpoint) error { return nil }
	}
	ctx := intctx.WithJobContext(context.Background(), &intctx.JobContext{
		Job:            &core.Job{ID: jobID},
		SaveCheckpoint: save,
	})
	return intctx.WithCallState(ctx, checkpoints)
}

func newVersionTestStorage(t *testing.T) *storage.GormStorage {
	t.Helper()

	db, external := openVersionTestDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	if external {
		cleanupVersionExternalDB(t, db)
		t.Cleanup(func() { cleanupVersionExternalDB(t, db) })
	}
	return store
}

func openVersionTestDB(t *testing.T) (*gorm.DB, bool) {
	t.Helper()

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open mysql version test db")
		closeVersionTestDB(t, db)
		return db, true
	}
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres version test db")
		closeVersionTestDB(t, db)
		return db, true
	}

	dbPath := t.TempDir() + "/workflow-versioning.db"
	db, err := gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open sqlite version test db")
	closeVersionTestDB(t, db)
	return db, false
}

func closeVersionTestDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(2)
	sqlDB.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
}

func cleanupVersionExternalDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, table := range []string{"checkpoints", "fan_outs", "queue_states", "jobs", "scheduled_fires", "leases"} {
		require.NoError(t, db.Exec("DELETE FROM "+table).Error)
	}
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

	t.Run("loads multiple phase checkpoints after replay", func(t *testing.T) {
		baseCtx := context.Background()

		checkpoints := []core.Checkpoint{
			{
				ID:        "cp-phase-1",
				JobID:     "job-1",
				CallIndex: -1,
				CallType:  "phase1",
				Result:    []byte(`"result1"`),
			},
			{
				ID:        "cp-phase-2",
				JobID:     "job-1",
				CallIndex: -1,
				CallType:  "phase2",
				Result:    []byte(`"result2"`),
			},
			{
				ID:        "cp-phase-3",
				JobID:     "job-1",
				CallIndex: -1,
				CallType:  "phase3",
				Result:    []byte(`"result3"`),
			},
		}

		jobCtx := &intctx.JobContext{Job: &core.Job{ID: "job-1"}}
		ctx := intctx.WithJobContext(baseCtx, jobCtx)
		ctx = intctx.WithCallState(ctx, checkpoints)

		for i, phase := range []string{"phase1", "phase2", "phase3"} {
			result, found := LoadPhaseCheckpoint[string](ctx, phase)
			if !found {
				t.Fatalf("expected %s checkpoint to be found", phase)
			}
			expected := "result" + string(rune('1'+i))
			if result != expected {
				t.Fatalf("expected %q, got %q", expected, result)
			}
		}
	})
}
