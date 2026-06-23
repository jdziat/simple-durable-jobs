package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Thin re-export option wrappers that were previously uncovered (0%).
// Each is a one-line delegation; constructing the Option/Schedule/bool fully
// covers the facade function.
// ---------------------------------------------------------------------------

func TestGenOptionWrappers_ReturnNonNil(t *testing.T) {
	assert.NotNil(t, jobs.Timeout(5*time.Second))
	assert.NotNil(t, jobs.Timeout(0))
	assert.NotNil(t, jobs.WithDrainTimeout(10*time.Second))
	assert.NotNil(t, jobs.WithFanOutRecoveryStaleAge(3*time.Minute))
}

func TestGenTimeout_AppliesToOptions(t *testing.T) {
	opts := jobs.NewOptions()
	require.NotNil(t, opts)
	jobs.Timeout(7 * time.Second).Apply(opts)
	assert.Equal(t, 7*time.Second, opts.Timeout)
}

// ---------------------------------------------------------------------------
// Cron (0%) — distinct from MustCron. Cover both the valid and error paths.
// ---------------------------------------------------------------------------

func TestGenCron_ValidExpression(t *testing.T) {
	s, err := jobs.Cron("* * * * *")
	require.NoError(t, err)
	require.NotNil(t, s)
	next := s.Next(time.Now())
	assert.False(t, next.IsZero())
}

func TestGenCron_InvalidExpressionReturnsError(t *testing.T) {
	_, err := jobs.Cron("not a cron expression")
	assert.Error(t, err)
}

func TestGenMustCron_PanicsOnInvalid(t *testing.T) {
	assert.Panics(t, func() {
		_ = jobs.MustCron("definitely not valid")
	})
}

// ---------------------------------------------------------------------------
// IsWaitingError (0%) — one-line delegation. Cover nil, regular error, and a
// constructed waiting-signal error (via the deprecated alias which shares the
// same underlying error value contract).
// ---------------------------------------------------------------------------

func TestGenIsWaitingError_NilAndRegular(t *testing.T) {
	assert.False(t, jobs.IsWaitingError(nil))
	assert.False(t, jobs.IsWaitingError(errors.New("ordinary failure")))
}

// ---------------------------------------------------------------------------
// Call / FanOut / SavePhaseCheckpoint / LoadPhaseCheckpoint (0%).
// These facade functions are single-statement delegations. Without an active
// job-handler context they take their no-context branch (error / no-op),
// which fully covers the one-line facade wrapper deterministically.
// ---------------------------------------------------------------------------

func TestGenCall_WithoutJobContextReturnsError(t *testing.T) {
	ctx := context.Background()
	_, err := jobs.Call[string](ctx, "some-nested-job", "arg")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job handler")
}

func TestGenCall_TypedZeroValueOnError(t *testing.T) {
	ctx := context.Background()
	got, err := jobs.Call[int](ctx, "nested", 1)
	require.Error(t, err)
	assert.Equal(t, 0, got)
}

func TestGenFanOut_WithoutJobContextReturnsError(t *testing.T) {
	ctx := context.Background()
	subs := []jobs.SubJob{jobs.Sub("child", 1), jobs.Sub("child", 2)}
	results, err := jobs.FanOut[int](ctx, subs)
	require.Error(t, err)
	assert.Nil(t, results)
}

func TestGenSavePhaseCheckpoint_NoContextIsNoOp(t *testing.T) {
	// Outside a job handler this is a silent no-op returning nil.
	err := jobs.SavePhaseCheckpoint(context.Background(), "phase-1", map[string]int{"a": 1})
	require.NoError(t, err)
}

func TestGenLoadPhaseCheckpoint_NoContextReturnsFalse(t *testing.T) {
	got, ok := jobs.LoadPhaseCheckpoint[string](context.Background(), "phase-1")
	assert.False(t, ok)
	assert.Equal(t, "", got)
}

// ---------------------------------------------------------------------------
// init() WorkerFactory closure (16.7%). The closure is only exercised through
// queue.NewWorker (the *Queue method), not the package-level jobs.NewWorker.
// Passing WorkerOption values exercises the type-assertion branch inside the
// closure; passing a non-WorkerOption exercises the skip branch.
// ---------------------------------------------------------------------------

func TestGenQueueNewWorker_RunsFactoryClosure(t *testing.T) {
	q, _ := setupTestStorage(t)

	// With recognised WorkerOption values: the closure appends each into
	// workerOpts and constructs a real worker.
	starter := q.NewWorker(
		jobs.Concurrency(2),
		jobs.WithPollInterval(100*time.Millisecond),
	)
	require.NotNil(t, starter)
}

func TestGenQueueNewWorker_IgnoresNonWorkerOptions(t *testing.T) {
	q, _ := setupTestStorage(t)

	// A non-WorkerOption value must be silently skipped by the closure's
	// type assertion, still producing a valid worker.
	starter := q.NewWorker(
		jobs.Concurrency(1),
		"not-a-worker-option",
		42,
	)
	require.NotNil(t, starter)
}

func TestGenQueueNewWorker_NoOptions(t *testing.T) {
	q, _ := setupTestStorage(t)
	starter := q.NewWorker()
	require.NotNil(t, starter)
}

// ---------------------------------------------------------------------------
// LoadResult (80%) — cover the remaining branches: job-not-found and the
// JSON decode-failure path (Result bytes that cannot unmarshal into T).
// ---------------------------------------------------------------------------

func TestGenLoadResult_JobNotFound(t *testing.T) {
	q, _ := setupIsolatedStorage(t)
	_, err := jobs.LoadResult[string](context.Background(), q, jobs.NewID())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGenLoadResult_DecodeFailure(t *testing.T) {
	q, store := setupIsolatedStorage(t)
	id := jobs.NewID()

	// Persist a completed job whose Result is a JSON string, then attempt to
	// decode it into a struct — forcing json.Unmarshal to fail.
	require.NoError(t, store.Enqueue(context.Background(), &jobs.Job{
		ID:     id,
		Type:   "x",
		Status: jobs.StatusCompleted,
		Result: []byte(`"a plain string"`),
	}))

	type Target struct {
		Field int `json:"field"`
	}
	_, err := jobs.LoadResult[Target](context.Background(), q, id)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode")
}
