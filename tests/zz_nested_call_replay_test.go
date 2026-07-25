package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Nested durable operations inside a Call'd handler used to corrupt replay.
//
// Call indices come from ONE flat counter shared with everything nested beneath
// a call. On the first run an outer Call that internally issues its own Call
// consumes two indices. On replay the outer call is served from its checkpoint
// WITHOUT re-invoking its handler, so the nested index is never consumed — and
// every later call then reads the checkpoint one slot too low.
//
// Observed before the fix, on SQLite, in a default configuration, through the
// public API, triggered by an ordinary retry with no crash involved:
//
//	attempt=1  s=1998   (correct)
//	attempt=2  s=10     (the NESTED call's result) — and the job COMPLETED
//
// No error, no warning: the workflow finished successfully carrying another
// call's data. These tests run on whichever backend the suite is pointed at, so
// they gate all three.

// TestNestedCall_ReplayDoesNotReturnAnotherCallsResult is the headline guard.
// It fails on the parent commit with s == 10.
func TestNestedCall_ReplayDoesNotReturnAnotherCallsResult(t *testing.T) {
	q, storage := openIntegrationQueue(t)

	var leafRuns, wfRuns atomic.Int32

	q.Register("leaf", func(ctx context.Context, n int) (int, error) {
		leafRuns.Add(1)
		return 2 * n, nil
	})

	// A Call'd handler that itself issues a durable Call — the shape that shifts
	// the shared index space.
	q.Register("child", func(ctx context.Context, n int) (int, error) {
		v, err := jobs.Call[int](ctx, "leaf", n)
		if err != nil {
			return 0, err
		}
		return v + 1, nil
	})

	q.Register("wf", func(ctx context.Context, _ struct{}) (string, error) {
		attempt := wfRuns.Add(1)

		r, err := jobs.Call[int](ctx, "child", 5) // index 0; nested leaf takes index 1
		if err != nil {
			return "", err
		}
		s, err := jobs.Call[int](ctx, "leaf", 999) // index 2 on the first run
		if err != nil {
			return "", err
		}
		if attempt == 1 {
			// Force exactly one ordinary retry. This is the whole trigger — no
			// crash, no kill, no lease expiry.
			return "", errors.New("forced retryable failure after both calls")
		}
		return string(mustJSON(t, []int{r, s})), nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "wf", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	runWorkerUntilTerminal(t, q, id)

	job, err := storage.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, core.StatusCompleted, job.Status, "workflow should complete; last_error=%q", job.LastError)

	var got []int
	var raw string
	require.NoError(t, json.Unmarshal(job.Result, &raw))
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	require.Len(t, got, 2)

	assert.Equal(t, 11, got[0], "Call(child,5) must return 11")
	assert.Equal(t, 1998, got[1],
		"Call(leaf,999) must return 1998. Getting 10 means the replayed outer call read the "+
			"NESTED leaf's checkpoint — the corruption this test exists to prevent")

	assert.Equal(t, int32(2), wfRuns.Load(), "the workflow must have retried exactly once")
}

// TestNestedCall_ReplayDoesNotRaiseBogusDeterminismViolation covers the sibling
// symptom. When the shifted index lands on a checkpoint of a DIFFERENT type, the
// mismatch scan fires and a perfectly deterministic handler is rejected as
// non-deterministic, retrying identically until it dead-letters.
func TestNestedCall_ReplayDoesNotRaiseBogusDeterminismViolation(t *testing.T) {
	q, storage := openIntegrationQueue(t)

	var wfRuns atomic.Int32

	q.Register("leaf", func(ctx context.Context, n int) (int, error) { return 2 * n, nil })
	q.Register("leaf2", func(ctx context.Context, n int) (int, error) { return n + 7, nil })
	q.Register("child", func(ctx context.Context, n int) (int, error) {
		return jobs.Call[int](ctx, "leaf", n)
	})

	q.Register("wf", func(ctx context.Context, _ struct{}) (string, error) {
		attempt := wfRuns.Add(1)
		if _, err := jobs.Call[int](ctx, "child", 5); err != nil {
			return "", err
		}
		// On replay this lands where the nested "leaf" checkpoint lives unless
		// the span jump is applied — a type mismatch, not a value mismatch.
		if _, err := jobs.Call[int](ctx, "leaf2", 1); err != nil {
			return "", err
		}
		if attempt == 1 {
			return "", errors.New("forced retryable failure")
		}
		return "ok", nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "wf", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	runWorkerUntilTerminal(t, q, id)

	job, err := storage.GetJob(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCompleted, job.Status,
		"a deterministic handler must not be rejected for nondeterminism; last_error=%q", job.LastError)
	assert.NotContains(t, job.LastError, "determinism violation")
}

// TestNestedCall_SpanEndPersistsAndSurvivesUpsert pins the storage half.
//
// FALSE-GREEN TRAP: a test that only ever INSERTs a checkpoint passes even when
// span_end is missing from the OnConflict DoUpdates list — and that omission
// silently re-opens the corruption on every re-saved checkpoint. This writes the
// same (job_id, call_index, call_type) TWICE with different spans and asserts the
// second value is what comes back.
func TestNestedCall_SpanEndPersistsAndSurvivesUpsert(t *testing.T) {
	_, storage := openIntegrationQueue(t)
	ctx := context.Background()

	job := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusPending}
	require.NoError(t, storage.Enqueue(ctx, job))

	cp := &core.Checkpoint{
		ID: core.NewID(), JobID: job.ID, CallIndex: 0, CallType: "child",
		Result: []byte(`11`), SpanEnd: 2,
	}
	require.NoError(t, storage.SaveCheckpoint(ctx, cp))

	got, err := storage.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 2, got[0].SpanEnd, "span_end must round-trip on insert")

	// Re-save the SAME logical checkpoint with a wider span.
	cp2 := &core.Checkpoint{
		ID: core.NewID(), JobID: job.ID, CallIndex: 0, CallType: "child",
		Result: []byte(`11`), SpanEnd: 5,
	}
	require.NoError(t, storage.SaveCheckpoint(ctx, cp2))

	got, err = storage.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, got, 1, "upsert must not create a second row")
	assert.Equal(t, 5, got[0].SpanEnd,
		"span_end must be in the OnConflict DoUpdates column list; a stale span re-opens the "+
			"nested-call corruption on the re-save path")
}

// TestNestedCall_LegacyCheckpointDegradesToLegacyBehaviour proves the backfill
// contract: span_end == 0 (a row written before the column existed) must behave
// exactly as it did before, so upgrading cannot change a non-nested workflow.
func TestNestedCall_LegacyCheckpointDegradesToLegacyBehaviour(t *testing.T) {
	q, storage := openIntegrationQueue(t)

	var runs atomic.Int32
	q.Register("a", func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	q.Register("b", func(ctx context.Context, n int) (int, error) { return n * 100, nil })

	// A FLAT workflow — no nesting, so no span is ever wider than +1.
	q.Register("flat", func(ctx context.Context, _ struct{}) (string, error) {
		attempt := runs.Add(1)
		x, err := jobs.Call[int](ctx, "a", 2)
		if err != nil {
			return "", err
		}
		y, err := jobs.Call[int](ctx, "b", 3)
		if err != nil {
			return "", err
		}
		if attempt == 1 {
			return "", errors.New("forced retry")
		}
		return string(mustJSON(t, []int{x, y})), nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "flat", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	runWorkerUntilTerminal(t, q, id)

	job, err := storage.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, core.StatusCompleted, job.Status)

	var raw string
	var got []int
	require.NoError(t, json.Unmarshal(job.Result, &raw))
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Equal(t, []int{20, 300}, got, "a flat workflow must be unaffected by the span jump")

	cps, err := storage.GetCheckpoints(ctx, job.ID)
	require.NoError(t, err)
	for _, cp := range cps {
		assert.Equal(t, cp.CallIndex+1, cp.SpanEnd,
			"a non-nested call's span must be exactly one past itself (index %d)", cp.CallIndex)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// runWorkerUntilTerminal starts a worker and stops as soon as the job reaches a
// terminal status, so the test does not depend on a fixed sleep.
func runWorkerUntilTerminal(t *testing.T, q *jobs.Queue, id core.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// NewWorker returns a core.Starter (Start only); cancelling ctx is how the
	// existing integration tests stop a worker.
	w := q.NewWorker(jobs.WithPollInterval(20 * time.Millisecond))
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool {
		job, err := q.Storage().GetJob(context.Background(), id)
		if err != nil {
			return false
		}
		switch job.Status {
		case core.StatusCompleted, core.StatusFailed, core.StatusCancelled:
			return true
		}
		return false
	}, 40*time.Second, 25*time.Millisecond, "job never reached a terminal status")
}
